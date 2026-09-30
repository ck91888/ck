import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {guardWorkChain,chainNeed,shippingBasisStatements} from '../worker-v2/work-chain.js';
import {uploadWorkMaterial} from '../worker-v2/work-chain.js';
import {handleSop} from '../worker-v2/sop.js';
import {FIELD_ACTIONS} from '../worker-v2/access-control.js';
function setup(){
 const DB=database(),objects=new Map(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ACCEPT_NEW:'true',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'Fixture manager',role:'manager',key:'work-chain-fixture-only'}]),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:await new Response(body).arrayBuffer(),httpMetadata:meta.httpMetadata});},async get(key){return objects.get(key);},async delete(key){objects.delete(key);}}};let cookie='';
 async function raw(body){const multipart=body instanceof FormData;const r=await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{Cookie:cookie,...(!multipart?{'Content-Type':'application/json'}:{})},body:multipart?body:JSON.stringify(body)}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];return r.json();}
 const call=(action,data={})=>raw({action,client_req_id:crypto.randomUUID(),...data});
 const login=()=>call('sop_login',{sop_key:'work-chain-fixture-only'});
 const get=async id=>(await call('sop_get',{id})).record;
 const change=async(action,id,fields={})=>call(action,{id,revision:(await get(id)).revision,...fields});
 async function need(fields={}){const r=await call('sop_need_create',{title:'Fixture work',customer:'Fixture customer',source_type:'inventory',supply_chain_no:'STOCK-FIXTURE',instructions:'Check ten cartons',planned_quantity:10,planned_unit:'箱',department:'bulk',owner:'Fixture service',...fields});assert.equal(r.ok,true,r.error);return get(r.id);}
 async function schedule(n,quantity=4,fields={}){return call('v2_outbound_order_create',{customer:n.customer,biz_class:n.department,outbound_mode:'customer_pickup',expected_ship_at:'2026-10-02',sop_existing_need_id:n.id,sop_need_revision:n.revision,sop_link_quantity:quantity,...fields});}
 async function upload(n,fields={}){const form=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',related_doc_type:'sop_need',related_doc_id:n.id,attachment_category:'work_material',revision:n.revision,client_req_id:crypto.randomUUID(),...fields}))form.set(k,v);form.set('file',new File(['%PDF-1.7\nfixture document'], 'fixture-label.pdf',{type:'application/pdf'}));return raw(form);}
 return {DB,env,objects,call,raw,login,get,change,need,schedule,upload};
}

test('shipping and requirement notifications show actual changes; acknowledgement cannot approve unseen revisions',async()=>{
 const s=setup();await s.login();const n=await s.need({planned_quantity:28}),ob=await s.schedule(n,2);
 await s.call('v2_outbound_order_update_status',{id:ob.id,status:'issued'});
 const r=await s.change('sop_need_shipping_basis',n.id,{quantity:2,unit:'托',reason:'Fixture packaging',allocations:[{outbound_id:ob.id,quantity:2}]});assert.equal(r.ok,true,r.error);
 const d=await s.call('v2_outbound_order_detail',{id:ob.id}),log=d.change_logs.at(0);assert.equal(log.change_type,'shipping_adjustment');assert.deepEqual(log.diff,{shipping_quantity:{from:'2 箱',to:'2 托'}});
 const revision=d.order.revision_no;
 assert.equal((await s.call('v2_outbound_order_ack_change',{id:ob.id})).ok,false);
 const edit=await s.change('sop_need_update',n.id,{instructions:'Fixture new requirements',owner:n.owner});assert.equal(edit.ok,true,edit.error);
 const latest=await s.call('v2_outbound_order_detail',{id:ob.id});assert.equal(latest.change_logs[0].change_type,'requirement_update');
 assert.equal((await s.call('v2_outbound_order_ack_change',{id:ob.id,revision_no:revision})).ok,false);
 assert.equal(s.DB.raw.prepare('SELECT warehouse_ack_required FROM v2_outbound_orders WHERE id=?').get(ob.id).warehouse_ack_required,1);
 // Simulate an office change after the confirmation read but before its atomic batch.
 const batch=s.DB.batch.bind(s.DB);let injected=false;
 s.DB.batch=async statements=>{if(!injected&&statements.some(x=>x.sql.includes('SET warehouse_ack_required=0'))){injected=true;s.DB.raw.prepare('UPDATE v2_outbound_orders SET revision_no=revision_no+1 WHERE id=?').run(ob.id);}return batch(statements);};
 assert.equal((await s.call('v2_outbound_order_ack_change',{id:ob.id,revision_no:latest.order.revision_no})).ok,false);assert.equal(injected,true);
 const current=await s.call('v2_outbound_order_detail',{id:ob.id});assert.equal(current.order.warehouse_ack_required,1);
 assert.equal((await s.call('v2_outbound_order_ack_change',{id:ob.id,revision_no:current.order.revision_no})).ok,true);
 assert.equal(s.DB.raw.prepare('SELECT warehouse_ack_required FROM v2_outbound_orders WHERE id=?').get(ob.id).warehouse_ack_required,0);
 s.DB.raw.prepare('UPDATE v2_outbound_orders SET pickup_confirm_required=1 WHERE id=?').run(ob.id);
 assert.equal((await s.call('v2_outbound_pickup_confirm',{id:ob.id,revision_no:revision})).ok,false);
 assert.equal(s.DB.raw.prepare('SELECT pickup_confirm_required FROM v2_outbound_orders WHERE id=?').get(ob.id).pickup_confirm_required,1);
 assert.equal((await s.call('v2_outbound_pickup_confirm',{id:ob.id,revision_no:current.order.revision_no})).ok,true);
});
test('new outbound must reference a requirement; reverse and orphan paths cannot bypass the chain',async()=>{
 const s=setup();await s.login();
 assert.equal((await s.call('v2_outbound_order_create',{customer:'Fixture customer',biz_class:'bulk',outbound_mode:'customer_pickup'})).ok,false);
 assert.equal((await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],auto_create_outbound:true})).ok,false);
 for(const source_type of ['outbound','unknown',''])assert.equal((await s.call('sop_need_create',{title:'Bad source',customer:'Fixture customer',source_type,department:'bulk',instructions:'Bad'})).ok,false);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n,0);
 const n=await s.need();assert.equal((await s.schedule(n,4,{expected_ship_at:'2026-02-31'})).ok,false);
 assert.equal((await s.schedule(n,4,{instruction:'Conflicting instructions'})).ok,false);
 assert.equal((await s.schedule(n,4,{customer:'Different customer'})).ok,false);
 assert.equal((await s.schedule(n,4,{sop_need_revision:0})).ok,false);
});
test('pending work can be booked and issued; actual loading waits for review; retries and cancel release capacity',async()=>{
 const s=setup();await s.login();let n=await s.need();
 const a=await s.schedule(n,6,{client_req_id:'ship-once'});assert.equal(a.ok,true,a.error);
 assert.equal((await s.schedule(n,6,{client_req_id:'ship-once'})).id,a.id);
 assert.equal((await s.call('v2_outbound_order_update_status',{id:a.id,status:'issued'})).ok,true);
 assert.match(await guardWorkChain({action:'v2_outbound_load_start',order_id:a.id},s.env),/尚未完成审核/);
 assert.equal((await s.change('sop_need_update',n.id,{instructions:n.instructions,owner:'Fixture service',planned_quantity:5,planned_unit:'箱'})).ok,false);
 assert.equal((await s.call('v2_outbound_order_update',{id:a.id,instruction:'Other'})).ok,false);
 assert.equal((await s.call('v2_outbound_order_update_status',{id:a.id,status:'cancelled'})).ok,true);
 n=await s.get(n.id);const b=await s.schedule(n,10);assert.equal(b.ok,true,b.error);
 assert.equal((await s.schedule(await s.get(n.id),1)).ok,false);
 const task=await s.call('sop_task_create',{department:'bulk',title:'Fixture work',job_type:'bulk_op',need_id:n.id,workers:[{id:'FIXTURE-A',name:'Fixture A'}],lead_id:'FIXTURE-A',estimated_minutes:10});assert.equal(task.ok,true,task.error);
 assert.equal((await s.change('sop_task_start',task.id)).ok,true);
 const done=await s.change('sop_task_complete_review',task.id,{result:{quantity:10,unit:'箱'},decision:'pass',reason:'Fixture verified'});assert.equal(done.ok,true,done.error);
 assert.equal(await guardWorkChain({action:'v2_outbound_load_start',order_id:b.id},s.env),null);
 assert.match(await guardWorkChain({action:'v2_outbound_load_start',order_id:a.id},s.env),/已取消/);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,1,'only the actual work created a labor job');
});
test('carton input can be packed into pallet output without rewriting input cargo or inventing labor',async()=>{
 const s=setup();await s.login();const n=await s.need({planned_quantity:28});const ob=await s.schedule(n,2);
 const task=await s.call('sop_task_create',{department:'bulk',title:'Fixture palletizing',job_type:'bulk_op',need_id:n.id,workers:[{id:'FIXTURE-A',name:'Fixture A'}],lead_id:'FIXTURE-A',estimated_minutes:10});
 await s.change('sop_task_start',task.id);
 const finish=result=>s.change('sop_task_complete_review',task.id,{result,decision:'pass',reason:'Fixture verified'});
 let r=await finish({quantity:2,unit:'托',operated_box_count:28,pallet_count:2});assert.equal(r.ok,false);assert.match(r.error,/单位不一致.*2 托.*2 箱/);
 assert.equal((await s.get(task.id)).status,'working');
 const payload={quantity:2,unit:'托',reason:'Fixture packaging output',allocations:[{outbound_id:ob.id,quantity:2}],client_req_id:'fixture-basis-once'};
 r=await s.change('sop_need_shipping_basis',n.id,payload);assert.equal(r.ok,true,r.error);
 const revision=r.revision;assert.equal((await s.change('sop_need_shipping_basis',n.id,payload)).revision,revision,'retry must not repeat adjustment');
 const need=await s.get(n.id);assert.equal(need.planned_quantity,28);assert.equal(need.planned_unit,'箱');assert.equal(need.shipping_basis.unit,'托');assert.equal(need.links[0].unit,'托');
 let order=s.DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').get(ob.id);assert.equal(order.planned_box_count,0);assert.equal(order.planned_pallet_count,2);assert.equal(order.warehouse_ack_required,1);
 const resolved=await s.call('sop_field_resolve',{code:task.id});assert.equal(resolved.shipping.unit,'托');assert.equal(resolved.shipping.quantity,2);assert.equal(resolved.shipping.used,2);
 r=await finish({quantity:1,unit:'托'});assert.equal(r.ok,false);assert.match(r.error,/实际成果不足.*1 托.*2 托.*1 托/);
 assert.equal((await s.get(task.id)).status,'working');assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);
 r=await finish({quantity:2,unit:'托',operated_box_count:28,pallet_count:2});assert.equal(r.ok,true,r.error);
 assert.equal((await s.get(n.id)).status,'linked');assert.equal((await s.get(n.id)).links[0].phase,'confirmed');
 order=s.DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').get(ob.id);assert.equal(order.stock_operation_status,'completed');
 const output=s.DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=?').get(task.id);assert.equal(output.box_count,28);assert.equal(output.pallet_count,2);
 assert.equal(s.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(task.id).n,0);
 assert.equal((await s.change('sop_need_shipping_basis',n.id,{...payload,client_req_id:crypto.randomUUID()})).ok,false,'reviewed output is immutable');
});
test('shipping unit adjustment covers every active booking, respects capacity, cancellation and stale revisions',async()=>{
 const s=setup();await s.login();const n=await s.need({planned_quantity:28});const first=await s.schedule(n,10),second=await s.schedule(await s.get(n.id),10),cancelled=await s.schedule(await s.get(n.id),8);
 await s.call('v2_outbound_order_update_status',{id:cancelled.id,status:'cancelled'});
 const basis={quantity:3,unit:'托',reason:'Fixture split pallets',allocations:[{outbound_id:first.id,quantity:1},{outbound_id:second.id,quantity:2}]};
 for(const bad of [{...basis,allocations:basis.allocations.slice(0,1)},{...basis,allocations:[basis.allocations[0],basis.allocations[0]]},{...basis,quantity:2},{...basis,unit:'???'},{...basis,reason:''}])assert.equal((await s.change('sop_need_shipping_basis',n.id,bad)).ok,false);
 assert.equal((await s.get(n.id)).shipping_basis,undefined);
 assert.equal(s.DB.raw.prepare('SELECT planned_box_count FROM v2_outbound_orders WHERE id=?').get(first.id).planned_box_count,10);
 const old=await s.get(n.id);assert.equal((await s.change('sop_need_shipping_basis',n.id,basis)).ok,true);
 assert.equal((await s.call('sop_need_shipping_basis',{id:n.id,revision:old.revision,...basis})).ok,false);
 const current=await s.get(n.id);assert.equal(current.links.find(l=>l.outbound_id===cancelled.id).unit,'箱','cancelled history is preserved');
 assert.equal((await s.change('sop_need_plan_quantity',n.id,{outbound_id:second.id,quantity:3,reason:'Fixture overflow'})).ok,false,'old adjustment route uses pallet capacity');
 assert.equal((await s.schedule(await s.get(n.id),1)).ok,false,'future scheduling uses expected output capacity');
 const search=await s.call('sop_work_need_search',{search:n.id});assert.equal(search.items[0].remaining,0);assert.equal(search.items[0].schedule_quantity,3);assert.equal(search.items[0].schedule_unit,'托');
});
test('review rechecks bookings changed after recording output; office-only unit changes cannot bypass loading or direct forwarding guards',async()=>{
 const s=setup();await s.login();const n=await s.need();const ob=await s.schedule(n,2);
 const task=await s.call('sop_task_create',{department:'bulk',title:'Fixture work',job_type:'bulk_op',need_id:n.id,workers:[{id:'FIXTURE-A',name:'Fixture A'}],lead_id:'FIXTURE-A',estimated_minutes:10});
 await s.change('sop_task_start',task.id);assert.equal((await s.change('sop_task_finish',task.id,{result:{quantity:2,unit:'箱'}})).ok,true);
 const basis={quantity:2,unit:'托',reason:'Fixture changed booking',allocations:[{outbound_id:ob.id,quantity:2}]};
 assert.equal((await s.change('sop_need_shipping_basis',n.id,basis)).ok,true);
 const review=await s.change('sop_task_review',task.id,{decision:'pass',reason:'Fixture review'});assert.equal(review.ok,false);assert.match(review.error,/单位不一致/);assert.equal((await s.get(task.id)).status,'awaiting_review');
 assert.equal((await s.change('sop_task_review',task.id,{decision:'return',reason:'Fixture correction'})).ok,true);
 const direct=await s.need({operation_kind:'direct_forward'});assert.equal((await s.change('sop_need_shipping_basis',direct.id,{quantity:1,unit:'托',reason:'Fixture',allocations:[]})).ok,false);
 await assert.rejects(shippingBasisStatements(s.env,{id:n.id},await s.get(n.id),basis,{role:'dispatcher'},new Date().toISOString()),/办公室/);
 s.DB.raw.prepare("UPDATE v2_outbound_orders SET status='shipped' WHERE id=?").run(ob.id);
 assert.equal((await s.change('sop_need_shipping_basis',n.id,basis)).ok,false);
});
test('one inbound can start multiple requirements and bookings in one transaction',async()=>{
 const s=setup();await s.login();const plan=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:20}],work_requests:[{title:'Palletize',instructions:'Palletize ten',department:'bulk',planned_quantity:10,planned_unit:'箱',outbounds:[{quantity:5,expected_ship_at:'2026-10-03',outbound_mode:'customer_pickup'}]},{title:'Direct forward',instructions:'Forward unopened cartons',department:'bulk',operation_kind:'direct_forward',planned_quantity:10,planned_unit:'箱'}]});
 assert.equal(plan.ok,true,plan.error);assert.equal(plan.needs.length,2);assert.equal(plan.outbounds.length,1);assert.equal(plan.outbounds[0].display_no,'CHU-FC-20261003');
 for(const n of plan.needs){const r=await s.get(n.id);assert.equal(r.source_type,'inbound');assert.equal(r.source_id,plan.id);}
 const bad=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],work_requests:[{title:'Bad booking',instructions:'Check',planned_quantity:5,planned_unit:'箱',outbounds:[{quantity:6,expected_ship_at:'2026-10-03',outbound_mode:'customer_pickup'}]}]});assert.equal(bad.ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);
});
test('direct forwarding requires receipt then quantity confirmation, without invented work hours',async()=>{
 const s=setup();await s.login();const plan=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:10}]});
 let n=await s.need({source_type:'inbound',source_id:plan.id,operation_kind:'direct_forward'});const ob=await s.schedule(n,10);assert.equal(ob.ok,true,ob.error);
 assert.equal((await s.change('sop_need_forward_ready',n.id,{quantity:10})).ok,false);
 s.DB.raw.prepare("UPDATE v2_inbound_plans SET status='completed',unload_completed_at=? WHERE id=?").run(new Date().toISOString(),plan.id);
 assert.equal((await s.change('sop_need_forward_ready',n.id,{quantity:9})).ok,false);
 const ready=await s.change('sop_need_forward_ready',n.id,{quantity:10});assert.equal(ready.ok,true,ready.error);
 assert.equal((await s.change('sop_need_forward_ready',n.id,{quantity:10})).ok,false);
 assert.equal(await guardWorkChain({action:'v2_outbound_load_start',order_id:ob.id},s.env),null);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,0);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);
});

test('inline inbound and inventory bookings preserve the same PO and shipping fields as standalone bookings',async()=>{
 const s=setup();await s.login();
 const modes=['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
 const bookings=modes.map((outbound_mode,i)=>({quantity:1,expected_ship_at:'2026-10-03',outbound_mode,po_no:'00-PO-'+i,destination:'Fixture destination '+i,outbound_requirement:'Fixture handover '+i}));
 const inbound=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:10}],work_requests:[{title:'Fixture bundled work',instructions:'Fixture instructions',department:'bulk',planned_quantity:10,planned_unit:'箱',outbounds:bookings}]});
 assert.equal(inbound.ok,true,inbound.error);assert.equal(inbound.outbounds.length,5);
 assert.deepEqual(inbound.outbounds.map(x=>x.display_no),['CHU-FC-20261003','CHU-FC-20261003-02','CHU-FC-20261003-03','CHU-FC-20261003-04','CHU-FC-20261003-05']);
 const inventory=await s.need({outbounds:[bookings[0]]});
 const savedInventory=(await s.call('v2_outbound_order_detail',{id:inventory.links[0].outbound_id})).order;
 assert.equal(savedInventory.po_no,bookings[0].po_no);
 for(const [i,b] of bookings.entries()){
  const bundled=(await s.call('v2_outbound_order_detail',{id:inbound.outbounds[i].id})).order;
  const n=await s.need(),created=await s.schedule(n,1,b);assert.equal(created.ok,true,created.error);
  const standalone=(await s.call('v2_outbound_order_detail',{id:created.id})).order;
  for(const key of ['po_no','destination','outbound_mode','expected_ship_at','outbound_requirement']){assert.equal(bundled[key],b[key],key);assert.equal(bundled[key],standalone[key],key+' matches standalone');}
 }
 const plain=await s.need({outbounds:[{quantity:1,expected_ship_at:'2026-10-03',outbound_mode:'customer_pickup'}]});
 assert.equal((await s.call('v2_outbound_order_detail',{id:plain.links[0].outbound_id})).order.po_no,'','PO stays optional');
 const before=s.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n;
 const bad=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],work_requests:[{title:'Invalid mode',instructions:'Fixture',planned_quantity:2,planned_unit:'箱',outbounds:[{...bookings[0],outbound_mode:'invalid'}]}]});
 assert.equal(bad.ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,before);
});
test('work files upload before work, retry once, flow to outbound list/detail, notify warehouse and can be withdrawn without losing history',async()=>{
 const s=setup();await s.login();let n=await s.need();const ob=await s.schedule(n,4);n=await s.get(n.id);
 const f=await s.upload(n,{client_req_id:'upload-once',material_kind:'pallet_label'});assert.equal(f.ok,true,f.error);assert.equal((await s.upload(n,{client_req_id:'upload-once',material_kind:'pallet_label'})).id,f.id);
 assert.equal(s.objects.size,1);assert.equal((await s.upload(n)).ok,false,'stale upload blocked');
 const docs=await s.call('sop_work_materials',{id:n.id});assert.equal(docs.items.length,1);assert.equal(docs.items[0].uploaded_by,'Fixture manager');
 const detail=await s.call('v2_outbound_order_detail',{id:ob.id});assert.equal(detail.attachments[0].id,f.id);assert.equal(detail.order.material_count,1);assert.equal(detail.order.warehouse_ack_required,1);
 const list=await s.call('v2_outbound_order_list',{has_material:'1'});assert.equal(list.ok,true,list.error);assert.equal(list.items[0].material_count,1);
 const removed=await s.change('sop_work_material_remove',n.id,{attachment_id:f.id});assert.equal(removed.ok,true,removed.error);
 assert.equal((await s.call('sop_work_materials',{id:n.id})).items.length,0);assert.equal((await s.call('v2_outbound_order_list',{has_material:'0'})).items.length,1);assert.equal(s.objects.size,1,'withdrawal retains the original blob');
 assert.equal((await s.call('v2_attachment_delete',{id:f.id})).ok,false);
 for(const related_doc_type of ['inbound_plan','outbound_order'])assert.equal((await s.upload(await s.get(n.id),{related_doc_type,related_doc_id:ob.id,attachment_category:'outbound_material'})).ok,false);
 await assert.rejects(()=>chainNeed(s.env,n.id,{role:'viewer',departments:['bulk']},true),/权限/);
});
test('office can record warehouse feedback without notifying warehouse; customer documents still require acknowledgement',async()=>{
 const s=setup();await s.login();let n=await s.need(),ob=await s.schedule(n,2);n=await s.get(n.id);
 const proxy=await s.upload(n,{material_kind:'work_material'});assert.equal(proxy.ok,true,proxy.error);
 let current=await s.get(n.id),order=s.DB.raw.prepare('SELECT warehouse_ack_required FROM v2_outbound_orders WHERE id=?').get(ob.id);
 assert.equal(current.requirement_version,n.requirement_version,'office-recorded feedback does not change warehouse instructions');
 assert.equal(current.material_version,(n.material_version||0)+1);assert.equal(order.warehouse_ack_required,0);
 assert.equal((await s.call('sop_work_materials',{id:n.id})).items[0].material_kind,'work_material');
 n=current;
 const field={id:'F',name:'Fixture dispatcher',role:'dispatcher',scope:'field',departments:['bulk']};
 const upload=(kind,revision)=>{const form=new FormData();for(const [k,v] of Object.entries({related_doc_id:n.id,material_kind:kind,revision,client_req_id:crypto.randomUUID()}))form.set(k,v);form.set('file',new File(['fixture details'],'work-details.pdf',{type:'application/pdf'}));return form;};
 await assert.rejects(()=>uploadWorkMaterial(upload('pallet_label',n.revision),{...s.env,SOP_REQUEST_USER:field}),/办公室客服/);
 const first=await uploadWorkMaterial(upload('work_material',n.revision),{...s.env,SOP_REQUEST_USER:field});assert.equal(first.ok,true);
 current=await s.get(n.id);order=s.DB.raw.prepare('SELECT warehouse_ack_required FROM v2_outbound_orders WHERE id=?').get(ob.id);
 assert.equal(current.requirement_version,n.requirement_version,'feedback does not change the printed instruction version');
 assert.equal(current.material_version,(n.material_version||0)+1);assert.equal(order.warehouse_ack_required,0,'warehouse need not acknowledge its own feedback');
 const files=(await s.call('sop_work_materials',{id:n.id})).items;assert.equal(files.length,2);assert.equal(files[0].material_kind,'work_material');assert.ok(files.some(f=>f.uploaded_by===field.name));
 assert.ok((await s.call('v2_outbound_order_detail',{id:ob.id})).attachments.some(f=>f.id===first.id),'customer can view the same feedback downstream');
 assert.equal(FIELD_ACTIONS.has('sop_work_material_remove'),true);
 const removed=await handleSop({action:'sop_work_material_remove',id:n.id,revision:current.revision,attachment_id:first.id,client_req_id:crypto.randomUUID()},{...s.env,SOP_REQUEST_USER:field});assert.equal(removed.ok,true,removed.error);
 current=await s.get(n.id);order=s.DB.raw.prepare('SELECT warehouse_ack_required FROM v2_outbound_orders WHERE id=?').get(ob.id);assert.equal(current.requirement_version,n.requirement_version);assert.equal(order.warehouse_ack_required,0);
 const office=await s.upload(current,{material_kind:'shipping_document'});assert.equal(office.ok,true,office.error);
 assert.equal(s.DB.raw.prepare('SELECT warehouse_ack_required FROM v2_outbound_orders WHERE id=?').get(ob.id).warehouse_ack_required,1,'new customer document requires warehouse acknowledgement');
});
test('batch files upload once, download from group and every related need, and notify downstream work',async()=>{
 const s=setup();await s.login();const p=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk']});
 let a=await s.need({source_type:'inbound',source_id:p.id}),b=await s.need({source_type:'inbound',source_id:p.id,department:'direct_ship',reason:'second part'});const other=await s.need();
 const ob=await s.schedule(a,4);a=await s.get(a.id);
 const request=crypto.randomUUID(),form=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:p.id,need_id:a.id,attachment_category:'batch_work_material',client_req_id:request}))form.set(k,v);form.set('file',new File(['carton,operation\n1,palletize'],'batch-work.csv',{type:'text/csv'}));
 const f=await s.raw(form);assert.equal(f.ok,true,f.error);assert.equal((await s.raw(form)).id,f.id);assert.equal(s.objects.size,1);
 const group=await s.call('sop_batch_work_materials',{id:a.id});assert.equal(group.ok,true,group.error);assert.equal(group.items[0].id,f.id);assert.equal(group.items[0].uploaded_by,'Fixture manager');assert.equal(group.plan.id,p.id);
 for(const n of [a,b]){const docs=await s.call('sop_work_materials',{id:n.id});assert.equal(docs.items[0].id,f.id);assert.equal(docs.items[0].batch,true);assert.equal(docs.items[0].historical,false);assert.equal((await s.get(n.id)).requirement_version,(n.requirement_version||1)+1);}
 assert.equal((await s.call('sop_work_materials',{id:other.id})).items.length,0);
 assert.equal((await s.call('sop_batch_work_materials',{id:other.id})).ok,false);
 const order=await s.call('v2_outbound_order_detail',{id:ob.id});assert.equal(order.attachments[0].id,f.id);assert.equal(order.order.warehouse_ack_required,1);assert.equal((await s.call('v2_outbound_order_list',{has_material:'1'})).items[0].material_count,1);
 assert.equal((await s.change('sop_work_material_remove',a.id,{attachment_id:f.id})).ok,false,'shared file cannot be removed in just one child');
 form.set('file',new File(['different'],'batch-work.csv',{type:'text/csv'}));assert.equal((await s.raw(form)).ok,false,'retry must be the same file');
 form.set('need_id',other.id);form.set('client_req_id',crypto.randomUUID());assert.equal((await s.raw(form)).ok,false,'cannot upload via unrelated need');
 const {uploadBatchMaterial,readBatchMaterials}=await import('../worker-v2/batch-work-materials.js');
 await assert.rejects(()=>uploadBatchMaterial(form,{...s.env,SOP_REQUEST_USER:{id:'F',name:'Fixture field',scope:'field',role:'dispatcher',departments:['bulk']}}),/仅入库|办公室/);
 await assert.rejects(()=>readBatchMaterials({action:'sop_batch_work_materials',id:a.id},s.env,{role:'viewer',departments:['import']}),/权限/);
});

test('historical attachments stay accessible from needs and downstream without copying; concurrent bookings cannot exceed capacity',async()=>{
 const s=setup();await s.login();const plan=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk']});const n=await s.need({source_type:'inbound',source_id:plan.id});
 s.DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,created_at) VALUES('OLD','inbound_plan',?,'inbound_material','old.csv','old-object',?)").run(plan.id,new Date().toISOString());
 const docs=await s.call('sop_work_materials',{id:n.id});assert.equal(docs.items[0].id,'OLD');assert.equal(docs.items[0].historical,true);
 const results=await Promise.all([s.schedule(n,7),s.schedule(n,7)]);assert.equal(results.filter(r=>r.ok).length,1);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n,1);const ob=results.find(r=>r.ok);assert.equal((await s.call('v2_outbound_order_detail',{id:ob.id})).attachments[0].id,'OLD');
 const search=await s.call('sop_work_need_search',{search:n.id});assert.equal(search.items[0].remaining,3);
});
