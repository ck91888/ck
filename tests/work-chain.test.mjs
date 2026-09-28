import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {guardWorkChain,chainNeed} from '../worker-v2/work-chain.js';
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
test('one inbound can start multiple requirements and bookings in one transaction',async()=>{
 const s=setup();await s.login();const plan=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:20}],work_requests:[{title:'Palletize',instructions:'Palletize ten',department:'bulk',planned_quantity:10,planned_unit:'箱',outbounds:[{quantity:5,expected_ship_at:'2026-10-03',outbound_mode:'customer_pickup'}]},{title:'Direct forward',instructions:'Forward unopened cartons',department:'bulk',operation_kind:'direct_forward',planned_quantity:10,planned_unit:'箱'}]});
 assert.equal(plan.ok,true,plan.error);assert.equal(plan.needs.length,2);assert.equal(plan.outbounds.length,1);
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
test('historical attachments stay accessible from needs and downstream without copying; concurrent bookings cannot exceed capacity',async()=>{
 const s=setup();await s.login();const plan=await s.call('v2_inbound_plan_create',{customer:'Fixture customer',biz_classes:['bulk']});const n=await s.need({source_type:'inbound',source_id:plan.id});
 s.DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,created_at) VALUES('OLD','inbound_plan',?,'inbound_material','old.csv','old-object',?)").run(plan.id,new Date().toISOString());
 const docs=await s.call('sop_work_materials',{id:n.id});assert.equal(docs.items[0].id,'OLD');assert.equal(docs.items[0].historical,true);
 const results=await Promise.all([s.schedule(n,7),s.schedule(n,7)]);assert.equal(results.filter(r=>r.ok).length,1);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n,1);const ob=results.find(r=>r.ok);assert.equal((await s.call('v2_outbound_order_detail',{id:ob.id})).attachments[0].id,'OLD');
 const search=await s.call('sop_work_need_search',{search:n.id});assert.equal(search.items[0].remaining,3);
});
