// Real authenticated Worker API with synthetic memory/local workerd data only.
import test from 'node:test';
import assert from 'node:assert/strict';
import {outboundFixture} from './outbound-page-material-fixture.mjs';

async function fixture(t){
 const f=await outboundFixture(t,{orders:10,native:process.env.CK_MATERIAL_NULL_NATIVE_D1==='true'}),id=f.oid(6),need='SYN-JSON-NULL';
 const state=value=>({status:'pending',source_type:'outbound',source_id:id,links:[],...(value===undefined?{}:{cargo_groups:value})});
 const set=value=>f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify(state(value)),need);
 const call=(action,body={})=>f.okTarget('office',action,body);
 const check=async(count,{readDetail=true,orderId=id}={})=>{
  const before=await f.snapshot(),list=await call('v2_outbound_order_list',{limit:200}),has=await call('v2_outbound_order_list',{limit:200,has_material:'1'}),none=await call('v2_outbound_order_list',{limit:200,has_material:'0'});
  assert.equal(list.items.find(x=>x.id===orderId).material_count,count);assert.equal(has.items.some(x=>x.id===orderId),count>0);assert.equal(none.items.some(x=>x.id===orderId),count===0);
  assert.equal(has.total+none.total,list.total);assert.equal(new Set([...has.items,...none.items].map(x=>x.id)).size,list.total);
  if(readDetail){const d=await call('v2_outbound_order_detail',{id:orderId}),a=await call('v2_attachment_list',{related_doc_type:'outbound_order',related_doc_id:orderId});assert.equal(d.order.material_count,count);assert.equal(a.items.filter(x=>x.attachment_category==='outbound_material').length,count);}
  assert.equal(await f.snapshot(),before,'reading material metadata must not change business or audit rows');
 };
 return {...f,id,need,state,set,check,call};
}
for(const [name,value]of [['JSON null',null],['false',false],['numeric zero',0],['empty string','']])test(name+' has the same legacy files in list, detail, attachment and both filters',async t=>{
 const f=await fixture(t);await f.set(value);await f.check(1);
});
test('a missing grouping field keeps its original legacy material semantics',async t=>{
 const f=await fixture(t);await f.set(undefined);await f.check(1);
});
test('truthy malformed grouping values cannot silently expose unselected library files',async t=>{
 const f=await fixture(t);
 for(const value of [{},[],true,1,'null','0','{"groups":[]}']){await f.set(value);await f.check(0);const before=await f.snapshot(),r=await f.callTarget('office','sop_cargo_groups',{id:f.need});assert.equal(r.ok,false,'invalid grouping remains a structural error');assert.equal(await f.snapshot(),before);}
});

test('a real saved group reservation freezes its selected/public files and excludes later library additions',async t=>{
 const f=await fixture(t),n=await f.call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYN-MATERIAL-GROUP',customer:'合成分组客户',title:'合成分组资料',instructions:'按本组资料作业'}),ids=['SYN-NULL-GROUP-SELECTED','SYN-NULL-GROUP-PUBLIC','SYN-NULL-GROUP-UNSELECTED'];
 for(const id of ids)await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key,file_name) VALUES(?,'sop_need',?,'shipping_document',?,'合成分组资料.pdf')",id,n.id,'synthetic/'+id);
 let row=(await f.rows('SELECT revision FROM sop_records WHERE id=?',n.id))[0];
 await f.call('sop_cargo_groups_save',{id:n.id,revision:row.revision,groups:[{name:'合成组',range:'SYN001',box_count:1,one_mark_one_box:true,attachment_ids:[ids[0],ids[0]]}],public_attachment_ids:[ids[1]]});
 let g=await f.call('sop_cargo_groups',{id:n.id});
 await f.call('sop_cargo_reservation_create',{id:n.id,revision:g.revision,group_id:g.cargo_groups.groups[0].id,reservation:{expected_ship_at:'2026-11-03',outbound_mode:'milk_express',destination:'合成目的地'}});
 const state=JSON.parse((await f.rows('SELECT state FROM sop_records WHERE id=?',n.id))[0].state),orderId=state.links[0].outbound_id,snapshot=structuredClone(state.links[0].cargo_reservation.groups);
 await f.check(2,{orderId});assert.equal((await f.call('sop_work_materials',{id:n.id})).items.length,3);
 await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES('SYN-LATER','sop_need',?,'shipping_document','synthetic/later')",n.id);await f.check(2,{orderId});
 g=await f.call('sop_cargo_groups',{id:n.id});assert.equal(g.files.length,4);
 assert.equal((await f.callTarget('office','sop_work_material_remove',{id:n.id,revision:g.revision,attachment_id:ids[0]})).ok,false,'referenced snapshots cannot be removed');
 assert.deepEqual(JSON.parse((await f.rows('SELECT state FROM sop_records WHERE id=?',n.id))[0].state).links[0].cargo_reservation.groups,snapshot);
});

test('approved frozen allocation snapshots keep public-file dedup and an explicit empty allocation takes precedence',async t=>{
 const f=await fixture(t);await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES('SYN-PUBLIC','sop_need',?,'shipping_document','synthetic/public')",f.need);
 await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES('SYN-NOT-SELECTED','sop_need',?,'shipping_document','synthetic/other')",f.need);
 const own={id:'SYN-JSON-NULL-FILE',file_key:'synthetic/shared'},common={id:'SYN-PUBLIC',file_key:'synthetic/public'},group={id:'G',mapping_version:1,current_output_id:'O',documents:[own],public_documents:[common],outputs:[{id:'O',version:1,mapping_version:1,status:'approved',quantity:1,unit:'箱'}]},selection={group_id:'G',output_id:'O',output_version:1,mapping_version:1,allocation_schema:2,output_quantity:1,quantity:1,unit:'箱',documents:[own,own],public_documents:[common,common]},data={...f.state(null),cargo_groups:{schema:1,groups:[group]},links:[{outbound_id:f.id,cargo_allocations:[selection]}]};
 await f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify(data),f.need);await f.check(2);assert.equal((await f.call('sop_cargo_groups',{id:f.need})).ok,true);
 data.links[0]={outbound_id:f.id,cargo_allocations:[],cargo_reservation:{groups:[selection]}};await f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify(data),f.need);await f.check(0);
});

test('legacy null reads deduplicate shared attachment IDs while separate IDs sharing a blob remain distinct',async t=>{
 const f=await fixture(t);await f.set(null);
 await f.run("INSERT INTO sop_records(id,kind,department,revision,state,updated_at) VALUES('SYN-SHARED-NEED','need','bulk',1,?,'2026-11-03T00:00:00Z')",JSON.stringify({...f.state(null)}));
 for(const id of ['SYN-SHARED-A','SYN-SHARED-B'])await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES(?,'outbound_order',?,'outbound_material','synthetic/shared')",id,f.id);
 await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES('SYN-PHOTO','outbound_order',?,'arrival_photo','synthetic/photo')",f.id);
 await f.check(3);
});

test('removing a null legacy need file updates both filters without deleting its original metadata',async t=>{
 const f=await fixture(t);await f.set(null);await f.check(1);const before=(await f.rows('SELECT * FROM v2_attachments WHERE id=?','SYN-JSON-NULL-FILE'))[0];
 await f.call('sop_work_material_remove',{id:f.need,revision:1,attachment_id:before.id});await f.check(0);
 assert.deepEqual((await f.rows('SELECT * FROM v2_attachments WHERE id=?',before.id))[0],before);
});

test('batch removal and restore preserve null legacy visibility and the original shared file',async t=>{
 const f=await fixture(t),plan=await f.call('v2_inbound_plan_create',{customer:'合成本批资料',biz_classes:['bulk']}),data={status:'pending',source_type:'inbound',source_id:plan.id,cargo_groups:null,links:[{outbound_id:f.id}],removed_material_ids:['SYN-JSON-NULL-FILE']};
 await f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify(data),f.need);
 await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES('SYN-NULL-BATCH','inbound_plan',?,'batch_work_material','synthetic/batch')",plan.id);await f.check(1);
 await f.call('sop_batch_work_material_remove',{id:f.need,attachment_id:'SYN-NULL-BATCH',material_revision:0});await f.check(0);
 await f.call('sop_batch_work_material_restore',{id:f.need,attachment_id:'SYN-NULL-BATCH',material_revision:1});await f.check(1);
 assert.equal((await f.rows("SELECT COUNT(*) n FROM v2_attachments WHERE id='SYN-NULL-BATCH'"))[0].n,1);assert.equal((await f.rows("SELECT COUNT(*) n FROM ck_batch_material_events WHERE attachment_id='SYN-NULL-BATCH'"))[0].n,2);
});

test('material filtering applies before every page with correct totals and stable order',async t=>{
 const f=await fixture(t);await f.set(null);const all=await f.call('v2_outbound_order_list',{limit:200});
 for(const value of ['0','1']){const expected=all.items.filter(row=>value==='1'?row.material_count>0:row.material_count===0),found=[];
  for(let offset=0;offset<=expected.length;offset+=2){const page=await f.call('v2_outbound_order_list',{has_material:value,limit:2,offset});assert.equal(page.total,expected.length);assert.equal(page.offset,offset);assert.deepEqual(page.items,expected.slice(offset,offset+2));found.push(...page.items);}
  assert.deepEqual(found,expected);
 }
});

test('verified field permissions and guest denial remain unchanged for null legacy metadata',async t=>{
 const f=await fixture(t);await f.set(null);const before=await f.snapshot();
 assert.equal((await f.okTarget('A','sop_work_materials',{id:f.need})).items.length,1);
 assert.equal((await f.callTarget('guest','v2_outbound_order_list',{})).ok,false);assert.equal((await f.callTarget('guest','v2_attachment_list',{related_doc_type:'outbound_order',related_doc_id:f.id})).ok,false);
 const p=(await f.ok('office','sop_attendance_register',{name:'合成受限资料派审',agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['direct_ship'],version:0,reason:'合成资料权限验证'});await f.ok('A','sop_login',{badge:p.badgeId});
 assert.equal((await f.callTarget('A','sop_work_materials',{id:f.need})).ok,false);assert.equal(await f.snapshot(),before);
});

test('active attendance/duty wrappers keep corrected reads read-only and legacy switches keep their old paths',async t=>{
 const f=await fixture(t);await f.set(null);f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';await f.call('v2_outbound_order_list',{customer_keyword:'SYN-ABSENT'});
 if(f.env.DB.metrics)f.env.DB.metrics.length=0;await f.check(1);if(f.env.DB.metrics)assert.ok(f.env.DB.metrics.every(m=>m.rows_written===0));
 for(const key of ['SOP_WORK_CHAIN_ENABLED','SOP_UPGRADE_ENABLED']){
  const old=f.env[key];f.env[key]='false';const r=await f.call('v2_outbound_order_list',{limit:200});assert.equal(r.items.find(x=>x.id===f.id).material_count,0);assert.equal((await f.call('v2_outbound_order_list',{has_material:'1',limit:200})).items.some(x=>x.id===f.id),false);f.env[key]=old;
 }
 await f.check(1);
});
