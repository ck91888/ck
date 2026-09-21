import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {guardLegacy} from '../worker-v2/sop.js';import {database} from './d1-adapter.mjs';
function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_AUTO_OUTBOUND:'true',SOP_ROLLOUT_DEPARTMENTS:'bulk',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'测试负责人',role:'manager',key:'fixture-only-authorization'}])};let cookie='';
 async function raw(body,override={}){return worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...override},body:JSON.stringify(body)}),env);}
 async function call(action,data={}){return (await raw({action,client_req_id:crypto.randomUUID(),...data})).json();}
 const login=async()=>{const r=await raw({action:'sop_login',sop_key:'fixture-only-authorization'});assert.equal((await r.clone().json()).ok,true);cookie=r.headers.get('set-cookie').split(';')[0];return r;};
 const get=async id=>{const r=await call('sop_get',{id});assert.equal(r.ok,true,r.error);return r.record;};
 const change=async(action,id,data={})=>call(action,{id,revision:(await get(id)).revision,...data});
 return {DB,env,raw,call,login,get,change};
}
test('shared cookie authorizes original modules; bad cookies and production remain locked',async()=>{
 const {call,login,raw,env}=setup();assert.equal((await call('v2_auth_check')).ok,false);
 const r=await login();assert.match(r.headers.get('set-cookie'),/HttpOnly; Secure; SameSite=Strict/);
 for(const a of ['v2_auth_check','v2_dashboard_summary','v2_003_dashboard','v2_dashboard_realtime_overview']){const x=await call(a);assert.equal(x.ok,true,a+': '+x.error);}
 const bad=await raw({action:'v2_auth_check'},{Cookie:'ck_sop_session=fake.fake'});assert.equal((await bad.json()).ok,false);
 env.SOP_ENVIRONMENT='production';assert.equal((await call('v2_auth_check')).ok,false);
});
test('fictional fixtures are idempotent and visible in original inbound issue and verification lists',async()=>{
 const {login,call,DB}=setup();await login();const a=await call('sop_demo_prepare');assert.equal(a.ok,true,a.error);
 const b=await call('sop_demo_prepare');assert.equal(b.ok,true,b.error);assert.equal(a.inbound_id,b.inbound_id);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);
 const ib=await call('v2_inbound_plan_detail',{id:a.inbound_id});assert.equal(ib.ok,true,ib.error);assert.ok(ib.plan.unload_completed_at);
 for(const [action,id] of [['v2_issue_detail',a.issue_id],['v2_verify_batch_detail',a.batch_id]])assert.equal((await call(action,{id})).ok,true);
});
test('native responsible dispatch records actual staff, not manager; original completion closes the team',async()=>{
 const {login,call,DB}=setup();await login();
 const ib=await call('v2_inbound_plan_create',{customer:'虚拟客户',biz_class:'bulk',biz_classes:['bulk'],lines:[{unit_type:'box',planned_qty:2}]});assert.equal(ib.ok,true,ib.error);
 const payload={action:'v2_unload_job_start',client_req_id:'native-start',plan_id:ib.id,biz_class:'bulk'};
 const start=await call('sop_native_start',{payload,workers:[{id:'A',name:'甲'},{id:'B',name:'乙'}],lead_id:'A',estimated_minutes:10});assert.equal(start.ok,true,start.error);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(start.job_id).n,2);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE worker_id='M'").get().n,0);
 const repeat=await call('sop_native_start',{payload,workers:[{id:'A',name:'甲'},{id:'B',name:'乙'}],lead_id:'A',estimated_minutes:10});assert.equal(repeat.job_id,start.job_id);assert.equal(DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE record_id=? AND action='sop_native_start'").get(start.job_id).n,1);
 const finish=await call('v2_unload_job_finish',{job_id:start.job_id,worker_id:'A',complete_job:true,result_lines:[{unit_type:'box',actual_qty:2}],box_count:2});assert.equal(finish.ok,true,finish.error);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(start.job_id).n,0);
 assert.ok(DB.raw.prepare('SELECT unload_completed_at FROM v2_inbound_plans WHERE id=?').get(ib.id).unload_completed_at);
});
test('inbound requirement -> dispatched work -> original results -> 60+40 outbound with no duplicate operation',async()=>{
 const {login,call,get,change,DB,env}=setup();await login();const seed=await call('sop_demo_prepare');assert.equal(seed.ok,true,seed.error);
 const t=await call('sop_task_create',{department:'bulk',need_id:seed.need_id,title:'验收贴标',job_type:'bulk_op',workers:[{id:'A',name:'甲'},{id:'B',name:'乙'}],lead_id:'A',estimated_minutes:30});assert.equal(t.ok,true,t.error);
 assert.equal((await change('sop_task_start',t.id)).ok,true);
 assert.equal((await change('sop_task_finish',t.id,{result:{quantity:100,unit:'箱',label_count:100,operated_box_count:100,description:'贴标100箱',location:'内场-验收位'}})).ok,true);
 assert.equal((await change('sop_task_review',t.id,{decision:'pass',reason:'全检通过'})).ok,true);
 assert.equal((await get(seed.need_id)).status,'waiting_customer');
 const base={customer:'虚拟验收客户',biz_class:'bulk',outbound_mode:'customer_pickup',uses_stock_operation:0,sop_existing_need_id:seed.need_id,expected_ship_at:'2026-09-25',created_by:'测试客服'};
 const ob=await call('v2_outbound_order_create',{...base,sop_link_quantity:60,planned_box_count:60});assert.equal(ob.ok,true,ob.error);
 const detail=await call('v2_outbound_order_detail',{id:ob.id});assert.equal(detail.sop_needs[0].id,seed.need_id);assert.equal(detail.order.source_inbound_plan_id,seed.inbound_id);
 assert.equal(await guardLegacy({action:'v2_outbound_load_start',order_id:ob.id},env),null,'60 allocated boxes must not wait for the other 40');
 assert.equal((await call('v2_outbound_order_create',{...base,sop_link_quantity:41})).ok,false);
 const ob2=await call('v2_outbound_order_create',{...base,sop_link_quantity:40});assert.equal(ob2.ok,true,ob2.error);
 assert.equal((await get(seed.need_id)).status,'linked');
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need'").get().n,1);
 const result=DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(t.id);assert.equal(JSON.parse(result.result_json).label_count,100);
});
test('native issue can change during work and cannot finish without acknowledging latest version',async()=>{
 const {login,call,get,change,DB}=setup();await login();
 const issue=await call('v2_issue_create',{biz_class:'bulk',customer:'虚拟客户',issue_description:'检查标签'});
 const start=await call('sop_native_start',{payload:{action:'v2_issue_handle_start',issue_id:issue.id,client_req_id:'native-issue'},workers:[{id:'A',name:'甲'}],lead_id:'A',estimated_minutes:5});assert.equal(start.ok,true,start.error);
 const adopted=await call('sop_issue_adopt',{legacy_id:issue.id,native:true});assert.equal(adopted.ok,true,adopted.error);
 assert.equal((await change('sop_issue_append',adopted.id,{message:'追加检查箱唛'})).ok,true);
 assert.equal((await change('sop_issue_feedback',adopted.id,{message:'已完成'})).ok,false);
 const updates=await call('sop_updates');assert.equal(updates.items[0].legacy_id,issue.id);
 const record=await get(adopted.id);assert.equal((await change('sop_issue_ack',adopted.id,{requirement_version:record.requirement_version})).ok,true);
 assert.equal((await change('sop_issue_feedback',adopted.id,{message:'已按最新要求完成'})).ok,true);
 const original=await call('v2_issue_detail',{id:issue.id});assert.equal(original.issue.status,'responded');assert.match(original.issue.issue_description,/箱唛/);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(start.job_id).n,0);
});
test('existing uploaded batch retains identity and evidence while items and rounds change',async()=>{
 const {login,call,get,change,DB}=setup();await login();const seed=await call('sop_demo_prepare');
 const r=await call('sop_check_adopt',{ship_date:'2026-09-25',legacy_id:seed.batch_id});assert.equal(r.ok,true,r.error);
 assert.equal((await change('sop_check_scan',r.id,{barcode:'TEST-BOX-001',pallet:'P1'})).ok,true);
 assert.equal((await change('sop_check_round',r.id,{slot:'11:00',reason:'第一轮'})).ok,true);
 assert.equal((await change('sop_check_items',r.id,{items:[{barcode:'TEST-BOX-003',customer:'虚拟验收客户',qty:1}]})).ok,true);
 const x=await get(r.id);assert.equal(x.legacy_id,seed.batch_id);assert.equal(x.summary.added,1);
 const removed=x.items.find(x=>x.barcode==='TEST-BOX-002');assert.equal((await change('sop_check_remove',r.id,{item_id:removed.id,reason:'取消出库'})).ok,true);
 const list=await call('v2_verify_batch_list');const batch=list.items.find(x=>x.id===seed.batch_id);assert.equal(batch.planned_qty,3);assert.equal(batch.scanned_ok_count,1);
 assert.equal((await change('sop_check_items',r.id,{duplicate_mode:'replace',reason:'数量修订',items:[{barcode:'TEST-BOX-001',customer:'虚拟验收客户',qty:1}]})).ok,true);
 const refreshed=await call('v2_verify_batch_detail',{id:seed.batch_id});assert.equal(refreshed.summary.scanned_ok_total_count,0);
 const next=await call('v2_verify_batch_list');assert.equal(next.items.find(x=>x.id===seed.batch_id).scanned_ok_count,0);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_verify_scan_logs WHERE batch_id=?').get(seed.batch_id).n,1);
});
