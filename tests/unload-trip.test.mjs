import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
async function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_AUTO_OUTBOUND:'true',SOP_ROLLOUT_DEPARTMENTS:'bulk',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'测试派审员',role:'manager',key:'fixture'}])};let cookie='';
 async function raw(body){return worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body:JSON.stringify(body)}),env);}
 const login=await raw({action:'sop_login',sop_key:'fixture'});cookie=login.headers.get('set-cookie').split(';')[0];
 const call=async(action,data={})=>(await raw({action,client_req_id:crypto.randomUUID(),...data})).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,r.error);return r;};
 const plan=async(name,biz=['bulk'],extra={})=>ok('v2_inbound_plan_create',{customer:name,biz_class:biz[0],biz_classes:biz,lines:[{unit_type:'carton',planned_qty:10}],...extra});
 const start=(ids,extra={})=>ok('sop_native_start',{payload:{action:'v2_unload_job_start',client_req_id:crypto.randomUUID(),plan_ids:ids},workers:[{id:'A',name:'甲'},{id:'B',name:'乙'},{id:'C',name:'丙'}],lead_id:'A',estimated_minutes:20,...extra});
 const result=async(job,values=[])=>{const d=await ok('v2_ops_job_detail',{job_id:job});return d.unload_plans.map((p,i)=>({plan_id:p.plan.id,lines:p.lines.filter(l=>l.unit_type!=='courier').map(l=>({line_id:l.id,actual_qty:values[i]??l.planned_qty}))}));};
 return {DB,env,call,ok,plan,start,result};
}
test('one truck with three plans makes one labor job; per-plan receipts survive list/detail/candidate repairs',async()=>{
 const {DB,ok,plan,start,result}=await setup();const a=await plan('客户甲'),b=await plan('客户乙',['return']),c=await plan('客户丙',['change_order']);
 const job=await start([a.id,b.id,c.id]);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,3);
 for(const p of [a,b,c]){const d=await ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.plan.status,'unloading');assert.equal(d.jobs[0].id,job.job_id);}
 await ok('v2_inbound_plan_ops_candidates',{scene:'unload'});assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_inbound_plans WHERE status='unloading'").get().n,3);
 DB.raw.prepare("UPDATE v2_ops_job_workers SET joined_at=?").run(new Date(Date.now()-30*60000).toISOString());
 const plan_results=await result(job.job_id,[10,12,8]);await ok('v2_unload_job_finish',{job_id:job.job_id,complete_job:true,worker_id:'A',plan_results});
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,1);assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,0);
 assert.equal(DB.raw.prepare('SELECT SUM(minutes_worked) n FROM v2_ops_job_workers').get().n,90);
 for(const [i,p] of [a,b,c].entries()){const d=await ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.plan.status,'completed');assert.equal(d.unload_summary.result_lines[0].actual_qty,[10,12,8][i]);}
 const total=JSON.parse(DB.raw.prepare('SELECT shared_result_json FROM v2_ops_jobs').get().shared_result_json);assert.equal(total.box_count,30);
 await ok('v2_unload_job_finish',{job_id:job.job_id,complete_job:true,worker_id:'A',plan_results});assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,1);
});
test('scans resolve plan numbers, IDs, QR links, and A1/A2 to one plan; invalid/completed codes are rejected',async()=>{
 const {plan,ok,call,start}=await setup();const a=await plan('同一批',['direct_ship'],{external_inbound_nos:['A1','A2']});
 for(const code of [a.id,a.display_no,'A1','A2','https://example.test/002/?inbound='+a.id]){const r=await ok('v2_inbound_plan_find_by_code',{code,scene:'unload_trip'});assert.equal(r.plan.id,a.id);}
 assert.equal((await call('v2_inbound_plan_find_by_code',{code:'不存在',scene:'unload_trip'})).ok,false);
 await start([a.id]);assert.equal((await call('v2_inbound_plan_find_by_code',{code:'A2',scene:'unload_trip'})).ok,false);
});
test('mixed direct-ship A1/A2 remains open until both bills finish, while other truck plans auto-complete',async()=>{
 const {plan,start,ok,result}=await setup();const a=await plan('理货',['direct_ship'],{external_inbound_nos:['A1','A2']}),b=await plan('直出');const j=await start([b.id,a.id]);
 await ok('v2_unload_job_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,plan_results:await result(j.job_id)});
 assert.equal((await ok('v2_inbound_plan_detail',{id:a.id})).plan.status,'arrived_pending_putaway');
 assert.equal((await ok('v2_inbound_plan_detail',{id:b.id})).plan.status,'completed');
 for(const [i,code] of ['A1','A2'].entries()){
  const k=await ok('sop_native_start',{payload:{action:'v2_inbound_job_start',client_req_id:crypto.randomUUID(),plan_id:a.id,job_type:'inbound_direct',biz_class:'direct_ship',external_inbound_no:code},workers:[{id:'A',name:'甲'}],lead_id:'A',estimated_minutes:10});
  await ok('v2_inbound_job_finish',{job_id:k.job_id,worker_id:'A',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:5}],box_count:5});
  assert.equal((await ok('v2_inbound_plan_detail',{id:a.id})).plan.status,i===0?'partially_completed':'completed');
 }
});
test('start retries retain the same team; overlap and stale selections cannot create duplicate jobs',async()=>{
 const {DB,plan,start,call}=await setup();const a=await plan('甲'),b=await plan('乙'),c=await plan('丙');const payload={action:'v2_unload_job_start',client_req_id:'same-start',plan_ids:[a.id,b.id]};
 const j=await start([a.id,b.id],{payload});const again=await start([a.id,b.id],{payload});assert.equal(again.job_id,j.job_id);
 const denied=await call('sop_native_start',{payload:{action:'v2_unload_job_start',client_req_id:'another',plan_ids:[b.id,c.id]},workers:[{id:'D',name:'丁'}],lead_id:'D',estimated_minutes:10});assert.equal(denied.ok,false);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);assert.equal(DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(c.id).status,'pending');
 assert.throws(()=>DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,status) VALUES('LEGACY','unload','inbound_plan',?,'working')").run(b.id),/unload_plan_busy/);
});
test('invalid per-plan quantities and missing plans do not close a person or partially update receipts',async()=>{
 const {DB,plan,start,result,call,ok}=await setup();const a=await plan('甲'),b=await plan('乙');const j=await start([a.id,b.id]);const good=await result(j.job_id);
 for(const bad of [good.slice(0,1),[good[0],good[0]],good.map(x=>({...x,lines:x.lines.map(l=>({...l,actual_qty:-1}))}))]){
  assert.equal((await call('v2_unload_job_finish',{job_id:j.job_id,complete_job:true,worker_id:'A',plan_results:bad})).ok,false);
  assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,3);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,0);
 }
 assert.equal((await call('v2_ops_job_finish',{job_id:j.job_id,worker_id:'A'})).ok,false);
 const d=await ok('v2_ops_job_detail',{job_id:j.job_id});assert.equal(d.can_manage_dispatch,true);assert.equal(d.unload_plans.length,2);
});
test('transaction failure rolls back the entire start, including first plan and idempotency key',async()=>{
 const {DB,plan,call}=await setup();const a=await plan('甲'),b=await plan('乙');DB.raw.exec(`CREATE TRIGGER fixture_failure BEFORE INSERT ON ck_unload_plan_links WHEN NEW.position=1 BEGIN SELECT RAISE(ABORT,'fixture failure'); END`);
 const failed=await call('sop_native_start',{payload:{action:'v2_unload_job_start',client_req_id:'atomic-failure',plan_ids:[a.id,b.id]},workers:[{id:'A',name:'甲'}],lead_id:'A',estimated_minutes:10});assert.equal(failed.ok,false);
 for(const table of ['v2_ops_jobs','v2_ops_job_workers','ck_unload_plan_links','ck_unload_trips'])assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM '+table).get().n,0);
 assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_inbound_plans WHERE status='pending'").get().n,2);
});
test('temporary unloading assigned to three people can be reopened and finished by its dispatcher',async()=>{
 const {DB,ok}=await setup();const j=await ok('sop_native_start',{payload:{action:'v2_unplanned_unload_start',client_req_id:'temporary-three',cargo_summary:'虚拟临时卸货'},workers:[{id:'TEST-A',name:'测试甲'},{id:'TEST-B',name:'测试乙'},{id:'TEST-C',name:'测试丙'}],lead_id:'TEST-A',estimated_minutes:10});
 const reopened=await ok('v2_ops_job_detail',{job_id:j.job_id});assert.equal(reopened.can_manage_dispatch,true);assert.equal(reopened.job.active_worker_count,3);assert.equal(reopened.job.related_doc_type,'field_feedback');
 await ok('v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:'TEST-A',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:30}]});
 assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,0);assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'completed');
});
