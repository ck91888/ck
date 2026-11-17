import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {resetAction,STAGING_DATABASE} from '../worker-v2/test-data-reset.js';
import {ATTENDANCE_SCHEMA} from '../worker-v2/attendance.js';
async function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'测试管理员',role:'manager',key:'fixture'},{id:'S',name:'测试客服',role:'service',departments:['bulk'],key:'service'},{id:'W',name:'只读',role:'viewer',key:'viewer'}])};let cookie='';
 async function raw(b,options={}){return worker.fetch(new Request('https://fixture.local/api'+(options.method==='GET'?'?'+new URLSearchParams(b):''),{method:options.method||'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...options.headers},...(options.method==='GET'?{}:{body:JSON.stringify(b)})}),env);}
 const login=async(key='fixture')=>{const r=await raw({action:'sop_login',sop_key:key});cookie=r.headers.get('set-cookie')?.split(';')[0]||'';};await login();
 const call=async(action,data={},opt)=>(await raw({action,client_req_id:crypto.randomUUID(),...data},opt)).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,r.error);return r;};
 const plan=(extra={})=>ok('v2_inbound_plan_create',{customer:'虚拟客户',biz_class:'bulk',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:50}],...extra});
 const unload=async(n=50)=>{const j=await ok('sop_native_start',{payload:{action:'v2_unplanned_unload_start',cargo_summary:'虚拟未识别货物',client_req_id:crypto.randomUUID()},workers:[{id:'A',name:'甲'},{id:'B',name:'乙'},{id:'C',name:'丙'}],lead_id:'A',estimated_minutes:15});DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-20*60000).toISOString(),j.job_id);if(n!==null)await ok('v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,result_lines:Array.isArray(n)?n:[{unit_type:'carton',actual_qty:n}],remark:'原卸货备注'});return {job_id:j.job_id,feedback_id:DB.raw.prepare('SELECT related_doc_id FROM v2_ops_jobs WHERE id=?').get(j.job_id).related_doc_id};};
 const preview=(f,p)=>ok('sop_feedback_link_preview',{feedback_id:f.feedback_id,plan_id:p.id});
 const payload=(f,r)=>({feedback_id:f.feedback_id,plan_id:r.plan.id,feedback_version:r.feedback_version,plan_version:r.plan_version,job_version:r.job_version,lines:r.lines.map(l=>({line_id:l.id,unit_type:l.unit_type,actual_qty:l.actual_qty}))});
 return {DB,env,call,ok,plan,unload,preview,payload,login};
}
test('completed temporary unload attaches to an existing plan without new jobs, output, or labor',async()=>{
 const {DB,plan,unload,preview,payload,ok,call}=await setup();const p=await plan(),f=await unload(),r=await preview(f,p);
 const before={workers:DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all(),results:DB.raw.prepare('SELECT * FROM v2_ops_job_results').all(),job:DB.raw.prepare('SELECT * FROM v2_ops_jobs').get()};
 const out=await ok('sop_feedback_link_save',payload(f,r));assert.equal(out.status,'completed');
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);
 assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all(),before.workers);assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_results').all(),before.results);
 assert.equal(DB.raw.prepare('SELECT updated_at FROM v2_ops_jobs').get().updated_at,before.job.updated_at);
 const d=await ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.unload_summary.completed,true);assert.equal(d.unload_summary.result_lines[0].actual_qty,50);assert.equal(d.unload_summary.total_minutes,60);assert.equal(d.jobs[0].id,f.job_id);assert.equal(d.existing_feedback_link.feedback_id,f.feedback_id);
 assert.equal(d.plan.source_feedback_id,'');assert.equal(d.plan.source_type,'manual');assert.equal(d.lines[0].planned_qty,50);
 const fd=await ok('v2_feedback_detail',{id:f.feedback_id});assert.equal(fd.feedback.status,'converted');assert.equal(fd.existing_plan_link.plan_id,p.id);assert.equal(fd.job_results.length,1);
 assert.equal((await ok('sop_feedback_link_save',payload(f,r))).already_linked,true);
 const other=await plan();assert.equal((await call('sop_feedback_link_save',{...payload(f,r),plan_id:other.id})).ok,false);
 assert.equal((await call('v2_feedback_finalize_to_inbound',{feedback_id:f.feedback_id,customer:'重复',biz_class:'bulk'})).ok,false);
 assert.equal((await call('v2_feedback_delete',{id:f.feedback_id})).ok,false);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,3);
});
test('all exempt classes complete while multi-bill putaway still requires A1 and A2',async()=>{
 for(const biz of ['bulk','return','change_order','direct_ship']){
  const {plan,unload,preview,payload,ok}=await setup();const p=await plan({biz_class:biz,biz_classes:biz==='direct_ship'?['direct_ship','bulk']:[biz],...(biz==='direct_ship'?{external_inbound_nos:['A1','A2']}:{})});const f=await unload();const linked=await ok('sop_feedback_link_save',payload(f,await preview(f,p)));
  if(biz!=='direct_ship'){assert.equal(linked.status,'completed');continue;}
  assert.equal(linked.status,'partially_completed');
  for(const [i,code] of ['A1','A2'].entries()){const j=await ok('sop_native_start',{payload:{action:'v2_inbound_job_start',plan_id:p.id,job_type:'inbound_direct',biz_class:'direct_ship',external_inbound_no:code,client_req_id:crypto.randomUUID()},workers:[{id:'A',name:'甲'}],lead_id:'A',estimated_minutes:10});await ok('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:25}],box_count:25});const d=await ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.plan.status,i?'completed':'partially_completed');}
 }
});
test('quantity differences preserve planned quantities; duplicate unit lines require exact allocation',async()=>{
 const {DB,plan,unload,preview,payload,ok,call}=await setup();const p=await plan({lines:[{unit_type:'carton',planned_qty:20},{unit_type:'carton',planned_qty:30}]}),f=await unload([{unit_type:'carton',actual_qty:48},{unit_type:'pallet',actual_qty:2}]);const r=await preview(f,p);assert.equal(r.lines[0].actual_qty,null);assert.equal(r.lines[2].planned_qty,0);
 const b=payload(f,r);assert.equal((await call('sop_feedback_link_save',b)).ok,false);
 b.lines[0].actual_qty=20;b.lines[1].actual_qty=30;assert.equal((await call('sop_feedback_link_save',b)).ok,false);
 b.lines[1].actual_qty=28;await ok('sop_feedback_link_save',b);
 const d=await ok('v2_inbound_plan_detail',{id:p.id});assert.deepEqual(d.lines.map(x=>x.planned_qty),[20,30,0]);assert.deepEqual(d.lines.map(x=>x.actual_qty),[20,28,2]);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,1);
});
test('unfinished, deleted, occupied, completed, accounted, or stale records cannot link',async()=>{
 const {DB,plan,unload,preview,payload,ok,call}=await setup();const p=await plan(),active=await unload(null);assert.equal((await call('sop_feedback_link_preview',{feedback_id:active.feedback_id,plan_id:p.id})).ok,false);
 await ok('v2_unplanned_unload_finish',{job_id:active.job_id,worker_id:'A',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]});
 const r=await preview(active,p);for(const change of ["status='cancelled'","is_deleted=1","accounted=1","status='completed'","updated_at='new-version'"]){DB.raw.prepare('UPDATE v2_inbound_plans SET '+change+' WHERE id=?').run(p.id);assert.equal((await call('sop_feedback_link_save',payload(active,r))).ok,false);DB.raw.prepare("UPDATE v2_inbound_plans SET status='pending',is_deleted=0,accounted=0,updated_at=? WHERE id=?").run(r.plan_version,p.id);}
 await ok('sop_native_start',{payload:{action:'v2_unload_job_start',plan_ids:[p.id],client_req_id:crypto.randomUUID()},workers:[{id:'A',name:'甲'}],lead_id:'A',estimated_minutes:10});assert.equal((await call('sop_feedback_link_save',payload(active,r))).ok,false);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_feedback_plan_links').get().n,0);
});
test('transaction rollback and concurrent receipt/conversion retain the original source',async()=>{
 const {DB,plan,unload,preview,payload,call,ok}=await setup();const p=await plan(),f=await unload(),r=await preview(f,p),b=payload(f,r);
 DB.raw.exec("CREATE TRIGGER fault BEFORE UPDATE ON v2_field_feedbacks BEGIN SELECT RAISE(ABORT,'fixture failure'); END");assert.equal((await call('sop_feedback_link_save',b)).ok,false);
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_feedback_plan_links').get().n,0);assert.equal(DB.raw.prepare('SELECT related_doc_type FROM v2_ops_jobs').get().related_doc_type,'field_feedback');assert.equal(DB.raw.prepare('SELECT actual_qty FROM v2_inbound_plan_lines').get().actual_qty,0);DB.raw.exec('DROP TRIGGER fault');
 const original=DB.batch;let once=true;DB.batch=async statements=>{if(once&&statements.some(x=>x.sql.startsWith('INSERT INTO ck_feedback_plan_links'))){once=false;DB.raw.prepare("UPDATE v2_inbound_plans SET accounted=1 WHERE id=?").run(p.id);}return original(statements);};assert.equal((await call('sop_feedback_link_save',b)).ok,false);DB.batch=original;DB.raw.prepare('UPDATE v2_inbound_plans SET accounted=0 WHERE id=?').run(p.id);
 await ok('sop_feedback_link_save',b);assert.throws(()=>DB.raw.prepare("INSERT INTO v2_inbound_plans(id,source_feedback_id) VALUES('duplicate',?)").run(f.feedback_id),/feedback_already_linked/);
});
test('mixed courier items remain outstanding after truck feedback is attributed',async()=>{
 const {plan,unload,preview,payload,ok}=await setup();const p=await plan({lines:[{unit_type:'carton',planned_qty:50},{unit_type:'courier',planned_qty:1,tracking_nos:['5000000001111']}]});const f=await unload();const out=await ok('sop_feedback_link_save',payload(f,await preview(f,p)));assert.equal(out.status,'arrived_pending_putaway');const d=await ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.courier_progress,undefined);assert.equal(d.plan.courier_progress.received,0);assert.equal(d.lines.find(x=>x.unit_type==='courier').actual_qty,0);
});
test('searches are paginated and support customer, plan number and external bill; only office can write',async()=>{
 const {plan,unload,ok,call,login,preview,payload}=await setup();const p=await plan({customer:'虚拟唯一客户',biz_class:'direct_ship',biz_classes:['direct_ship'],external_inbound_nos:['EXT-001','EXT-002']}),f=await unload();for(const keyword of ['唯一',p.display_no,'EXT-002']){const r=await ok('sop_feedback_link_candidates',{feedback_id:f.feedback_id,keyword});assert.equal(r.items[0].id,p.id);}
 const b=payload(f,await preview(f,p));assert.equal((await call('sop_feedback_link_save',b,{method:'GET'})).ok,false);assert.equal((await call('sop_feedback_link_save',b,{headers:{Origin:'https://unrelated.local'}})).ok,false);
 await login('viewer');assert.equal((await call('sop_feedback_link_save',b)).ok,false);await login('service');await ok('sop_feedback_link_save',b);
});
test('linked records participate in backup/reset and concurrent cleanup cannot delete their labor',async()=>{
 const {DB,env,plan,unload,preview,payload,ok,call}=await setup();const p=await plan(),f=await unload(),b=payload(f,await preview(f,p));
 const batch=DB.batch;let race=true;DB.batch=async statements=>{if(race&&statements.some(s=>s.sql.startsWith('DELETE FROM v2_ops_job_workers'))){race=false;await ok('sop_feedback_link_save',b);}return batch(statements);};
 assert.equal((await call('v2_feedback_delete',{id:f.feedback_id})).ok,false);DB.batch=batch;assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,3);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results').get().n,1);
 for(const sql of ATTENDANCE_SCHEMA)DB.raw.exec(sql);
 Object.assign(env,{SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE});const manager={id:'M',role:'manager'};const {run}=await resetAction('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'},env,manager);let done;for(let i=0;i<30;i++){done=await resetAction('step',{runId:run.id},env,manager);if(done.run.completedAt)break;}
 assert.ok(done.run.completedAt);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_feedback_plan_links').get().n,0);assert.equal(DB.raw.prepare(`SELECT COUNT(*) n FROM ck_backup_${run.id}_ck_feedback_plan_links`).get().n,1);assert.equal(DB.raw.prepare(`SELECT COUNT(*) n FROM ck_backup_${run.id}_v2_ops_job_workers`).get().n,3);
});
