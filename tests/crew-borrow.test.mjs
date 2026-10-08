import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {loadFixture} from './outbound-load-fixture.mjs';
import {digest} from '../worker-v2/access-control.js';
import {crewBorrowEnabled,ensureCrewBorrow} from '../worker-v2/crew-borrow.js';
import {TABLES,resetAction} from '../worker-v2/test-data-reset.js';

async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://isolated.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data={})=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 const days=[];for(const name of ['借调甲','留岗乙','目的丙'])days.push((await ok('sop_attendance_checkin',{name,agency:'가온'})).record);
 const staff=days.map(d=>({id:d.badgeId,name:d.name}));
 const start=(action,data={},workers=staff.slice(0,2))=>ok('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...data},workers,lead_id:workers[0].id,estimated_minutes:15,labor_department:'bulk'});
 const source=()=>start('v2_ops_job_start',{job_type:'load_import',flow_stage:'import',biz_class:'import'});
 const segments=id=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY rowid').all(id);
 const live=id=>segments(id).filter(s=>!s.left_at);
 const loans=()=>DB.raw.prepare('SELECT * FROM ck_crew_borrows ORDER BY rowid').all();
 const confirm=async(person,payload,job_id)=>{const a=await ok('sop_crew_availability',{worker_id:person.id,payload,job_id});assert.equal(a.can_borrow,true);return {worker_id:person.id,source_job_id:a.source.job_id,source_segment_id:a.source.segment_id,source_revision:a.source.revision};};
 const request=async(workers=[staff[0]],payload={action:'v2_unplanned_unload_start',biz_class:'bulk',cargo_summary:'合成借调卸货',client_req_id:crypto.randomUUID()})=>({payload,workers,lead_id:workers[0].id,estimated_minutes:15,labor_department:'bulk',borrow_confirmations:await Promise.all(workers.map(w=>confirm(w,payload)))});
 const finish=(job)=>ok('v2_unplanned_unload_finish',{job_id:job.job_id,worker_id:staff[0].id,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:10}]});
 const record=id=>DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(id);
 const change=(action,id,data={})=>ok(action,{id,revision:record(id).revision,...data});
 const task=async(workers=staff.slice(0,2))=>{const n=await ok('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'BORROW-'+crypto.randomUUID(),title:'合成原任务',customer:'合成客户',instructions:'合成打托'}),t=await ok('sop_task_create',{department:'bulk',need_id:n.id,title:'合成原任务',job_type:'bulk_op',workers,lead_id:workers[0].id,estimated_minutes:15});await change('sop_task_start',t.id);return {job_id:t.id};};
 return {DB,env,call,ok,days,staff,start,source,segments,live,loans,confirm,request,finish,record,change,task};
}

test('availability and cancelled confirmation do not alter any clock; borrow closes only selected person',async()=>{
 const f=await fixture(),s=await f.source(),prior=f.segments(s.job_id),req=await f.request();
 assert.deepEqual(f.segments(s.job_id),prior);assert.equal(f.loans().length,0);
 const j=await f.ok('sop_native_start',req),after=f.segments(s.job_id),target=f.live(j.job_id);
 assert.equal(j.has_crew_borrows,true);assert.equal(after[0].left_at,target[0].joined_at);assert.deepEqual(after[1],prior[1]);assert.equal(f.live(s.job_id).length,1);
 assert.deepEqual(JSON.parse(f.record(s.job_id).state).workers,f.staff.slice(0,2));assert.equal(f.loans()[0].status,'borrowed');
 await f.finish(j);assert.equal(f.live(s.job_id).length,2);assert.equal(f.loans()[0].status,'returned');
 const own=f.segments(s.job_id).filter(w=>w.worker_id===f.staff[0].id);assert.equal(own.length,2);assert.ok(own[1].joined_at>=f.segments(j.job_id)[0].left_at);assert.deepEqual(f.segments(s.job_id)[1],prior[1]);
 await f.finish(j);await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.segments(s.job_id).length,3);
});

test('all staff can be lent without finishing source; same request replays after commit and changed payload is rejected',async()=>{
 const f=await fixture(),s=await f.source(),req=await f.request(f.staff.slice(0,2)),j=await f.ok('sop_native_start',req);
 assert.equal(f.live(s.job_id).length,0);assert.equal(f.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(s.job_id).status,'awaiting_close');
 const repeat=await f.ok('sop_native_start',req);assert.equal(repeat.job_id,j.job_id);assert.equal(f.loans().length,2);assert.equal(f.segments(j.job_id).length,2);
 assert.equal((await f.call('sop_native_start',{...req,estimated_minutes:16})).ok,false);
 await f.finish(j);assert.equal(f.live(s.job_id).length,2);assert.equal(f.loans().filter(b=>b.status==='returned').length,2);
});

test('new confirmed source revision rejects stale scan without ending source or creating destination',async()=>{
 const f=await fixture(),s=await f.source(),req=await f.request();await f.ok('sop_native_people',{job_id:s.job_id,revision:1,workers:f.staff.slice(0,2),lead_id:f.staff[1].id});
 const before=f.segments(s.job_id);assert.equal((await f.call('sop_native_start',req)).ok,false);assert.deepEqual(f.segments(s.job_id),before);assert.equal(f.loans().length,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,1);
});

test('destination start failure rolls back borrowed source, feedback, assignment and idempotency record',async()=>{
 const f=await fixture(),s=await f.source(),before=f.segments(s.job_id),req=await f.request();
 f.DB.raw.exec("CREATE TRIGGER fail_borrow BEFORE INSERT ON v2_ops_job_workers WHEN NEW.job_id LIKE 'JOB-CB-%' BEGIN SELECT RAISE(ABORT,'fixture_start_failure'); END");
 assert.equal((await f.call('sop_native_start',req)).ok,false);assert.deepEqual(f.segments(s.job_id),before);assert.equal(f.loans().length,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_field_feedbacks').get().n,0);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_crew_requests').get().n,0);f.DB.raw.exec('DROP TRIGGER fail_borrow');await f.ok('sop_native_start',req);assert.equal(f.loans().length,1);
});

test('two destination starts racing for one person commit only one borrow and preserve the other worker',async()=>{
 const f=await fixture(),s=await f.source(),b=f.segments(s.job_id)[1],a=await f.request(),c=await f.request();
 const results=await Promise.all([f.call('sop_native_start',a),f.call('sop_native_start',c)]);assert.equal(results.filter(r=>r.ok).length,1);assert.equal(f.loans().length,1);assert.deepEqual(f.segments(s.job_id)[1],b);assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''").get(f.staff[0].id).n,1);
});

test('add borrowed person to existing destination and remove early returns them once; source crew adjustment keeps away person assigned',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.start('v2_unplanned_unload_start',{biz_class:'bulk'},[f.staff[2]]),c=await f.confirm(f.staff[0],null,j.job_id);
 const add={job_id:j.job_id,revision:1,workers:[f.staff[2],f.staff[0]],lead_id:f.staff[2].id,borrow_confirmations:[c],client_req_id:crypto.randomUUID()};await f.ok('sop_native_people',add);await f.ok('sop_native_people',add);assert.equal(f.loans().length,1);assert.equal(f.live(j.job_id).length,2);
 await f.ok('sop_native_people',{job_id:s.job_id,revision:f.record(s.job_id).revision,workers:f.staff.slice(0,2),lead_id:f.staff[1].id});assert.equal(f.live(s.job_id).length,1);
 const result=await f.ok('sop_native_people',{job_id:j.job_id,revision:f.record(j.job_id).revision,workers:[f.staff[2]],lead_id:f.staff[2].id});assert.equal(result.crew_returns[0].status,'returned');assert.equal(f.live(s.job_id).length,2);assert.equal(f.live(j.job_id).length,1);
});

test('nested borrowing is rejected until original loan is returned',async()=>{
 const f=await fixture();await f.source();const j=await f.ok('sop_native_start',await f.request());
 const a=await f.ok('sop_crew_availability',{worker_id:f.staff[0].id,payload:{action:'v2_unplanned_unload_start'}});assert.equal(a.can_borrow,false);assert.match(a.reason,/借调/);await f.finish(j);
 assert.equal((await f.ok('sop_crew_availability',{worker_id:f.staff[0].id,payload:{action:'v2_unplanned_unload_start'}})).can_borrow,true);
});

test('planned multi-plan unloading borrows once, invalid receipt leaves both clocks intact, valid finish returns once',async()=>{
 const f=await fixture(),s=await f.source(),plans=[];for(let i=0;i<2;i++)plans.push(await f.ok('v2_inbound_plan_create',{customer:'合成借调'+i,biz_class:'bulk',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:10+i}]}));
 const req=await f.request([f.staff[0]],{action:'v2_unload_job_start',plan_ids:plans.map(p=>p.id),client_req_id:crypto.randomUUID()}),j=await f.ok('sop_native_start',req),detail=await f.ok('v2_ops_job_detail',{job_id:j.job_id});
 assert.equal(detail.unload_plans.length,2);const good=detail.unload_plans.map(p=>({plan_id:p.plan.id,lines:p.lines.filter(l=>l.unit_type!=='courier').map(l=>({line_id:l.id,actual_qty:l.planned_qty}))}));
 assert.equal((await f.call('v2_unload_job_finish',{job_id:j.job_id,worker_id:f.staff[0].id,complete_job:true,plan_results:[good[0]]})).ok,false);assert.equal(f.loans()[0].status,'borrowed');assert.equal(f.live(j.job_id).length,1);
 await f.ok('v2_unload_job_finish',{job_id:j.job_id,worker_id:f.staff[0].id,complete_job:true,plan_results:good});assert.equal(f.loans()[0].status,'returned');assert.equal(f.live(s.job_id).length,2);
});

test('true no-document loading remains usable and returns borrowed staff on completion',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request([f.staff[0]],{action:'v2_ops_job_start',job_type:'load_outbound',flow_stage:'outbound',client_req_id:crypto.randomUUID()}));
 await f.ok('v2_outbound_load_finish',{job_id:j.job_id,worker_id:f.staff[0].id,complete_job:true,box_count:5});assert.equal(f.loans()[0].status,'returned');assert.equal(f.live(s.job_id).length,2);
});

test('one truck multiple approved orders uses one loan and still rejects every partial loading result atomically',async()=>{
 const f=await loadFixture(),s=await f.ok('sop_native_start',{payload:{action:'v2_ops_job_start',job_type:'load_import',flow_stage:'import',client_req_id:crypto.randomUUID()},workers:f.staff,lead_id:f.staff[0].id,estimated_minutes:10,labor_department:'import'}),orders=[await f.order('合成客户甲',12),await f.order('合成客户乙',2,'托')],req=f.payload(orders);
 const a=await f.ok('sop_crew_availability',{worker_id:f.staff[0].id,payload:req.payload}),b=await f.ok('sop_crew_availability',{worker_id:f.staff[1].id,payload:req.payload});req.borrow_confirmations=[a,b].map((x,i)=>({worker_id:f.staff[i].id,source_job_id:x.source.job_id,source_segment_id:x.source.segment_id,source_revision:x.source.revision}));
 const j=await f.ok('sop_native_start',req),body=f.finishBody(j,orders);body.order_results[0].box_count=11;assert.equal((await f.call('v2_outbound_load_finish',body)).ok,false);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_crew_borrows WHERE status='borrowed'").get().n,2);assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_outbound_orders WHERE status='shipped'").get().n,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,0);
 await f.ok('v2_outbound_load_finish',f.finishBody(j,orders));assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM ck_crew_borrows WHERE status='returned'").get().n,2);assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(s.job_id).n,2);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,1);
});

test('work-plan source keeps assignment and working state while borrowed, then returns',async()=>{
 const f=await fixture(),s=await f.task([f.staff[0]]),j=await f.ok('sop_native_start',await f.request());assert.equal(JSON.parse(f.record(s.job_id).state).status,'working');assert.equal(f.live(s.job_id).length,0);await f.finish(j);assert.equal(f.live(s.job_id).length,1);
});

test('paused source keeps pending return; source resumption cannot double insert away worker',async()=>{
 const f=await fixture(),s=await f.task(),j=await f.ok('sop_native_start',await f.request());await f.change('sop_task_pause',s.job_id,{reason:'合成暂停'});await f.finish(j);assert.equal(f.loans()[0].status,'return_pending');assert.equal(f.live(s.job_id).length,0);
 await f.change('sop_task_start',s.job_id);assert.equal(f.live(s.job_id).length,1);await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.live(s.job_id).length,2);assert.equal(f.loans()[0].status,'returned');
});

test('rest during borrowed work leaves return pending; rest end returns to original source without reopening destination',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request());await f.ok('sop_attendance_break_start',{id:f.days[0].id});assert.equal(f.loans()[0].status,'return_pending');assert.equal(f.live(s.job_id).length,1);assert.equal(f.live(j.job_id).length,0);
 const end=await f.ok('sop_attendance_break_end',{id:f.days[0].id});assert.equal(end.crew_returns[0].status,'returned');assert.equal(f.live(s.job_id).length,2);assert.equal(f.live(j.job_id).length,0);
});

for(const scenario of ['completed','cancelled','removed','owner_changed','day_changed','signed_out','disabled','elsewhere'])test('automatic return never revives source/person after '+scenario,async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request());
 if(['completed','cancelled'].includes(scenario))f.DB.raw.prepare('UPDATE v2_ops_jobs SET status=? WHERE id=?').run(scenario,s.job_id);
 if(scenario==='removed')await f.ok('sop_native_people',{job_id:s.job_id,revision:f.record(s.job_id).revision,workers:[f.staff[1]],lead_id:f.staff[1].id});
 if(scenario==='owner_changed'){const d=JSON.parse(f.record(s.job_id).state);d.owner_id='OTHER';f.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(d),s.job_id);}
 if(scenario==='day_changed')f.DB.raw.prepare("UPDATE ck_crew_borrows SET source_day='2000-01-01'").run();
 if(scenario==='signed_out')await f.ok('sop_attendance_checkout',{id:f.days[0].id});
 if(scenario==='disabled')f.DB.raw.prepare('UPDATE ck_attendance_people SET enabled=0 WHERE id=?').run(f.days[0].personId);
 if(scenario==='elsewhere'){f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=? WHERE job_id=?").run(new Date().toISOString(),j.job_id);await f.start('v2_ops_job_start',{job_type:'qc',flow_stage:'internal'},[f.staff[0]]);}
 await f.finish(j);assert.equal(f.loans()[0].status,'closed');assert.equal(f.live(s.job_id).filter(w=>w.worker_id===f.staff[0].id).length,0);await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.loans()[0].status,'closed');
});

test('return batch failure preserves completed destination and pending loan, then retry returns once',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request());f.DB.raw.exec("CREATE TRIGGER fail_return BEFORE INSERT ON v2_ops_job_workers WHEN NEW.id LIKE 'WS-RETURN-%' BEGIN SELECT RAISE(ABORT,'fixture_return_failure'); END");
 const end=await f.finish(j);assert.equal(end.crew_returns[0].status,'return_pending');assert.equal(f.live(s.job_id).length,1);assert.equal(f.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'completed');f.DB.raw.exec('DROP TRIGGER fail_return');await f.ok('sop_crew_return',{job_id:j.job_id});await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.live(s.job_id).length,2);assert.equal(f.segments(s.job_id).length,3);
});

test('source completion racing with return batch is guarded atomically and cannot create a new source segment',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request()),batch=f.DB.batch;let armed=true;
 f.DB.batch=async statements=>{if(armed&&statements.some(x=>x.sql.startsWith('INSERT INTO v2_ops_job_workers')&&x.args[0]?.startsWith('WS-RETURN-'))){armed=false;f.DB.raw.prepare("UPDATE v2_ops_jobs SET status='completed' WHERE id=?").run(s.job_id);}return batch(statements);};
 await f.finish(j);assert.equal(f.live(s.job_id).filter(w=>w.worker_id===f.staff[0].id).length,0);assert.equal(f.loans()[0].status,'return_pending');await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.loans()[0].status,'closed');
});

test('signout and disable racing with return batch cannot open a source segment',async()=>{
 for(const mode of ['signed_out','disabled']){const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request()),batch=f.DB.batch;let armed=true;
 f.DB.batch=async statements=>{if(armed&&statements.some(x=>x.sql.startsWith('INSERT INTO v2_ops_job_workers')&&x.args[0]?.startsWith('WS-RETURN-'))){armed=false;if(mode==='disabled')f.DB.raw.prepare('UPDATE ck_attendance_people SET enabled=0 WHERE id=?').run(f.days[0].personId);else f.DB.raw.prepare('UPDATE ck_attendance_days SET signed_out=? WHERE id=?').run(new Date().toISOString(),f.days[0].id);}return batch(statements);};
 await f.finish(j);assert.equal(f.live(s.job_id).filter(w=>w.worker_id===f.staff[0].id).length,0,mode);}
});

test('old resume endpoint cannot rejoin completed task',async()=>{
 const f=await fixture(),s=await f.source();await f.ok('v2_ops_job_finish',{job_id:s.job_id,worker_id:f.staff[0].id});assert.equal((await f.call('v2_ops_job_resume',{parent_job_id:s.job_id,worker_id:f.staff[0].id,worker_name:f.staff[0].name})).ok,false);assert.equal(f.live(s.job_id).length,0);
});

test('old-shift open source segment is visible as blocked and cannot be borrowed into today',async()=>{
 const f=await fixture(),s=await f.source();f.DB.raw.prepare("UPDATE v2_ops_job_workers SET joined_at='2000-01-01T00:00:00Z' WHERE job_id=? AND worker_id=?").run(s.job_id,f.staff[0].id);
 const a=await f.ok('sop_crew_availability',{worker_id:f.staff[0].id,payload:{action:'v2_unplanned_unload_start'}});assert.equal(a.can_borrow,false);assert.match(a.reason,/旧班次/);assert.equal(f.loans().length,0);
});

test('source submitted for review is a completed work segment and cannot be automatically resumed',async()=>{
 const f=await fixture(),s=await f.task(),j=await f.ok('sop_native_start',await f.request());await f.change('sop_task_finish',s.job_id,{result:{quantity:2,unit:'托'}});await f.finish(j);assert.equal(f.loans()[0].status,'closed');assert.equal(f.live(s.job_id).length,0);assert.equal(JSON.parse(f.record(s.job_id).state).status,'awaiting_review');
});

test('destination owner may borrow across source owners; unrelated employee cannot manage destination or query borrowing',async()=>{
 const DB=database(),origin='https://isolated-auth.test',env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-only-admin')};
 let office='',field='';const call=async(scope,action,data={})=>{const r=await worker.fetch(new Request(origin+(scope==='office'?'/api':'/001/api'),{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:scope==='office'?office:field},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(action==='sop_login'&&r.ok){const c=r.headers.get('set-cookie').split(';')[0];if(scope==='office')office=c;else field=c;}return r.json();};
 const ok=async(s,a,d)=>{const r=await call(s,a,d);assert.equal(r.ok,true,a+': '+r.error);return r;};await ok('office','sop_login',{sop_key:'synthetic-only-admin'});const people=[];
 for(const employeeNo of ['SOURCE','DEST','UNRELATED']){const p=(await ok('office','sop_attendance_employee_register',{employeeNo,name:'合成员工'+employeeNo,department:'bulk'})).person;await ok('office','sop_access_update',{person_id:p.id,enabled:true,version:0});await ok('office','sop_attendance_checkin',{badge:p.badgeId});people.push(p);}
 const daily=(await ok('office','sop_attendance_checkin',{name:'合成日当工',agency:'가온'})).record,w={id:daily.badgeId,name:daily.name};
 await ok('field','sop_login',{badge:people[0].badgeId});const s=await ok('field','sop_native_start',{payload:{action:'v2_ops_job_start',job_type:'qc',flow_stage:'internal',client_req_id:crypto.randomUUID()},workers:[w],lead_id:w.id,estimated_minutes:10,labor_department:'bulk'});
 await ok('field','sop_login',{badge:people[1].badgeId});const payload={action:'v2_unplanned_unload_start',client_req_id:crypto.randomUUID()},a=await ok('field','sop_crew_availability',{payload,worker_id:w.id}),j=await ok('field','sop_native_start',{payload,workers:[w],lead_id:w.id,estimated_minutes:10,labor_department:'bulk',borrow_confirmations:[{worker_id:w.id,source_job_id:a.source.job_id,source_segment_id:a.source.segment_id,source_revision:a.source.revision}]});
 await ok('field','sop_login',{badge:people[2].badgeId});assert.equal((await call('field','sop_crew_status',{job_id:j.job_id})).ok,false);assert.equal((await call('field','sop_crew_availability',{job_id:j.job_id,worker_id:w.id})).ok,false);assert.equal((await call('field','sop_native_people',{job_id:j.job_id,revision:1,workers:[],lead_id:''})).ok,false);
 await ok('field','sop_login',{badge:people[1].badgeId});await ok('field','v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:w.id,result_lines:[{unit_type:'carton',actual_qty:1}]});assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(s.job_id).n,1);
});

test('production flag cannot install loan tables or enable borrowing',async()=>{
 const DB=database(),env={DB,SOP_ENVIRONMENT:'production',SOP_UPGRADE_ENABLED:'true'};assert.equal(crewBorrowEnabled(env),false);await ensureCrewBorrow(env);assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name LIKE 'ck_crew_%'").get().n,0);
});

test('historical unmanaged job can lend and return a person without converting its crew or task',async()=>{
 const f=await fixture(),w={id:'LEGACY-BORROW-A',name:'合成旧工单人员'},t=new Date().toISOString();f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,active_worker_count) VALUES('LEGACY-SOURCE','qc','working',1)").run();f.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)').run('LEGACY-SEGMENT','LEGACY-SOURCE',w.id,w.name,t);
 const j=await f.ok('sop_native_start',await f.request([w]));assert.equal(f.loans()[0].source_kind,'legacy');assert.equal(f.live('LEGACY-SOURCE').length,0);await f.finish(j);assert.equal(f.live('LEGACY-SOURCE').length,1);assert.equal(f.record('LEGACY-SOURCE'),undefined);assert.equal(f.loans()[0].status,'returned');
});

test('picking source closes only borrowed person documents and returns a fresh segment for each unfinished document without duplicating labor',async()=>{
 const f=await fixture(),s=await f.start('v2_pick_job_start',{pick_doc_nos:['BORROW-PICK-1','BORROW-PICK-2']}),b=f.segments(s.job_id)[1],j=await f.ok('sop_native_start',await f.request());
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_pick_worker_docs WHERE worker_id=? AND status='completed'").get(f.staff[0].id).n,2);assert.deepEqual(f.segments(s.job_id)[1],b);await f.finish(j);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_pick_worker_docs WHERE worker_id=? AND status='working'").get(f.staff[0].id).n,2);assert.equal(f.segments(s.job_id).length,3);const r=await f.ok('v2_pick_job_finalize',{job_id:s.job_id,worker_id:f.staff[0].id});
 assert.equal(r.worker_count,2);assert.equal(r.total_minutes,f.DB.raw.prepare('SELECT ROUND(SUM(minutes_worked),1) n FROM v2_ops_job_workers WHERE job_id=?').get(s.job_id).n);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(s.job_id).n,1);
});

test('existing staging maintenance archives synthetic loan tables with task tables; no orphaned active loan remains',async()=>{
 const f=await fixture();await f.source();await f.ok('sop_native_start',await f.request());assert.ok(TABLES.includes('ck_crew_borrows'));assert.ok(TABLES.includes('ck_crew_requests'));
 const user={id:'SYNTHETIC-MAINTENANCE',role:'manager'},start=await resetAction('start',{confirmation:'CLEAR_TEST_DATA',requestId:crypto.randomUUID()},f.env,user);let run=start.run;for(let i=0;i<30&&!run.completedAt;i++)run=(await resetAction('step',{runId:run.id},f.env,user)).run;assert.ok(run.completedAt);assert.equal(f.loans().length,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_crew_requests').get().n,0);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_backup_'+run.id+'_ck_crew_borrows').get().n,1);
});

test('native paused source retains borrowed assignment; destination completion waits, resume and return count once',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request());
 await f.ok('sop_native_pause',{job_id:s.job_id,revision:f.record(s.job_id).revision,reason:'原任务暂停'});assert.equal(f.live(s.job_id).length,0);
 await f.finish(j);assert.equal(f.loans()[0].status,'return_pending');assert.equal(f.live(s.job_id).length,0);
 assert.equal((await f.call('sop_native_resume',{job_id:s.job_id,revision:f.record(s.job_id).revision,workers:[f.staff[0]],lead_id:f.staff[0].id})).ok,false);
 await f.ok('sop_native_resume',{job_id:s.job_id,revision:f.record(s.job_id).revision,workers:[f.staff[1]],lead_id:f.staff[1].id});
 await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.live(s.job_id).length,2);assert.equal(f.loans()[0].status,'returned');
 await f.ok('sop_crew_return',{job_id:j.job_id});assert.equal(f.live(s.job_id).length,2);assert.equal(f.segments(s.job_id).filter(w=>w.worker_id===f.staff[0].id).length,2);
});
test('rest on a paused borrowed destination does not revive it, and signed-out borrower never returns',async()=>{
 const f=await fixture(),s=await f.source(),j=await f.ok('sop_native_start',await f.request());
 await f.ok('sop_native_pause',{job_id:j.job_id,revision:f.record(j.job_id).revision,reason:'借调任务暂停'});
 await f.ok('sop_attendance_break_start',{id:f.days[0].id});assert.equal(f.loans()[0].status,'return_pending');
 await f.ok('sop_attendance_checkout',{id:f.days[0].id});assert.equal((await f.call('sop_attendance_break_end',{id:f.days[0].id})).ok,false);
 assert.equal(f.live(j.job_id).length,0);assert.equal(f.live(s.job_id).length,1);
 assert.equal((await f.call('sop_native_resume',{job_id:j.job_id,revision:f.record(j.job_id).revision,workers:[f.staff[0]],lead_id:f.staff[0].id})).ok,false);
});
