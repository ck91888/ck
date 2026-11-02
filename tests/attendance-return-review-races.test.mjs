// Independent review blockers, real authenticated API on isolated Node SQLite or native local D1.
import test from 'node:test';import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';import {localD1} from './d1-local-runtime.mjs';
import {confirmOperation} from './office-operator-fixture.mjs';
async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-02T00:00:00Z')});const f=await ownedHomeFixture();let native;
 if(process.env.CK_RETURN_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}t.after(async()=>{await native?.close();f.DB.raw.close();});f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';
 const first=(sql,...args)=>f.env.DB.prepare(sql).bind(...args).first(),preview=badge=>f.ok('office','sop_attendance_return_preview',{badge}),cursor=p=>({badge:p.record.badgeId,id:p.record.id,version:p.version,span_id:p.span_id,span_version:p.span_version});
 return {...f,first,preview,cursor,advance:m=>t.mock.timers.tick(m*60000)};
}
async function holdBatch(f,action){
 const db=f.env.DB;let reached,release,held=false;const ready=new Promise(r=>reached=r),gate=new Promise(r=>release=r);
 f.env.DB={prepare:db.prepare.bind(db),async batch(ss){if(!held&&ss.some(s=>/INSERT INTO sop_events VALUES/.test(s.sql||'')&&s.args?.[3]===action)){held=true;reached();await gate;}return db.batch(ss);}};
 return {ready,release,restore(){f.env.DB=db;}};
}
test('linked badges use the active badge identity name for courier admission while preserving the original punch name',async t=>{
 const f=await fixture(t),p=(await f.ok('office','sop_attendance_register',{name:'合成稳定工牌姓名',agency:'가온'})).person;
 await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk','direct_ship'],version:0,reason:'合成明确身份授权'});
 const day=(await f.ok('office','sop_attendance_checkin',{name:'合成首段临时姓名',agency:'가온'})).record;
 await f.ok('office','sop_daily_dispatch_link',{person_id:p.id,badge:p.badgeId,daily_badge:day.badgeId,daily_person_id:day.personId,attendance_id:day.id,reason:'合成核对临时牌与长期牌是同一人'});
 f.advance(5);await f.ok('office','sop_attendance_checkout',f.cursor(await f.preview(day.badgeId)));f.advance(5);const v=await f.preview(p.badgeId);await f.ok('office','sop_attendance_return_confirm',{...f.cursor(v),badge:p.badgeId,confirmed:true});
 const scan=await f.ok('office','sop_attendance_lookup',{badge:p.badgeId});assert.equal(scan.person.name,p.name);
 const started=await f.ok('office','sop_courier_receive',{operation:'start',workers:[{id:p.badgeId,name:scan.person.name}],lead_id:p.badgeId});assert.equal(started.batch.workers[0].name,p.name);
 assert.equal((await f.call('office','sop_courier_receive',{operation:'people',batch_id:started.batch.id,revision:started.batch.revision,workers:[{id:p.badgeId,name:day.name}],lead_id:p.badgeId})).ok,false);
 assert.equal((await f.first('SELECT name FROM ck_attendance_days WHERE id=?',day.id)).name,day.name);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_attendance_spans WHERE attendance_id=?',day.id)).n,2);
});
test('an in-flight claim cannot take ownership after the original owner returns later than the request timestamp',async t=>{
 const f=await fixture(t),job=await f.task('A','合成在途接班竞态');f.advance(5);await f.ok('office','sop_attendance_checkout',f.cursor(await f.preview(f.people.A.badgeId)));assert.equal((await f.ok('B','sop_owner_transfer_options',{id:job.id})).can_claim,true);
 const row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),hold=await holdBatch(f,'sop_owner_transfer');
 const pending=f.call('B','sop_owner_transfer',{id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成请求发出时原负责人已离岗'});await hold.ready;
 try{f.advance(5);await f.ok('office','sop_attendance_return_confirm',{...f.cursor(await f.preview(f.people.A.badgeId)),confirmed:true});assert.equal((await f.ok('B','sop_owner_transfer_options',{id:job.id})).can_claim,false);}finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,false,JSON.stringify(result));assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.A.id);assert.equal((await f.first("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_owner_transfer'",job.id)).n,0);
});
async function officeAgain(f){await f.ok('office','sop_login',{sop_key:'synthetic-owned-home-fixture'});await confirmOperation((a,b)=>f.ok('office',a,b),'合成竞态核验员');}
for(const mode of ['cross-midnight','return-then-checkout'])test('claim evidence survives '+mode+' and a stale request cannot reuse absence from preparation',async t=>{
 const f=await fixture(t),job=await f.task('A','合成接班变体'+mode);if(mode==='cross-midnight'){f.advance(14*60+55);await officeAgain(f);await f.ok('B','sop_login',{badge:f.people.B.badgeId});}else f.advance(5);
 await f.ok('office','sop_attendance_checkout',f.cursor(await f.preview(f.people.A.badgeId)));const row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),body={id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成准备后重新核对出勤',client_req_id:crypto.randomUUID()},hold=await holdBatch(f,'sop_owner_transfer');const pending=f.call('B','sop_owner_transfer',body);await hold.ready;
 try{f.advance(mode==='cross-midnight'?10:5);await f.ok('office','sop_attendance_return_confirm',{...f.cursor(await f.preview(f.people.A.badgeId)),confirmed:true});if(mode==='return-then-checkout'){f.advance(1);await f.ok('office','sop_attendance_checkout',f.cursor(await f.preview(f.people.A.badgeId)));}}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.A.id);
 if(mode==='return-then-checkout'){await f.ok('B','sop_owner_transfer',body);const latest=JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state);assert.equal(latest.owner_id,f.people.B.id);assert.equal(latest.last_owner_transfer.at,new Date().toISOString());}
});
test('a transfer target entering and ending rest while the write waits requires a fresh admission',async t=>{
 const f=await fixture(t),job=await f.task('A','合成转单目标休息往返'),row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),hold=await holdBatch(f,'sop_owner_transfer');const pending=f.call('A','sop_owner_transfer',{id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成目标资格已变化'});await hold.ready;
 try{f.advance(2);await f.ok('office','sop_attendance_break_start',{id:f.people.B.record.id});f.advance(2);await f.ok('office','sop_attendance_break_end',{id:f.people.B.record.id});}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.A.id);
});
for(const mode of ['ordinary','multi-order','courier'])test(mode+' worker admission cannot backdate a new work clock over a rest round trip while its transaction waits',async t=>{
 const f=await fixture(t),w=await f.person(),workers=[{id:w.badgeId,name:w.name}];let action,body,event;
 if(mode==='ordinary'){const n=await f.need('合成普通单入场竞态');action='sop_task_dispatch';event=action;body={need_id:n.id,department:'bulk',title:'合成普通单入场竞态',job_type:'bulk_op',workers,lead_id:w.badgeId,estimated_minutes:10};}
 else if(mode==='multi-order'){const needs=await Promise.all(['甲','乙'].map(x=>f.need('合成多单入场竞态'+x)));action='sop_bulk_batch_create';event=action;body={items:needs.map(n=>({source_type:'need',id:n.id,revision:1})),workers,lead_id:w.badgeId};}
 else{action='sop_courier_receive';event='sop_native_start';body={operation:'start',workers,lead_id:w.badgeId};}
 const hold=await holdBatch(f,event),pending=f.call('office',action,body);await hold.ready;
 try{f.advance(2);await f.ok('office','sop_attendance_break_start',{id:w.id});f.advance(2);await f.ok('office','sop_attendance_break_end',{id:w.id});}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal((await f.first('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=?',w.badgeId)).n,0);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_attendance_span_checks')).n,0);
 const retry=await f.ok('office',action,body);assert.ok(retry.id||retry.batch_id||retry.batch);const clock=await f.first("SELECT joined_at FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''",w.badgeId);assert.equal(clock.joined_at,new Date().toISOString());
});
test('a prepared verification takeover is rejected when its original owner returns before the transaction starts',async t=>{
 const f=await fixture(t),check=await f.ok('office','sop_check_create',{ship_date:'2026-11-02'});await f.ok('office','sop_check_items',{id:check.id,revision:1,items:[{barcode:'SYNTH-TAKEOVER',customer:'合成核对竞态',qty:1}]});const a=f.people.A;
 const job=await f.ok('A','sop_native_start',{payload:{action:'v2_verify_job_start',batch_id:check.id,client_req_id:crypto.randomUUID()},workers:[{id:a.badgeId,name:a.name}],lead_id:a.badgeId,estimated_minutes:10,labor_department:'bulk'});f.advance(5);await f.ok('office','sop_attendance_checkout',f.cursor(await f.preview(a.badgeId)));const active=(await f.ok('B','sop_verify_active',{batch_id:check.id})).active;assert.equal(active.can_takeover,true);
 const hold=await holdBatch(f,'sop_verify_takeover'),pending=f.call('B','sop_verify_takeover',{job_id:job.job_id,revision:active.revision});await hold.ready;
 try{f.advance(5);await f.ok('office','sop_attendance_return_confirm',{...f.cursor(await f.preview(a.badgeId)),confirmed:true});}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.job_id)).state).owner_id,a.id);assert.equal((await f.first("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_verify_takeover'",job.job_id)).n,0);
});

test('revoking and restoring a transfer target grant while the transaction waits still invalidates the prepared admission',async t=>{
 const f=await fixture(t),p=(await f.ok('office','sop_attendance_register',{name:'合成目标派审资格',agency:'가온'})).person;
 await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成初次资格'});await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:'가온'});const job=await f.task('A','合成目标资格往返'),row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),hold=await holdBatch(f,'sop_owner_transfer');const pending=f.call('A','sop_owner_transfer',{id:job.id,revision:row.revision,target_badge:p.badgeId,reason:'合成目标授权版本已变化'});await hold.ready;
 try{f.advance(1);await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:false,departments:['bulk'],version:1,reason:'合成临时撤销'});f.advance(1);await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk'],version:2,reason:'合成核实恢复'});}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.A.id);
});

for(const initial of ['implicit','explicit'])test('field access revoke and restore invalidates a prepared employee transfer from '+initial+' permission state',async t=>{
 const f=await fixture(t);let version=0;if(initial==='explicit'){const grant=await f.ok('office','sop_access_update',{person_id:f.people.B.id,enabled:true,version:0});version=grant.version;}
 const job=await f.task('A','合成职员现场权限往返'+initial),row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),body={id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成现场权限版本已变化',client_req_id:crypto.randomUUID()},hold=await holdBatch(f,'sop_owner_transfer'),pending=f.call('A','sop_owner_transfer',body);await hold.ready;
 try{f.advance(1);const revoked=await f.ok('office','sop_access_update',{person_id:f.people.B.id,enabled:false,version});f.advance(1);await f.ok('office','sop_access_update',{person_id:f.people.B.id,enabled:true,version:revoked.version});}finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,false,JSON.stringify(result));assert.match(result.error,/出勤状态已变化/);assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.A.id);assert.equal((await f.first("SELECT COUNT(*) n FROM sop_events WHERE record_id=? AND action='sop_owner_transfer'",job.id)).n,0);
 await f.ok('A','sop_owner_transfer',body);const saved=JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state);assert.equal(saved.owner_id,f.people.B.id);assert.equal(saved.last_owner_transfer.at,new Date().toISOString());
});
test('field access kept revoked rejects the waiting transfer and a fresh transfer without changing the original owner',async t=>{
 const f=await fixture(t),job=await f.task('A','合成现场权限持续撤销'),row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),body={id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成拒绝失去现场权限的接收人'},hold=await holdBatch(f,'sop_owner_transfer'),pending=f.call('A','sop_owner_transfer',body);await hold.ready;
 try{await f.ok('office','sop_access_update',{person_id:f.people.B.id,enabled:false,version:0});}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal((await f.call('A','sop_owner_transfer',body)).ok,false);assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.A.id);
});
test('an unrelated employee field permission round trip does not invalidate a scoped transfer',async t=>{
 const f=await fixture(t),c=(await f.ok('office','sop_attendance_employee_register',{employeeNo:'FIELD-UNRELATED',name:'合成无关职员',department:'bulk'})).person,job=await f.task('A','合成无关人员授权变化'),row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),hold=await holdBatch(f,'sop_owner_transfer'),pending=f.call('A','sop_owner_transfer',{id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成只校验相关人员'});await hold.ready;
 try{await f.ok('office','sop_access_update',{person_id:c.id,enabled:false,version:0});await f.ok('office','sop_access_update',{person_id:c.id,enabled:true,version:1});}finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,true,JSON.stringify(result));assert.equal(JSON.parse((await f.first('SELECT state FROM sop_records WHERE id=?',job.id)).state).owner_id,f.people.B.id);
});
test('an actor field permission round trip rejects its old session and requires a new login before transfer',async t=>{
 const f=await fixture(t),job=await f.task('A','合成操作者现场权限往返'),row=await f.first('SELECT revision FROM sop_records WHERE id=?',job.id),body={id:job.id,revision:row.revision,target_badge:f.people.B.badgeId,reason:'合成旧会话不得恢复'},hold=await holdBatch(f,'sop_owner_transfer'),pending=f.call('A','sop_owner_transfer',body);await hold.ready;
 try{await f.ok('office','sop_access_update',{person_id:f.people.A.id,enabled:false,version:0});await f.ok('office','sop_access_update',{person_id:f.people.A.id,enabled:true,version:1});}finally{hold.release();}
 assert.equal((await pending).ok,false);hold.restore();assert.equal((await f.call('A','sop_owner_transfer',body)).ok,false);await f.ok('A','sop_login',{badge:f.people.A.badgeId});await f.ok('A','sop_owner_transfer',body);
});
test('static field login denial does not change an enabled employee attendance or physical worker assignment rule',async t=>{
 const f=await fixture(t);await f.ok('office','sop_access_update',{person_id:f.people.B.id,enabled:false,version:0});assert.equal((await f.call('B','sop_login',{badge:f.people.B.badgeId})).ok,false);
 const job=await f.task('A','合成保留实际操作员准入',f.people.B);assert.ok(job.id);assert.equal((await f.first("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at=''",job.id,f.people.B.badgeId)).n,1);
});
