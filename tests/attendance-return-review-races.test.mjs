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
