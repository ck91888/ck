// Real local workerd D1 SQL grammar/transaction smoke. Synthetic memory copy only.
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
const f=await ownedHomeFixture();let d1;
try{
 d1=await localD1(f.DB.raw);f.env.DB=d1;f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';f.env.SOP_DISPATCH_SHARE_ENABLED='true';
 const job=await f.task('A','合成D1派审事务'),states=()=>d1.prepare('SELECT * FROM ck_dispatch_time_days WHERE person_id=?').bind(f.people.A.id).first();assert.equal((await states()).status,'dispatching');
 await f.ok('office','sop_attendance_break_start',{id:f.people.A.record.id});assert.equal((await states()).status,'rest');await f.ok('office','sop_attendance_break_end',{id:f.people.A.record.id});assert.equal((await states()).status,'dispatching');
 const r=await d1.prepare('SELECT revision,state FROM sop_records WHERE id=?').bind(job.id).first();await f.ok('A','sop_task_complete_review',{id:job.id,revision:r.revision,result:{quantity:2,unit:'箱'},decision:'pass',reason:'合成D1核对'});assert.equal((await states()).status,'waiting_dispatch');
 assert.equal((await d1.prepare("SELECT count(*) n FROM ck_dispatch_time_spans WHERE ended_at=''").first()).n,0);assert.equal((await d1.prepare('SELECT count(*) n FROM v2_ops_job_results').first()).n,1);
 const summary=await f.ok('office','sop_attendance_summary',{date:f.people.A.record.day,scope:'employee'}),item=summary.items.find(x=>x.record.id===f.people.A.record.id);assert.ok(item.totals.dispatch>=0);assert.equal(item.workday.dispatched.total,1);assert.equal(item.workday.own.participated,0);
 console.log('PASS local workerd D1: fresh trigger schema, actual dispatch/rest/return/final review transactions and employee aggregation');
}finally{await d1?.close();f.DB.raw.close();}
