// Real local workerd D1 SQL grammar/transaction smoke. Synthetic memory copy only.
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {legacyDispatchLedger,dispatchHistory} from './dispatch-time-legacy-fixture.mjs';
const f=await ownedHomeFixture();let d1;
try{
 if(process.env.CK_DISPATCH_D1_HISTORY==='true')dispatchHistory(f.DB.raw);
 if(process.env.CK_DISPATCH_D1_UPGRADE==='true')legacyDispatchLedger(f.DB.raw,f.people.A);
 d1=await localD1(f.DB.raw);f.env.DB=d1;f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';f.env.SOP_DISPATCH_SHARE_ENABLED='true';
 const job=await f.task('A','合成D1派审事务'),states=()=>d1.prepare('SELECT * FROM ck_dispatch_time_days WHERE person_id=?').bind(f.people.A.id).first();assert.equal((await states()).status,'dispatching');
 await f.ok('office','sop_attendance_break_start',{id:f.people.A.record.id});assert.equal((await states()).status,'rest');await f.ok('office','sop_attendance_break_end',{id:f.people.A.record.id});assert.equal((await states()).status,'dispatching');
 const r=await d1.prepare('SELECT revision,state FROM sop_records WHERE id=?').bind(job.id).first();await f.ok('A','sop_task_complete_review',{id:job.id,revision:r.revision,result:{quantity:2,unit:'箱'},decision:'pass',reason:'合成D1核对'});assert.equal((await states()).status,'waiting_dispatch');
 assert.equal((await d1.prepare("SELECT count(*) n FROM ck_dispatch_time_spans WHERE ended_at=''").first()).n,0);assert.equal((await d1.prepare('SELECT count(*) n FROM v2_ops_job_results').first()).n,1);
 const summary=await f.ok('office','sop_attendance_summary',{date:f.people.A.record.day,scope:'employee'}),item=summary.items.find(x=>x.record.id===f.people.A.record.id);assert.ok(item.totals.dispatch>=0);assert.equal(item.workday.dispatched.total,1);assert.equal(item.workday.own.participated,0);
 console.log('PASS local workerd D1: fresh trigger schema, actual dispatch/rest/return/final review transactions and employee aggregation');
 if(process.env.CK_DISPATCH_D1_UPGRADE==='true'){assert.ok((await d1.prepare('PRAGMA table_info(ck_dispatch_time_days)').all()).results.some(c=>c.name==='requires_action'));assert.equal((await d1.prepare("SELECT count(*) n FROM ck_dispatch_time_spans WHERE id='DTS-SYNTHETIC-V1'").first()).n,1);console.log('PASS local workerd D1: v1 labor preserved, requires_action added, old trigger replaced');}
 if(process.env.CK_DISPATCH_D1_HISTORY==='true'){
  const next=await f.task('A','合成D1容量重算');d1.metrics.length=0;const at=new Date().toISOString(),start=performance.now();for(let i=0;i<10;i++)await d1.prepare("INSERT INTO ck_dispatch_time_ticks(person_id,at,reason,event_key) VALUES(?,?,'synthetic_capacity',?)").bind(f.people.A.id,at,'SYNTH-D1-CAPACITY-'+i).run();const measurements=d1.metrics.filter(x=>x.sql.startsWith('INSERT INTO ck_dispatch_time_ticks')),rows=measurements.reduce((n,x)=>n+(x.rows_read||0),0);assert.ok(rows<10000,'Recalculation scans historical tables: '+rows);console.log('PASS local workerd D1 capacity '+JSON.stringify({historicalAttendances:10000,closedOperations:10000,recalculations:10,rowsRead:rows,wallMs:performance.now()-start,serverMs:measurements.reduce((n,x)=>n+(x.duration||0),0)}));
 }
}finally{await d1?.close();f.DB.raw.close();}
