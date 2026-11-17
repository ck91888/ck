import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {attendanceVoidBlockers} from '../worker-v2/attendance-management.js';
import {workerBadgeMatch} from '../worker-v2/daily-badge-identity.js';

const date='2026-11-16';
const old=`SELECT DISTINCT d.id FROM ck_attendance_days d JOIN v2_ops_job_workers w
 ON ${workerBadgeMatch('w.worker_id','d.worker_id')} AND julianday(w.joined_at)<julianday(d.day,'+1 day','-9 hours')
 AND (w.left_at='' OR julianday(w.left_at)>=julianday(d.day,'-9 hours')) WHERE d.day=?`;

test('attendance blocker scans worker history once and preserves exact badges and KST boundaries',async t=>{
 const db=new DatabaseSync(':memory:');t.after(()=>db.close());
 db.exec(`CREATE TABLE ck_attendance_days(id TEXT PRIMARY KEY,day TEXT NOT NULL,worker_id TEXT NOT NULL);
 CREATE INDEX ck_attendance_day ON ck_attendance_days(day);
 CREATE TABLE v2_ops_job_workers(worker_id TEXT NOT NULL,joined_at TEXT,left_at TEXT);
 CREATE INDEX idx_v2_ops_workers_worker ON v2_ops_job_workers(worker_id);
 CREATE TABLE ck_attendance_breaks(attendance_id TEXT);`);
 const mark=db.prepare('INSERT INTO ck_attendance_days VALUES (?,?,?)');
 const work=db.prepare('INSERT INTO v2_ops_job_workers VALUES (?,?,?)');
 const dbStart='2026-11-15T15:00:00.000Z',dbEnd='2026-11-16T15:00:00.000Z';
 const fixtures=[
  ['canonical','DA-20261116-ABCDEF01','dA-20261116-aBcDEf01','2026-11-15T20:00:00+09:00',dbStart],
  ['before-start','EMP-before','EMP-before','2026-11-15T20:00:00+09:00','2026-11-15T14:59:59Z'],
  ['at-end','EMP-end','EMP-end',dbEnd,''],
  ['invalid-time','EMP-time','EMP-time','not a time',''],
  ['case-sensitive','EMP-custom','emp-custom',dbStart,''],
  ['bad-daily-date','DA-20260230-abcdef01','da-20260230-abcdef01',dbStart,''],
  ['custom-da','DA-ManuallyIssued','da-ManuallyIssued',dbStart,''],
  ['active','EMP-still','EMP-still','2026-11-16T23:59:59+09:00',''],
  ['no-left','EMP-null','EMP-null',dbStart,null],
 ];
 for(const [id,dayBadge,workerBadge,joined,left] of fixtures){mark.run(id,date,dayBadge);work.run(workerBadge,joined,left);}
 mark.run('other-day','2026-11-15','EMP-still');
 work.run('EMP-still',dbStart,'');
 for(let i=0;i<32;i++)mark.run('x'+i,date,'EMP-UNUSED-'+i);
 for(let i=0;i<2500;i++)work.run('EMP-HISTORY-'+i,'2026-10-01T00:00:00Z','2026-10-01T01:00:00Z');
 const queries=[];
 const DB={prepare(sql){return {bind(...args){queries.push({sql,args});return {all:async()=>({results:db.prepare(sql).all(...args)}),first:async()=>db.prepare(sql).get(...args)};}};}};
 const blockers=await attendanceVoidBlockers({DB},date);
 const expected=db.prepare(old).all(date).map(r=>r.id).sort();
 assert.deepEqual(expected,['active','canonical']);
 assert.deepEqual([...blockers.keys()].sort(),expected);
 // Count fetched worker rows directly with identical table shapes, query
 // plans and seed data; probe first in WHERE/ON to see every worker visit.
 let reads=0;db.function('probe_worker',(_badge)=>{reads++;return 1;});
 const {sql,args}=queries[0];
 db.prepare(sql.replace('WHERE julianday(w.joined_at)','WHERE probe_worker(w.worker_id) AND julianday(w.joined_at)')).all(...args);
 const newReads=reads;reads=0;
 db.prepare(old.replace(' ON ',' ON probe_worker(w.worker_id) AND ')).all(date);
 const oldReads=reads;
 assert.equal(newReads,2510);
 assert.ok(oldReads>=newReads*30,);
 t.diagnostic(`worker table row visits: ${oldReads} → ${newReads}`);
});
