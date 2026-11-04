import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {seedDaily} from './labor-export-fixture.mjs';
import {collectLaborExport} from '../shared/labor-export-ui.js';
export async function midnightFixture(returns){const f=await ownedHomeFixture();f.env.SOP_ATTENDANCE_RETURNS_ENABLED=String(returns);await f.ok('office','sop_attendance_summary',{date:'2026-01-02'});return f;}
export function seedMidnight(f,index,{open=false,next=false}={}){
 const p=seedDaily(f,index,'2026-01-01',{start:'23:00:00',out:''});
 if(!open)f.DB.raw.prepare('UPDATE ck_attendance_days SET signed_out=? WHERE id=?').run('2026-01-02T01:00:00+09:00',p.dayId);
 if(next)seedDaily(f,index,'2026-01-02',{start:'09:00:00',out:'10:00:00'});
 f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,created_at) VALUES(?, 'bulk_op','bulk','completed',?)").run('MID-J-'+index,'2026-01-01T23:00:00+09:00');
 f.DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run('MID-W-'+index,'MID-J-'+index,p.badge,'合成跨夜人员','2026-01-01T23:00:00+09:00','2026-01-02T01:00:00+09:00',120);return p;
}
export async function assertMidnightExport(f){
 const result=await collectLaborExport(b=>f.ok('office','sop_attendance_daily_export',b),{start:'2026-01-01',end:'2026-01-02'});
 assert.equal(result.rows.length,2);assert.deepEqual(result.rows.map(x=>x.date),['2026-01-01','2026-01-02']);assert.deepEqual(result.rows.map(x=>x.minutes.bulk),[60,60]);assert.deepEqual(result.rows.map(x=>x.minutes.presence),[60,60]);assert.equal(result.rows[1].inAt,'2026-01-01T23:00:00+09:00');assert.equal(result.rows[1].outAt,'2026-01-02T01:00:00+09:00');return result;
}
