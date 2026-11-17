import test from 'node:test';import assert from 'node:assert/strict';
import {atTime} from './labor-export-fixture.mjs';
import {midnightFixture,seedMidnight,assertMidnightExport} from './labor-export-midnight-fixture.mjs';
import {collectLaborExport} from '../shared/labor-export-ui.js';
for(const returns of [false,true])for(const policy of [false,true])test(`midnight closed shift exports both natural days; returns=${returns}, policy=${policy}`,()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await midnightFixture(returns);f.env.SOP_ATTENDANCE_SHIFT_ACCOUNTING=String(policy);seedMidnight(f,100);const before=f.DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all();await assertMidnightExport(f);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all(),before);f.DB.raw.close();
}));
for(const returns of [false,true])test(`unclosed overnight keeps pending facts, no invented checkout or later-day attendance; returns=${returns}`,()=>atTime('2026-01-03T11:00:00Z',async()=>{
 const f=await midnightFixture(returns),p=seedMidnight(f,101,{open:true}),result=await collectLaborExport(b=>f.ok('office','sop_attendance_daily_export',b),{start:'2026-01-01',end:'2026-01-03'});
 assert.equal(result.rows.length,2);assert.deepEqual(result.rows.map(r=>r.date),['2026-01-01','2026-01-02']);assert.equal(result.rows[1].outAt,'');assert.equal(result.rows[1].checkoutState,'未签退（待核实）');assert.equal(result.rows[1].needsVerification,true);if(!returns){assert.equal(result.rows[1].minutes.presence,0);assert.equal(result.rows[1].minutes.operation,0);assert.equal(result.rows[1].intervals[0].minutes.rawPresence,0);}assert.equal(f.DB.raw.prepare('SELECT signed_out FROM ck_attendance_days WHERE id=?').get(p.dayId).signed_out,'');f.DB.raw.close();
}));
for(const returns of [false,true])test(`overnight and next normal shift are one person-day with two factual pairs; returns=${returns}`,()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await midnightFixture(returns);seedMidnight(f,102,{next:true});const r=await f.ok('office','sop_attendance_daily_export',{start_date:'2026-01-02',end_date:'2026-01-02'});assert.equal(r.total,1);assert.equal(r.items[0].intervals.length,2);assert.equal(r.items[0].minutes.presence,120);assert.equal(r.items[0].minutes.rawPresence,120);assert.equal(r.items[0].minutes.bulk,60);assert.equal(r.items[0].minutes.gap,480);assert.equal(r.items[0].outAt,'2026-01-02T10:00:00+09:00');f.DB.raw.close();
}));
for(const returns of [false,true])test(`departure exactly at midnight does not invent a next-day row; returns=${returns}`,()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await midnightFixture(returns),p=seedMidnight(f,103);f.DB.raw.prepare('UPDATE ck_attendance_days SET signed_out=? WHERE id=?').run('2026-01-02T00:00:00+09:00',p.dayId);const r=await f.ok('office','sop_attendance_daily_export',{start_date:'2026-01-02',end_date:'2026-01-02'});assert.equal(r.total,0);f.DB.raw.close();
}));
test('legacy overlapping source punches reject export instead of duplicating labor or deleting evidence',()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await midnightFixture(false),p=seedMidnight(f,104,{next:true});f.DB.raw.prepare('UPDATE ck_attendance_days SET signed_out=? WHERE id=?').run('2026-01-02T09:30:00+09:00',p.dayId);const before=f.DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all(),r=await f.call('office','sop_attendance_daily_export',{start_date:'2026-01-02',end_date:'2026-01-02'});assert.equal(r.ok,false);assert.match(r.error,/出勤区间重叠/);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all(),before);f.DB.raw.close();
}));
