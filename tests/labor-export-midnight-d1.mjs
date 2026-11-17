import assert from 'node:assert/strict';
import {atTime} from './labor-export-fixture.mjs';
import {midnightFixture,seedMidnight,assertMidnightExport} from './labor-export-midnight-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {collectLaborExport} from '../shared/labor-export-ui.js';
for(const returns of [false,true])for(const policy of [false,true])await atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await midnightFixture(returns);f.env.SOP_ATTENDANCE_SHIFT_ACCOUNTING=String(policy);seedMidnight(f,100);const d1=await localD1(f.DB.raw);f.env.DB=d1;try{const before=(await d1.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all()).results;await assertMidnightExport(f);assert.deepEqual((await d1.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all()).results,before);console.log(`PASS D1 closed overnight both dates and unchanged punches; returns=${returns} policy=${policy}`);}finally{await d1.close();f.DB.raw.close();}
});
for(const returns of [false,true])await atTime('2026-01-03T11:00:00Z',async()=>{
 const f=await midnightFixture(returns),p=seedMidnight(f,101,{open:true});seedMidnight(f,102,{next:true});const d1=await localD1(f.DB.raw);f.env.DB=d1;try{const r=await collectLaborExport(b=>f.ok('office','sop_attendance_daily_export',b),{start:'2026-01-01',end:'2026-01-03'}),open=r.rows.find(x=>x.personKey===p.id&&x.date==='2026-01-02'),closed=r.rows.find(x=>x.personKey!==p.id&&x.date==='2026-01-02');assert.equal(r.rows.length,4);assert.equal(open.outAt,'');assert.equal(open.checkoutState,'未签退（待核实）');if(!returns){assert.equal(open.minutes.presence,0);assert.equal(open.minutes.operation,0);}assert.equal(closed.intervals.length,2);assert.equal(closed.minutes.presence,120);assert.equal(closed.minutes.bulk,60);assert.equal(closed.minutes.gap,480);assert.equal((await d1.prepare('SELECT signed_out FROM ck_attendance_days WHERE id=?').bind(p.dayId).first()).signed_out,'');console.log(`PASS D1 missing checkout, finite cutoff, existing rule, next shift one person-day; returns=${returns}`);}finally{await d1.close();f.DB.raw.close();}
});
