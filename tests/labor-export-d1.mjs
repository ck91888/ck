// Synthetic local workerd only. Never opens a remote database.
import assert from 'node:assert/strict';
import {atTime,exportFixture,seedDaily} from './labor-export-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {collectLaborExport} from '../shared/labor-export-ui.js';
await atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await exportFixture();for(let i=0;i<205;i++)seedDaily(f,i);seedDaily(f,0,'2025-12-31');seedDaily(f,0,'2026-01-01',{out:''});
 const d1=await localD1(f.DB.raw);f.env.DB=d1;try{
  const source=await d1.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all(),spans=await d1.prepare('SELECT * FROM ck_attendance_spans ORDER BY id').all();
  const result=await collectLaborExport(data=>f.ok('office','sop_attendance_daily_export',data),{start:'2025-12-31',end:'2026-01-02'});
  assert.equal(result.rows.length,207);assert.equal(new Set(result.rows.map(x=>x.date+'|'+x.personKey)).size,207);assert.equal(result.rows.filter(x=>x.date==='2026-01-02').length,205);assert.deepEqual([...new Set(result.rows.map(x=>x.date))],['2025-12-31','2026-01-01','2026-01-02']);console.log('PASS native D1 complete 205 people, cross-year date range and two pages; no duplicate or current-page cap');
  assert.deepEqual((await d1.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all()).results,source.results);assert.deepEqual((await d1.prepare('SELECT * FROM ck_attendance_spans ORDER BY id').all()).results,spans.results);console.log('PASS native D1 read export preserves all punch and span evidence');
  const range={start_date:'2026-01-02',end_date:'2026-01-02'},first=await f.ok('office','sop_attendance_daily_export',{...range,page_size:1});
  await d1.prepare('UPDATE ck_attendance_people SET name=? WHERE id=?').bind('合成更正档案',result.rows.at(-1).personKey).run();
  // Parent names are historical evidence. Change a real source record instead.
  await d1.prepare('UPDATE ck_attendance_days SET name=? WHERE id=?').bind('合成更正打卡姓名','FIN-D-2026-01-02-204').run();
  const changed=await f.call('office','sop_attendance_daily_export',{...range,cursor:first.next,as_of:first.asOf,snapshot:first.snapshot});assert.equal(changed.ok,false);console.log('PASS native D1 changed-day snapshot rejects continuation instead of missing or duplicating people');
  assert.equal((await f.call('A','sop_attendance_daily_export',range)).ok,false);assert.equal((await f.call('office','sop_attendance_daily_export',{...range,end_date:'2026-01-03'})).ok,false);console.log('PASS native D1 historical office boundary and future date rejection');
  console.log('MEASURE native D1 export total read rows='+d1.metrics.reduce((n,m)=>n+(m.rows_read||0),0)+' queries='+d1.metrics.length+' wall_ms='+Math.round(d1.metrics.wall_ms));
 }finally{await d1.close();f.DB.raw.close();}
});
