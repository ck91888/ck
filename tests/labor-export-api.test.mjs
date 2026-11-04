import test from 'node:test';
import assert from 'node:assert/strict';
import {atTime} from './labor-export-fixture.mjs';
import {attendanceExportPage} from '../worker-v2/attendance-export.js';
import {exportRange,financePersonDay,financeWorkbookData,decimalHours} from '../shared/labor-export-data.js';
import {collectLaborExport} from '../shared/labor-export-ui.js';
import {exportFixture,seedDaily} from './labor-export-fixture.mjs';
const range={start_date:'2026-01-02',end_date:'2026-01-02'};
test('calendar boundaries are inclusive and invalid/reversed/oversized ranges are rejected',()=>{
 assert.equal(exportRange('2024-01-01','2024-12-31').days,366);assert.equal(exportRange('2025-12-31','2026-01-02').days,3);
 for(const pair of [['2026-02-29','2026-03-01'],['2026-01-03','2026-01-02'],['2025-01-01','2026-01-02'],['bad','2026-01-02']])assert.throws(()=>exportRange(...pair));
});
test('real Worker exports every person beyond 50 and 200, complete cross-year dates, stable sort, filters and raw precision',()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await exportFixture();for(let i=0;i<205;i++)seedDaily(f,i);seedDaily(f,0,'2025-12-31');seedDaily(f,0,'2026-01-01',{out:''});
 const source=JSON.stringify(f.DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all()),first=await f.ok('office','sop_attendance_daily_export',range);assert.equal(first.total,205);assert.equal(first.items.length,200);assert.equal(first.next.offset,200);assert.equal(first.items.filter(x=>x.badge.startsWith('EMP-')).length,0);
 const second=await f.ok('office','sop_attendance_daily_export',{...range,cursor:first.next,as_of:first.asOf,snapshot:first.snapshot});assert.equal(second.items.length,5);assert.equal(second.next,null);assert.equal(new Set([...first.items,...second.items].map(x=>x.personKey)).size,205);
 const all=await collectLaborExport(b=>f.ok('office','sop_attendance_daily_export',b),{start:'2025-12-31',end:'2026-01-02'});assert.equal(all.rows.length,207);assert.deepEqual([...new Set(all.rows.map(x=>x.date))],['2025-12-31','2026-01-01','2026-01-02']);assert.equal(all.rows.find(x=>x.date==='2026-01-01').checkoutState,'未签退（待核实）');
 const filtered=await f.ok('office','sop_attendance_daily_export',{...range,keyword:'合成财务人员0204',agency:'가온'});assert.equal(filtered.total,1);assert.equal((await f.ok('office','sop_attendance_daily_export',{...range,agency:'直招'})).total,0);
 assert.equal(JSON.stringify(f.DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all()),source);
}));
test('server rejects future, invalid cursors, stale/future asOf, changed day snapshot, field and kiosk authority',()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await exportFixture();seedDaily(f,1);seedDaily(f,2);const first=await f.ok('office','sop_attendance_daily_export',{...range,page_size:1});
 for(const patch of [{end_date:'2026-01-03'},{cursor:{date:'2026-01-01',offset:0}},{cursor:{date:'2026-01-02',offset:-1}},{page_size:201},{as_of:'2026-01-02T12:00:00Z'},{as_of:'2026-01-02T09:59:59Z'},{cursor:first.next,as_of:first.asOf,snapshot:'wrong'}])assert.equal((await f.call('office','sop_attendance_daily_export',{...range,...patch})).ok,false);
 seedDaily(f,3);assert.equal((await f.call('office','sop_attendance_daily_export',{...range,cursor:first.next,as_of:first.asOf,snapshot:first.snapshot,page_size:1})).ok,false);
 assert.equal((await f.call('A','sop_attendance_daily_export',range)).ok,false);
 const response=await f.worker.fetch(new Request('http://127.0.0.1/attendance/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_attendance_daily_export',...range})}),f.env);assert.equal((await response.json()).ok,false);
 for(const u of [{role:'manager',scope:'field'},{role:'kiosk',scope:'kiosk'},{role:'viewer',scope:'field'}])await assert.rejects(attendanceExportPage(range,{...f.env,SOP_REQUEST_USER:u},'2026-01-02T11:00:00Z',()=>{throw Error('must not read');}),/办公室/);
}));
test('same-name people remain separate; void/restore excludes and restores factual rows, not a name merge',()=>atTime('2026-01-02T11:00:00Z',async()=>{
 const f=await exportFixture(),a=seedDaily(f,1,'2026-01-02',{name:'合成同名人员'});seedDaily(f,2,'2026-01-02',{name:'合成同名人员'});let p=await f.ok('office','sop_attendance_daily_export',range);assert.equal(p.total,2);assert.equal(new Set(p.items.map(x=>x.personKey)).size,2);
 await f.ok('office','sop_attendance_void',{id:a.dayId,version:1,reason:'合成财务撤回误签到'});p=await f.ok('office','sop_attendance_daily_export',range);assert.equal(p.total,1);assert.equal(p.items[0].name,'合成同名人员');
 const original=f.DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(a.dayId);await f.ok('office','sop_attendance_restore',{id:a.dayId,version:original.version,reason:'合成财务恢复误撤回'});assert.equal((await f.ok('office','sop_attendance_daily_export',range)).total,2);
}));
test('department hours remain actual, sum before rounding, preserve gaps, no punch invented and no technical IDs in workbook',()=>{
 const item={record:{personId:'PRIVATE-ID',badgeId:'DAF-SYNTHETIC',name:'合成跨部门',agency:'直招',inAt:'2026-01-02T09:00:00+09:00',outAt:''},totals:{presence:60,bulk:0.49,direct_ship:0.49,import:0.49,other:0,dispatch:0.5,rest:10,unassigned:48.03,conflict:0,offShiftGapMinutes:60,flags:[],accounting:{rawPresenceMinutes:60,registeredRestMinutes:10,automaticRestMinutes:0}},attendanceSpans:[{id:'INTERNAL-SPAN',ordinal:2,kind:'return',start:'2026-01-02T09:00:00+09:00',end:'',totals:{presence:60,bulk:0.49,direct_ship:0.49,import:0.49,rest:10}}]};
 const row=financePersonDay(item,'2026-01-02','2026-01-02T01:00:00Z'),book=financeWorkbookData([row],{start:'2026-01-02',end:'2026-01-02',asOf:'2026-01-02T01:00:00Z'}),d=Object.fromEntries(book.daily[0].map((k,i)=>[k,book.daily[1][i]]));
 assert.equal(d['上班时间（韩国时间）'],'2026-01-02 09:00:00');assert.equal(d['下班时间（韩国时间）'],'未下班');assert.equal(d['实际操作合计小时'],0.02);assert.equal(d['大货小时'],0.01);assert.equal(d['离岗间隔小时'],1);assert.equal(decimalHours(1.47),0.02);assert.doesNotMatch(JSON.stringify(book),/PRIVATE-ID|INTERNAL-SPAN/);
});
test('paging stops without partial output on missing days, duplicate rows, errors, limit, cancellation or identity changes',async()=>{
 const row=financePersonDay({record:{personId:'P',name:'合成',badgeId:'DAF-X',inAt:'2026-01-02T00:00:00Z'},totals:{}},'2026-01-02','2026-01-02T01:00:00Z');
 const good={ok:true,start:'2026-01-02',end:'2026-01-03',date:'2026-01-02',filters:{agency:'',keyword:''},asOf:'2026-01-02T01:00:00Z',items:[row],total:1,snapshot:'s',next:{date:'2026-01-03',offset:0}},request=async()=>good;
 await assert.rejects(collectLaborExport(async()=>({...good,next:null}),{start:good.start,end:good.end}),/不完整/);
 await assert.rejects(collectLaborExport(request,{start:good.start,end:good.end}),/校验失败/);
 await assert.rejects(collectLaborExport(async()=>{throw Error('synthetic failed page');},{start:good.start,end:good.end}),/synthetic failed/);
 await assert.rejects(collectLaborExport(request,{start:good.start,end:good.end},{valid:()=>false}),/身份变化/);
 let allowed=true;await assert.rejects(collectLaborExport(async()=>{allowed=false;return good;},{start:good.start,end:good.end},{valid:()=>allowed}),/身份变化/);
});
test('client refuses more than 20000 person-days and duplicate or inconsistent page data instead of partial export',async()=>{
 const asOf='2026-01-02T01:00:00Z',range={start:'2026-01-02',end:'2026-01-02'},response=(offset,total)=>({ok:true,start:range.start,end:range.end,date:range.start,filters:{agency:'',keyword:''},asOf,items:Array.from({length:Math.min(200,total-offset)},(_,i)=>({date:range.start,personKey:'SYN-'+(offset+i)})),total,snapshot:'same',next:offset+200<total?{date:range.start,offset:offset+200}:null});
 await assert.rejects(collectLaborExport(async b=>response(b.cursor.offset,20001),range),/20000/);
 await assert.rejects(collectLaborExport(async b=>{const r=response(b.cursor.offset,201);if(b.cursor.offset)r.items[0].personKey='SYN-0';return r;},range),/人员重复/);
 await assert.rejects(collectLaborExport(async b=>{const r=response(b.cursor.offset,201);if(b.cursor.offset)r.asOf='2026-01-02T02:00:00Z';return r;},range),/校验失败/);
});
