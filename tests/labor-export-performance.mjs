// Synthetic scale measurement. No remote accounts, database or user data.
import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
import {atTime,exportFixture,seedDaily} from './labor-export-fixture.mjs';
import {collectLaborExport} from '../shared/labor-export-ui.js';import {financeWorkbookData} from '../shared/labor-export-data.js';
const context={};vm.runInNewContext(fs.readFileSync('shared/xlsx.full.min.js','utf8'),context);const XLSX=context.XLSX;
await atTime('2026-01-02T11:00:00Z',async()=>{
 for(const count of [100,1000,10000]){const f=await exportFixture();for(let i=0;i<count;i++)seedDaily(f,i);let requests=0,payload=0;const start=performance.now(),request=async data=>{requests++;const result=await f.ok('office','sop_attendance_daily_export',data);payload+=Buffer.byteLength(JSON.stringify(result));return result;};
  const result=count<=1000?await collectLaborExport(request,{start:'2026-01-02',end:'2026-01-02'}):{rows:(await request({start_date:'2026-01-02',end_date:'2026-01-02'})).items,meta:{start:'2026-01-02',end:'2026-01-02',asOf:'2026-01-02T11:00:00Z'}};
  assert.equal(result.rows.length,Math.min(count,count>1000?200:count));const apiMs=performance.now()-start,rows=count>1000?Array.from({length:count},(_,i)=>({...result.rows[i%200],personKey:'PERF-'+i,name:'合成性能'+i})):result.rows,formatStart=performance.now(),data=financeWorkbookData(rows,result.meta),book=XLSX.utils.book_new();for(const [key,title] of [['daily','每日工时'],['spans','出勤时段'],['notes','说明']])XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet(data[key]),title);const bytes=XLSX.write(book,{type:'buffer',bookType:'xlsx'});console.log(JSON.stringify({people:count,api_scope:count<=1000?'complete_export':'first_page_only',requests,api_ms:Math.round(apiMs),response_bytes:payload,workbook_rows:rows.length,xlsx_build_ms:Math.round(performance.now()-formatStart),xlsx_bytes:bytes.length}));f.DB.raw.close();
 }
});
