// Timing includes real request/auth, full history, grouping, filters, pagination,
// JSON serialization. Data preparation and row snapshots are outside the clock.
import assert from 'node:assert/strict';import fs from 'node:fs';import {mock} from 'node:test';import {pathToFileURL} from 'node:url';
import {historyApiFixture,historyHash} from './work-print-history-api-fixture.mjs';import {PRINT_HISTORY_SQL} from './work-print-history-fixture.mjs';
mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
const module=process.env.CK_PRINT_HISTORY_API_MODULE?pathToFileURL(process.env.CK_PRINT_HISTORY_API_MODULE).href:new URL('../worker-v2/index.js',import.meta.url).href,{default:worker}=await import(module),samples=[];
try{for(const size of (process.env.CK_PRINT_HISTORY_SIZES||'100,1000,10000').split(',').map(Number)){
 const f=await historyApiFixture(worker,size,{native:process.env.CK_PRINT_HISTORY_NATIVE_D1==='true'});
 try{
  const before=await f.snapshot();
  for(const [scenario,body]of [['first-page',{limit:50}],...(size===10000?[['late-filter',{reference:'SYN-GROUP-9999',limit:1,offset:9999}]]:[])]){
   const {r,text,ms,queries}=await f.measure('sop_need_groups',body);assert.equal(r.total,scenario==='first-page'?size:1);assert.equal(r.items.length,scenario==='first-page'?Math.min(50,size):1);assert.equal(r.offset,0);
   const history=queries.filter(q=>q.sql===PRINT_HISTORY_SQL);assert.equal(history.length,1);
   const sample={size,events:f.eventCount,scenario,api_ms:Number(ms.toFixed(3)),payload_bytes:Buffer.byteLength(text),payload_sha256:historyHash(text),total:r.total,returned:r.items.length,offset:r.offset,queries:queries.length,sql_sha256:historyHash(queries.map(q=>q.sql)),history_query_sha256:historyHash(history)};samples.push(sample);console.log(JSON.stringify(sample));
  }
  assert.equal(await f.snapshot(),before,'read API preserves original needs, numbers, audit, labor, results and inventory data');
 }finally{await f.close();}
}}finally{mock.timers.reset();}
if(process.env.CK_PRINT_HISTORY_BASELINE_METRICS){
 const baseline=JSON.parse(fs.readFileSync(process.env.CK_PRINT_HISTORY_BASELINE_METRICS,'utf8')).samples;
 const withoutTime=xs=>xs.map(({api_ms,...rest})=>rest);assert.deepEqual(withoutTime(samples),withoutTime(baseline),'complete published API payloads, pagination, queries and history arguments must match');
}
if(process.env.CK_PRINT_HISTORY_METRICS)fs.writeFileSync(process.env.CK_PRINT_HISTORY_METRICS,JSON.stringify({module,samples},null,2));
