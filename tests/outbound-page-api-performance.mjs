// Real authenticated Worker fetch, synthetic memory/local D1 only. No online load.
import assert from 'node:assert/strict';import fs from 'node:fs';import {mock} from 'node:test';import {pathToFileURL} from 'node:url';
import {outboundFixture,outboundHash,publishedOutboundSql} from './outbound-page-material-fixture.mjs';
mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
const module=process.env.CK_OUTBOUND_PAGE_API_MODULE?pathToFileURL(process.env.CK_OUTBOUND_PAGE_API_MODULE).href:new URL('../worker-v2/index.js',import.meta.url).href,{default:worker}=await import(module),samples=[];
const configs=(process.env.CK_OUTBOUND_PAGE_CONFIGS||'100:100,1000:100,10000:100,100:1000,100:10000').split(',').map(x=>x.split(':').map(Number));
try{for(const [orders,history]of configs){
 const f=await outboundFixture(undefined,{worker,orders,history,sequential:true});
 try{
  const before=await f.snapshot();
  for(const [scenario,body]of [['first-page',{limit:50}],['last-page',{limit:50,offset:orders-50}],...(orders===100&&history===100?[['has-1',{has_material:'1',limit:50}],['has-0',{has_material:'0',limit:50}]]:[])]){
   const timings=[];let result,queries,metrics=[];
   for(let i=0;i<3;i++){
    if(f.env.DB.metrics)f.env.DB.metrics.length=0;
    const sample=await f.read(body);timings.push(Number(sample.ms.toFixed(3)));if(result)assert.deepEqual(sample.r,result);result=sample.r;queries=sample.queries;
    if(f.env.DB.metrics){metrics=[...f.env.DB.metrics];assert.ok(metrics.every(m=>m.rows_written===0),'a list read must not synchronize configuration or mutate any record');}
   }
   const normalized=queries.map(batch=>batch.map(s=>({...s,sql:publishedOutboundSql(s.sql)})));
   const sample={orders,history,scenario,api_ms_median:[...timings].sort((a,b)=>a-b)[1],times:timings,payload_sha256:outboundHash(result),payload_bytes:Buffer.byteLength(JSON.stringify(result)),total:result.total,returned:result.items.length,offset:result.offset,normalized_queries_sha256:outboundHash(normalized),...(metrics.length?{rows_read:metrics.reduce((s,m)=>s+m.rows_read,0),rows_written:metrics.reduce((s,m)=>s+m.rows_written,0)}:{})};
   samples.push(sample);console.log(JSON.stringify(sample));
  }
  assert.equal(await f.snapshot(),before,'original material, audit, labor and business data stay unchanged');
 }finally{await f.close();}
}}finally{mock.timers.reset();}
if(process.env.CK_OUTBOUND_PAGE_BASELINE_METRICS){
 const baseline=JSON.parse(fs.readFileSync(process.env.CK_OUTBOUND_PAGE_BASELINE_METRICS,'utf8')).samples;
 const comparable=xs=>xs.map(({api_ms_median,times,rows_read,rows_written,...rest})=>rest);assert.deepEqual(comparable(samples),comparable(baseline),'all fields, order, material counts, paging and normalized SQL/args must match the published API');
}
if(process.env.CK_OUTBOUND_PAGE_METRICS)fs.writeFileSync(process.env.CK_OUTBOUND_PAGE_METRICS,JSON.stringify({module,synthetic:true,samples},null,2));
