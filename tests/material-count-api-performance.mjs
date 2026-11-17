// Independent published/current API comparison. Only synthetic local databases.
import assert from 'node:assert/strict';import fs from 'node:fs';import {mock} from 'node:test';import {pathToFileURL} from 'node:url';
import worker from '../worker-v2/index.js';
import {outboundFixture,outboundHash,publishedOutboundSql} from './outbound-page-material-fixture.mjs';
assert.ok(process.env.CK_MATERIAL_NULL_BASELINE_MODULE,'Provide the fixed published Worker module');
const {default:oldWorker}=await import(pathToFileURL(process.env.CK_MATERIAL_NULL_BASELINE_MODULE).href);
mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
const samples=[],predicate="COALESCE(json_extract(n.state,'$.cargo_groups'),0) NOT IN (0,'')";
const normalized=queries=>queries.map(batch=>batch.map(s=>({...s,sql:publishedOutboundSql(s.sql).replaceAll('NOT ('+predicate+')',"json_type(n.state,'$.cargo_groups') IS NULL").replaceAll(predicate,"json_type(n.state,'$.cargo_groups') IS NOT NULL")})));
async function sample(f,body){
 const times=[];let response,queries,metrics;
 for(let i=0;i<3;i++){
  if(f.env.DB.metrics)f.env.DB.metrics.length=0;const x=await f.read(body);times.push(x.ms);if(response)assert.deepEqual(x.r,response);response=x.r;queries=x.queries;
  if(f.env.DB.metrics){metrics=[...f.env.DB.metrics];assert.ok(metrics.every(x=>x.rows_written===0));}
 }
 return {response,queries,median_ms:[...times].sort((a,b)=>a-b)[1],...(metrics?{rows_read:metrics.reduce((n,x)=>n+x.rows_read,0),rows_written:metrics.reduce((n,x)=>n+x.rows_written,0)}:{})};
}
try{for(const spec of (process.env.CK_OUTBOUND_PAGE_CONFIGS||'100:100,1000:100,10000:100,100:1000,100:10000').split(',')){
 const [orders,history]=spec.split(':').map(Number),old=await outboundFixture(undefined,{worker:oldWorker,orders,history,sequential:true}),current=await outboundFixture(undefined,{worker,orders,history,sequential:true});
 try{
  const beforeOld=await old.snapshot(),beforeNew=await current.snapshot();
  for(const [scenario,body]of [['first-page',{limit:50}],['last-page',{limit:50,offset:orders-50}],...(orders===100&&history===100?[['has-1',{has_material:'1',limit:50}],['has-0',{has_material:'0',limit:50}]]:[])]){
   const a=await sample(old,body),b=await sample(current,body);let expected=structuredClone(a.response);
   if(!body.has_material)expected.items=expected.items.map(row=>row.id===old.oid(6)?{...row,material_count:1}:row);
   else{
    const all=(await old.read({limit:200})).r;assert.equal(all.items.length,all.total);const corrected=all.items.map(row=>row.id===old.oid(6)?{...row,material_count:1}:row),matches=corrected.filter(row=>body.has_material==='1'?row.material_count>0:row.material_count===0);
    expected={...expected,items:matches.slice(expected.offset,expected.offset+expected.limit),total:matches.length,page_count:Math.max(1,Math.ceil(matches.length/expected.limit))};
   }
   assert.deepEqual(b.response,expected,'only explicit null quantity/filter changes are permitted');assert.deepEqual(normalized(b.queries),normalized(a.queries),'only the two grouping predicates may change SQL');
   const entry={orders,history,scenario,before_ms:Number(a.median_ms.toFixed(3)),after_ms:Number(b.median_ms.toFixed(3)),response_sha256:outboundHash(b.response),exact_intended_response:true,exact_intended_sql:true,...(a.rows_read!=null?{before_rows_read:a.rows_read,after_rows_read:b.rows_read,rows_written:b.rows_written}:{})};
   samples.push(entry);console.log(JSON.stringify(entry));
  }
  assert.equal(await old.snapshot(),beforeOld);assert.equal(await current.snapshot(),beforeNew);
 }finally{await old.close();await current.close();}
}}finally{mock.timers.reset();}
if(process.env.CK_MATERIAL_NULL_METRICS)fs.writeFileSync(process.env.CK_MATERIAL_NULL_METRICS,JSON.stringify({synthetic:true,native:process.env.CK_OUTBOUND_PAGE_NATIVE_D1==='true',samples},null,2));
