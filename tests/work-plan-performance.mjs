// Isolated synthetic benchmark, not a CI wall-clock assertion or a live load test.
// CK_WORK_PLAN_MODULE may point to a frozen published module for the baseline.
import {performance} from 'node:perf_hooks';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import fs from 'node:fs';
import {factsFixture} from './work-plan-performance-fixture.mjs';
const url=process.env.CK_WORK_PLAN_MODULE?pathToFileURL(process.env.CK_WORK_PLAN_MODULE).href:new URL('../worker-v2/work-plan-list.js',import.meta.url).href;
const {workPlanFacts}=await import(url),samples=[];
for(const size of (process.env.CK_WORK_PLAN_SIZES||'100,1000,10000').split(',').map(Number)){
 const f=factsFixture(size),start=performance.now();await workPlanFacts(f.env,f.groups,f.knownPlans,f.orders);const ms=performance.now()-start;
 const output=JSON.stringify(f.groups),queries=JSON.stringify(f.reads),hash=x=>createHash('sha256').update(x).digest('hex');
 const r={size,orders:f.orders.length,plans:f.plans.length,association_ms:Number(ms.toFixed(3)),output_bytes:Buffer.byteLength(output),output_sha256:hash(output),queries_sha256:hash(queries)};samples.push(r);console.log(JSON.stringify(r));
}
if(process.env.CK_WORK_PLAN_METRICS)fs.writeFileSync(process.env.CK_WORK_PLAN_METRICS,JSON.stringify({module:url,samples},null,2));
