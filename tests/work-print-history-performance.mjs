// Read helper timing only; exact published output/query differential is mandatory.
import assert from 'node:assert/strict';import fs from 'node:fs';import {performance} from 'node:perf_hooks';import {createHash} from 'node:crypto';
import {workNeedPrintFacts} from '../worker-v2/work-need-print.js';import {historyFixture,historyEnvironment,publishedPrintFacts} from './work-print-history-fixture.mjs';
const samples=[],hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
for(const size of (process.env.CK_PRINT_HISTORY_SIZES||'100,1000,10000').split(',').map(Number)){
 const f=await historyFixture(size),results=[];
 for(const [name,fn]of [['published',publishedPrintFacts],['indexed',workNeedPrintFacts]]){
  const items=structuredClone(f.items),{env,reads}=historyEnvironment(f.events),start=performance.now();await fn(env,items);const ms=performance.now()-start;
  results.push({name,ms:Number(ms.toFixed(3)),output_sha256:hash(items),queries_sha256:hash(reads)});
 }
 assert.equal(results[0].output_sha256,results[1].output_sha256);assert.equal(results[0].queries_sha256,results[1].queries_sha256);
 const sample={size,events:f.events.length,results};samples.push(sample);console.log(JSON.stringify(sample));
}
if(process.env.CK_PRINT_HISTORY_METRICS)fs.writeFileSync(process.env.CK_PRINT_HISTORY_METRICS,JSON.stringify(samples,null,2));
