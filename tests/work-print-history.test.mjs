import test from 'node:test';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {workNeedPrintFacts,workNeedPrintSummary} from '../worker-v2/work-need-print.js';
import {historyFixture,historyEnvironment,publishedPrintFacts,PRINT_HISTORY_SQL} from './work-print-history-fixture.mjs';
async function compare(items,events,options){
 const old=structuredClone(items),next=structuredClone(items),before=structuredClone(events),a=historyEnvironment(events,options),b=historyEnvironment(events,options);
 assert.equal(await publishedPrintFacts(a.env,old),old);assert.equal(await workNeedPrintFacts(b.env,next),next);
 assert.deepEqual(next,old);assert.deepEqual(b.reads,a.reads);assert.deepEqual(events,before);
 if(next.some(x=>x.kind==='need'))assert.equal(b.reads[0].sql,PRINT_HISTORY_SQL);
 assert.deepEqual(workNeedPrintSummary(next),workNeedPrintSummary(old));return next;
}
for(const size of [100,1000])test('print history indexing matches published full output and exact query at '+size+' needs',async()=>{const f=await historyFixture(size);await compare(f.items,f.events);});
test('ten thousand needs retain the frozen published complete output and query hashes',async()=>{
 const f=await historyFixture(10000),{env,reads}=historyEnvironment(f.events);await workNeedPrintFacts(env,f.items);const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
 assert.equal(hash(f.items),'9c1e61cfb16050a8031afbaf002795f2f47770bc2016176bbd1cf528dd6e2c8a');assert.equal(hash(reads),'1d650d64521e941112a9d930af9f8a84f9b48eaa3ed8aba27795fb3023d6fb09');
});
test('equal timestamps keep request ordering, oldest matching version still counts, and mixed kinds are untouched',async()=>{
 const f=await historyFixture(3,12);f.events=f.events.map(e=>({...e,created_at:'2026-11-03T00:00:00.000Z'})).sort((a,b)=>b.request_id.localeCompare(a.request_id));f.items.push({id:f.items[1].id,kind:'task',print_status:{state:'synthetic-existing'}});
 const result=await compare(f.items,f.events),own=f.events.filter(x=>x.record_id===f.items[1].id);
 assert.equal(result[1].print_status.last_by,own[0].actor_name);assert.equal(result[1].print_status.last_at,own[0].created_at);assert.equal(result[1].print_status.request_count,12);assert.equal(result[1].print_status.state,'requested');assert.equal(result[0].print_status.state,'unknown');assert.equal(result[3].print_status.state,'synthetic-existing');
});
test('legacy missing fields, malformed JSON and primitive JSON preserve original fallback values',async()=>{
 const f=await historyFixture(1);f.events=['{broken','','[]','0','true','"legacy"','{}'].map((after_json,i)=>({record_id:f.items[0].id,created_at:'SYN-TIME-'+i,after_json}));
 const result=await compare(f.items,f.events);assert.equal(result[0].print_status.state,'reprint');assert.equal(result[0].print_status.current_count,0);assert.equal(result[0].print_status.last_by,'');assert.equal(result[0].print_status.last_requirement_version,undefined);
});
test('literal JSON null keeps the existing error and preceding mutation order, rather than silently changing policy',async()=>{
 const f=await historyFixture(3),events=[...f.events,{record_id:f.items[2].id,after_json:'null'}],a=structuredClone(f.items),b=structuredClone(f.items);
 await assert.rejects(publishedPrintFacts(historyEnvironment(events).env,a),TypeError);await assert.rejects(workNeedPrintFacts(historyEnvironment(events).env,b),TypeError);assert.deepEqual(b,a);
});
test('duplicate need IDs, special string keys and strict numeric/string/null/NaN matching remain identical',async()=>{
 const ids=['__proto__','constructor','toString','__proto__',0,'0',null,undefined,NaN],items=ids.map(id=>({id,kind:'need',title:'合成'})),events=ids.map((record_id,i)=>({record_id,actor_name:'SYN-'+i,after_json:'{}'}));
 await compare(items,events);
});
test('no needs performs no query; missing results returns unknown without changing array or item identity',async()=>{
 await compare([{kind:'task',id:'SYN-TASK'}],[]);const items=[{kind:'need',id:'SYN-EMPTY'}],original=items[0];
 const result=await workNeedPrintFacts(historyEnvironment([],{missingResults:true}).env,items);assert.equal(result,items);assert.equal(result[0],original);assert.equal(original.print_status.state,'unknown');await compare([{kind:'need',id:'SYN-EMPTY'}],[],{missingResults:true});
});
