import test from 'node:test';import assert from 'node:assert/strict';import worker from '../worker-v2/index.js';
import {historyApiFixture} from './work-print-history-api-fixture.mjs';import {PRINT_HISTORY_SQL} from './work-print-history-fixture.mjs';
test('real authenticated grouped API preserves history state and last-event order with legacy damaged JSON',async t=>{
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});const f=await historyApiFixture(worker,24,{native:process.env.CK_PRINT_HISTORY_NATIVE_D1==='true'});t.after(()=>f.close());const before=await f.snapshot();
 const {r,queries}=await f.measure('sop_need_groups',{limit:50});assert.equal(r.total,24);assert.equal(queries.filter(q=>q.sql===PRINT_HISTORY_SQL).length,1);
 const items=new Map(r.items.flatMap(g=>g.items).map(x=>[x.id,x]));assert.equal(items.get('SYN-PRINT-0').print_status.state,'unknown');
 const current=items.get('SYN-PRINT-1').print_status;assert.equal(current.state,'requested');assert.equal(current.request_count,10);assert.equal(current.current_count,4);assert.equal(current.last_by,'合成打印员9');assert.equal(current.last_requirement_version,10);
 assert.equal(items.get('SYN-PRINT-3').print_status.state,'reprint');assert.equal(items.get('SYN-PRINT-4').status,'cancelled');
 const filtered=(await f.call('sop_need_groups',{reference:'SYN-GROUP-23',limit:1,offset:9999})).r;assert.equal(filtered.total,1);assert.equal(filtered.offset,0);assert.equal(filtered.items[0].items[0].id,'SYN-PRINT-23');
 assert.equal(await f.snapshot(),before);
});
