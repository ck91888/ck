import test from 'node:test';import assert from 'node:assert/strict';
import {completionDay,completionDate} from '../worker-v2/completion-date.js';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
test('completion dates keep calendar days and interpret historical timestamps in Korea without rewriting them',()=>{
 for(const value of ['2026-10-05','2026-10-05T17:12','2026-10-05T00:00:01+09:00','2026-10-04T15:00:01Z'])assert.equal(completionDay(value),'2026-10-05');
 assert.equal(completionDay('2026-10-05T14:59:59.999Z'),'2026-10-05');assert.equal(completionDay('2026-10-05T15:00:00Z'),'2026-10-06');
 assert.equal(completionDate('2026-10-05','2026-10-04T15:00:01Z'),'2026-10-04T15:00:01Z');assert.equal(completionDate('2026-10-06','2026-10-04T15:00:01Z'),'2026-10-06');
 assert.equal(completionDate(''),'');for(const value of ['2026-02-30','2026-10-05T25:12','wrong'])assert.throws(()=>completionDay(value),/完成日期/);
});
test('new work dates persist without time; editing other requirements retains an original historical timestamp',async()=>{
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const n=await call('sop_need_create',{source_type:'inventory',department:'bulk',customer:'QA-date',title:'QA-date work',instructions:'QA instructions',supply_chain_no:'QA-stock',deadline:'2026-10-05'});assert.equal(n.ok,true,n.error);
 let r=(await call('sop_get',{id:n.id})).record;assert.equal(r.deadline,'2026-10-05');
 const state=JSON.parse(DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(n.id).state);state.deadline='2026-10-04T15:00:01Z';DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(state),n.id);
 r=(await call('sop_get',{id:n.id})).record;
 const saved=await call('sop_need_update',{id:n.id,revision:r.revision,instructions:'QA changed instructions',owner:r.owner,deadline:'2026-10-05'});assert.equal(saved.ok,true,saved.error);
 r=(await call('sop_get',{id:n.id})).record;assert.equal(r.deadline,'2026-10-04T15:00:01Z');
 const bad=await call('sop_need_update',{id:n.id,revision:r.revision,instructions:r.instructions,owner:r.owner,deadline:'2026-02-30'});assert.equal(bad.ok,false);assert.equal((await call('sop_get',{id:n.id})).record.deadline,state.deadline);
});
