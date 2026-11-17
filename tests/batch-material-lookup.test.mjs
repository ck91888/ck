import test from 'node:test';import assert from 'node:assert/strict';
import {materialReferenceFixture,holdMaterialBatch} from './batch-material-reference-fixture.mjs';
import {ensureBatchMaterialNeedLookup} from '../worker-v2/batch-material-state.js';
import {seedMaterialHistory,lookupIndex,inboundChildren} from './batch-material-lookup-fixture.mjs';
const index=async f=>f.first("SELECT sql FROM sqlite_master WHERE type='index' AND name=?",lookupIndex);

test('inbound lookup preserves every child and the old department/time/rowid iteration order',async t=>{
 const f=await materialReferenceFixture(t);
 await f.ok('sop_batch_work_materials',{id:f.a.id});await seedMaterialHistory(f,1000);
 const sql='SELECT * FROM sop_records WHERE '+inboundChildren,before=(await f.query(sql,f.plan.id).all()).results,snapshot=await f.snapshot();
 const oldPlan=await f.query('EXPLAIN QUERY PLAN '+sql,f.plan.id).all();assert.match(JSON.stringify(oldPlan),/sop_records_kind/);
 await ensureBatchMaterialNeedLookup(f.env);assert.equal(await f.snapshot(),snapshot);assert.deepEqual((await f.query(sql,f.plan.id).all()).results,before);assert.equal(before.length,6);
 const plan=await f.query('EXPLAIN QUERY PLAN '+sql,f.plan.id).all();assert.match(JSON.stringify(plan),new RegExp(lookupIndex));
 for(const statement of ['SELECT COUNT(*) n FROM sop_records WHERE '+inboundChildren,'SELECT 1 FROM sop_records WHERE '+inboundChildren+' AND id NOT IN (SELECT value FROM json_each(?))'])
  assert.match(JSON.stringify(await f.query('EXPLAIN QUERY PLAN '+statement,...(statement.includes('json_each')?[f.plan.id,JSON.stringify(before.map(r=>r.id))]:[f.plan.id])).all()),new RegExp(lookupIndex));
 assert.doesNotMatch(JSON.stringify(await f.query("EXPLAIN QUERY PLAN SELECT * FROM sop_records WHERE kind='need' AND department='bulk'").all()),new RegExp(lookupIndex));
});
test('material reads and invalid mutations do not install the historical lookup index',async t=>{
 const f=await materialReferenceFixture(t);await f.ok('sop_batch_work_materials',{id:f.a.id});assert.equal(await index(f),null);
 for(const body of [f.remove({material_revision:99}),f.remove({attachment_id:'SYN-NOT-OWNED'}),f.remove({client_req_id:'bad'})])assert.equal((await f.raw(body)).ok,false);
 assert.equal(await index(f),null);assert.equal((await f.raw(f.remove())).ok,true);assert.ok(await index(f));
 const info=(await f.query("PRAGMA index_list('sop_records')").all()).results.find(r=>r.name===lookupIndex);assert.equal(info.unique,0);assert.equal(info.partial,1);
});
test('successful receipt replay does not initialize or rebuild the lookup',async t=>{
 const f=await materialReferenceFixture(t),body=f.remove(),result=await f.raw(body);assert.equal(result.ok,true,result.error);
 await f.query('DROP INDEX '+lookupIndex).run();const db=f.env.DB;f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};
 const before=await f.snapshot();assert.deepEqual(await f.raw(body),result);assert.equal(await index(f),null);assert.equal(await f.snapshot(),before);
});
test('two cold old-schema wrappers converge on one additive index and keep all business bytes',async t=>{
 const f=await materialReferenceFixture(t),before=await f.snapshot(),db=f.env.DB,wrap=()=>({prepare:db.prepare.bind(db),batch:db.batch.bind(db)});
 assert.equal(await index(f),null);await Promise.all([ensureBatchMaterialNeedLookup({...f.env,DB:wrap()}),ensureBatchMaterialNeedLookup({...f.env,DB:wrap()})]);
 assert.ok(await index(f));assert.equal(await f.snapshot(),before);assert.equal((await f.raw(f.remove())).ok,true);
});
test('failed index initialization is not cached, writes no business rows and is safely retryable',async t=>{
 const f=await materialReferenceFixture(t),db=f.env.DB;let fail=true;
 f.env.DB={prepare:db.prepare.bind(db),async batch(statements){if(fail&&statements.some(s=>s.sql.includes('CREATE INDEX IF NOT EXISTS '+lookupIndex))){fail=false;throw Error('synthetic index unavailable');}return db.batch(statements);}};
 const before=await f.snapshot(),body=f.remove(),result=await f.raw(body);assert.equal(result.ok,false);assert.match(result.error,/synthetic index unavailable/);assert.equal(await f.snapshot(),before);assert.equal(await index(f),null);
 assert.equal((await f.raw(body)).ok,true);assert.ok(await index(f));
});
test('disabled rollout does not initialize the index or change historical data',async t=>{
 const f=await materialReferenceFixture(t),before=await f.snapshot();for(const [key,value]of [['SOP_UPGRADE_ENABLED','false'],['SOP_WORK_CHAIN_ENABLED','false'],['SOP_ENVIRONMENT','production']]){
  const original=f.env[key];f.env[key]=value;await ensureBatchMaterialNeedLookup(f.env);assert.equal((await f.raw(f.remove())).ok,false);f.env[key]=original;
 }
 assert.equal(await index(f),null);assert.equal(await f.snapshot(),before);
});
test('non-inbound historical JSON scalars stay excluded and duplicate plan keys stay legal',async t=>{
 const f=await materialReferenceFixture(t);
 for(const [i,state]of ['null','false','0','""','{}','[]',JSON.stringify({source_type:'outbound',source_id:f.plan.id})].entries())await f.query("INSERT INTO sop_records VALUES(?,'need',0,'bulk',?,'2026-11-03T00:00:00Z')",'SYN-JSON-'+i,state).run();
 await ensureBatchMaterialNeedLookup(f.env);await f.need();assert.equal((await f.raw(f.remove())).ok,true);assert.equal((await f.query('SELECT * FROM sop_records WHERE '+inboundChildren,f.plan.id).all()).results.length,2);
});
test('an old malformed need remains visible as an error without rewriting or silently ignoring it',async t=>{
 const f=await materialReferenceFixture(t);await f.ok('sop_batch_work_materials',{id:f.a.id});
 // Simulate a genuinely old corrupt row, before JSON-dependent document
 // triggers existed. Reinstall those triggers before exercising the API.
 const triggers=(await f.query("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='sop_records'").all()).results;
 for(const row of triggers)await f.query('DROP TRIGGER '+row.name).run();
 await f.query("INSERT INTO sop_records VALUES('SYN-CORRUPT','need',0,'bulk','{','2026-11-03T00:00:00Z')").run();
 for(const row of triggers)await f.query(row.sql).run();
 const before=await f.snapshot(),result=await f.raw(f.remove());assert.equal(result.ok,false);assert.match(result.error,/malformed JSON/);assert.equal(await index(f),null);assert.equal(await f.snapshot(),before);
 assert.equal((await f.call('sop_batch_work_materials',{id:f.a.id})).ok,true,'a read-only material listing must not start the index build');
});
test('indexed membership guard rejects a changed child set even when its size stays identical',async t=>{
 const f=await materialReferenceFixture(t),old=await f.need(),hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.startsWith('INSERT INTO ck_batch_material_events'))),pending=f.raw(f.remove());await hold.ready;
 let before;try{
  await f.need();const row=await f.current(old.id);await f.query('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify({...row.data,source_id:'SYN-MOVED-PLAN'}),old.id).run();
  assert.equal((await f.query('SELECT * FROM sop_records WHERE '+inboundChildren,f.plan.id).all()).results.length,2);before=await f.snapshot();
 }finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,false);assert.match(result.error,/更新/);assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);
});
for(const change of ['delete','source-type'])test('index maintenance preserves the guard when a child leaves via '+change,async t=>{
 const f=await materialReferenceFixture(t),child=await f.need(),hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.startsWith('INSERT INTO ck_batch_material_events'))),pending=f.raw(f.remove());await hold.ready;
 let before;try{if(change==='delete')await f.query('DELETE FROM sop_records WHERE id=?',child.id).run();else {const row=await f.current(child.id);await f.query('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify({...row.data,source_type:'outbound'}),child.id).run();}before=await f.snapshot();}finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,false);assert.match(result.error,/更新/);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
