import test from 'node:test';import assert from 'node:assert/strict';
import {materialReferenceFixture,holdMaterialBatch} from './batch-material-reference-fixture.mjs';
import {ensureBatchMaterialState} from '../worker-v2/batch-material-state.js';
const removing=statements=>statements.some(s=>s.sql.startsWith('INSERT INTO ck_batch_material_events'));
const referencing=statements=>statements.some(s=>s.sql.startsWith('INSERT INTO sop_events')&&s.args[3]==='sop_cargo_groups_save');

test('batch removal rechecks references from a need created after its initial read',async t=>{
 const f=await materialReferenceFixture(t),hold=holdMaterialBatch(f,removing),pending=f.raw(f.remove());await hold.ready;
 let before;try{const newer=await f.need();await f.raw(await f.group(newer.id)).then(r=>assert.equal(r.ok,true,r.error));before=await f.snapshot();}finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,false,JSON.stringify(result));assert.match(result.error,/资料.*(引用|保留|更新)/);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('a held new-need group save cannot reference a batch file removed before its commit',async t=>{
 const f=await materialReferenceFixture(t),removal=holdMaterialBatch(f,removing),removeRequest=f.raw(f.remove());await removal.ready;
 const newer=await f.need(),saving=holdMaterialBatch(f,referencing),saveRequest=f.raw(await f.group(newer.id));await saving.ready;
 let before;try{const unchanged=await f.snapshot();removal.release();const stale=await removeRequest;assert.equal(stale.ok,false,JSON.stringify(stale));assert.match(stale.error,/更新/);assert.equal(await f.snapshot(),unchanged);const removed=await f.raw(f.remove());assert.equal(removed.ok,true,removed.error);before=await f.snapshot();}finally{saving.release();}
 const saved=await saveRequest;saving.restore();removal.restore();assert.equal(saved.ok,false,JSON.stringify(saved));assert.match(saved.error,/更新|revision/);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('a new unreferenced child also forces a retry, then receives the material version exactly once',async t=>{
 const f=await materialReferenceFixture(t),body=f.remove(),hold=holdMaterialBatch(f,removing),pending=f.raw(body);await hold.ready;const newer=await f.need(),original=await f.current(newer.id),before=await f.snapshot();hold.release();const stale=await pending;hold.restore();assert.equal(stale.ok,false,JSON.stringify(stale));assert.match(stale.error,/更新/);assert.equal(await f.snapshot(),before);
 const saved=await f.raw(body);assert.equal(saved.ok,true,saved.error);const child=await f.current(newer.id);assert.equal(child.data.material_version,(original.data.material_version||0)+1);assert.equal(child.data.requirement_version,(original.data.requirement_version||1)+1);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);const committed=await f.snapshot();assert.deepEqual(await f.raw(body),saved);assert.equal(await f.snapshot(),committed);
});
test('a restore with a newly created child retries without losing the restored material notification',async t=>{
 const f=await materialReferenceFixture(t);await f.raw(f.remove()).then(r=>assert.equal(r.ok,true,r.error));const body=f.remove({action:'sop_batch_work_material_restore',material_revision:1}),hold=holdMaterialBatch(f,removing),pending=f.raw(body);await hold.ready;const newer=await f.need(),original=await f.current(newer.id),before=await f.snapshot();hold.release();assert.equal((await pending).ok,false);hold.restore();assert.equal(await f.snapshot(),before);const restored=await f.raw(body);assert.equal(restored.ok,true,restored.error);assert.equal((await f.current(newer.id)).data.requirement_version,(original.data.requirement_version||1)+1);assert.equal((await f.ok('sop_work_materials',{id:newer.id})).items[0].id,f.file.id);assert.equal(f.objects.size,1);
});
test('a reference added to an existing child still aborts the entire removal through its original revision guard',async t=>{
 const f=await materialReferenceFixture(t),hold=holdMaterialBatch(f,removing),pending=f.raw(f.remove());await hold.ready;await f.raw(await f.group(f.a.id)).then(r=>assert.equal(r.ok,true,r.error));const before=await f.snapshot();hold.release();const stale=await pending;hold.restore();assert.equal(stale.ok,false,JSON.stringify(stale));assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);
});
test('new children under a different inbound plan do not block the target material change',async t=>{
 const f=await materialReferenceFixture(t),other=await f.ok('v2_inbound_plan_create',{customer:'合成资料客户',biz_classes:['bulk']}),hold=holdMaterialBatch(f,removing),pending=f.raw(f.remove());await hold.ready;const newer=await f.need({source_id:other.id}),original=await f.current(newer.id);hold.release();const removed=await pending;hold.restore();assert.equal(removed.ok,true,removed.error);assert.deepEqual(await f.current(newer.id),original);assert.equal(f.objects.size,1);
});
test('replaying a committed removal after a later child creation remains a read-only original receipt',async t=>{
 const f=await materialReferenceFixture(t),body=f.remove(),saved=await f.raw(body);assert.equal(saved.ok,true,saved.error);await f.need();const before=await f.snapshot();assert.deepEqual(await f.raw(body),saved);assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);
});
test('a failure after the child-set claim rolls back state, events, notices and the transient claim',async t=>{
 const f=await materialReferenceFixture(t);await ensureBatchMaterialState(f.env);await f.query("CREATE TRIGGER synthetic_material_abort BEFORE INSERT ON ck_batch_material_events BEGIN SELECT RAISE(ABORT,'synthetic material rollback'); END").run();const before=await f.snapshot(),body=f.remove(),failed=await f.raw(body);assert.equal(failed.ok,false);assert.match(failed.error,/synthetic material rollback/);assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);await f.query('DROP TRIGGER synthetic_material_abort').run();assert.equal((await f.raw(body)).ok,true);assert.equal(f.objects.size,1);
});
test('same request and different requests keep one committed material event under concurrent attempts',async t=>{
 const f=await materialReferenceFixture(t),body=f.remove(),hold=holdMaterialBatch(f,removing),pending=f.raw(body);await hold.ready;const saved=await f.raw(body);assert.equal(saved.ok,true,saved.error);hold.release();assert.deepEqual(await pending,saved);hold.restore();assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_events')).n,1);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);const before=await f.snapshot();assert.equal((await f.raw({...body,action:'sop_batch_work_material_restore'})).ok,false);assert.equal(await f.snapshot(),before);
});
for(const [label,reference] of [
 ['public files',id=>({cargo_groups:{public_documents:[{id}]}})],
 ['archived outputs',id=>({cargo_groups:{archived_groups:[{outputs:[{documents:[{id}]}]}]}})],
 ['frozen allocations',id=>({links:[{cargo_allocations:[{documents:[{id}]}]}]})],
 ['frozen reservations',id=>({links:[{cargo_reservation:{groups:[{public_documents:[{id}]}]}}]})]
])test('existing '+label+' continue to prevent removal while preserving historical bytes',async t=>{
 const f=await materialReferenceFixture(t),row=await f.current(f.a.id);await f.query('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify({...row.data,...reference(f.file.id)}),row.id).run();const before=await f.snapshot(),result=await f.raw(f.remove());assert.equal(result.ok,false,JSON.stringify(result));assert.match(result.error,/引用|保留/);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('unrelated metadata containing the same ID is not a frozen document reference',async t=>{
 const f=await materialReferenceFixture(t),row=await f.current(f.a.id);await f.query('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify({...row.data,cargo_groups:{note:{id:f.file.id}}}),row.id).run();const result=await f.raw(f.remove());assert.equal(result.ok,true,result.error);assert.equal(f.objects.size,1);
});
test('old v1 data upgrades safely through two cold DB wrappers without rewriting business or history',async t=>{
 const f=await materialReferenceFixture(t);await ensureBatchMaterialState(f.env);await f.query('DROP TABLE ck_batch_material_need_set_guards').run();const before=await f.snapshot(),db=f.env.DB;
 const wrap=()=>({prepare:db.prepare.bind(db),batch:db.batch.bind(db)});await Promise.all([ensureBatchMaterialState({...f.env,DB:wrap()}),ensureBatchMaterialState({...f.env,DB:wrap()})]);assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);assert.equal((await f.raw(f.remove())).ok,true);
});
test('a failed additive DDL is not cached and the same binding can retry without altering historical rows',async t=>{
 const f=await materialReferenceFixture(t);await ensureBatchMaterialState(f.env);await f.query('DROP TABLE ck_batch_material_need_set_guards').run();const before=await f.snapshot(),db=f.env.DB;let reject=true;
 const wrapper={prepare(sql){const stmt=db.prepare(sql);if(sql.includes('CREATE TABLE IF NOT EXISTS ck_batch_material_need_set_guards'))return {...stmt,async run(){if(reject){reject=false;throw Error('synthetic additive DDL failure');}return stmt.run();}};return stmt;},batch:db.batch.bind(db)};
 await assert.rejects(()=>ensureBatchMaterialState({...f.env,DB:wrapper}),/synthetic additive DDL failure/);assert.equal(await f.snapshot(),before);await ensureBatchMaterialState({...f.env,DB:wrapper});assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);
});
test('different concurrent request keys permit only one material revision and leave no transient claims',async t=>{
 const f=await materialReferenceFixture(t),hold=holdMaterialBatch(f,removing),pending=f.raw(f.remove());await hold.ready;const winner=await f.raw(f.remove());assert.equal(winner.ok,true,winner.error);hold.release();const loser=await pending;hold.restore();assert.equal(loser.ok,false);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_events')).n,1);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);assert.equal(f.objects.size,1);
});
test('cancellation after the initial material read still rejects the complete material transaction',async t=>{
 const f=await materialReferenceFixture(t),hold=holdMaterialBatch(f,removing),pending=f.raw(f.remove());await hold.ready;await f.query("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?",f.plan.id).run();const before=await f.snapshot();hold.release();assert.equal((await pending).ok,false);hold.restore();assert.equal(await f.snapshot(),before);assert.equal((await f.first('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards')).n,0);
});
test('disabled rollout, work-chain or non-staging configuration rejects material changes without writes',async t=>{
 const f=await materialReferenceFixture(t),before=await f.snapshot();for(const [key,value]of [['SOP_UPGRADE_ENABLED','false'],['SOP_WORK_CHAIN_ENABLED','false'],['SOP_ENVIRONMENT','production']]){const original=f.env[key];f.env[key]=value;try{const result=await f.raw(f.remove());assert.equal(result.ok,false,JSON.stringify(result));assert.equal(await f.snapshot(),before);}finally{f.env[key]=original;}}assert.equal((await f.raw(f.remove())).ok,true);assert.equal(f.objects.size,1);
});
