import test from 'node:test';import assert from 'node:assert/strict';
import {materialReferenceFixture,holdMaterialBatch} from './batch-material-reference-fixture.mjs';
const removing=statements=>statements.some(s=>s.sql.startsWith('INSERT INTO ck_batch_material_events'));
const referencing=statements=>statements.some(s=>s.sql.startsWith('INSERT INTO sop_events')&&s.args[3]==='sop_cargo_groups_save');

test('batch removal rechecks references from a need created after its initial read',async t=>{
 const f=await materialReferenceFixture(t),hold=holdMaterialBatch(f,removing),pending=f.raw(f.remove());await hold.ready;
 let before;try{const newer=await f.need();await f.raw(await f.group(newer.id)).then(r=>assert.equal(r.ok,true,r.error));before=await f.snapshot();}finally{hold.release();}
 const result=await pending;hold.restore();assert.equal(result.ok,false,JSON.stringify(result));assert.match(result.error,/资料.*(引用|保留)/);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('a held new-need group save cannot reference a batch file removed before its commit',async t=>{
 const f=await materialReferenceFixture(t),removal=holdMaterialBatch(f,removing),removeRequest=f.raw(f.remove());await removal.ready;
 const newer=await f.need(),saving=holdMaterialBatch(f,referencing),saveRequest=f.raw(await f.group(newer.id));await saving.ready;
 let before;try{removal.release();const removed=await removeRequest;assert.equal(removed.ok,true,removed.error);before=await f.snapshot();}finally{saving.release();}
 const saved=await saveRequest;saving.restore();removal.restore();assert.equal(saved.ok,false,JSON.stringify(saved));assert.match(saved.error,/资料.*(移除|撤下|更新)/);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
