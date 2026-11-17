import assert from 'node:assert/strict';import test from 'node:test';
import worker from '../worker-v2/index.js';import {materialReferenceFixture} from './batch-material-reference-fixture.mjs';
function form(type,id,key,extra={},contents='synthetic upload row'){
 const data=new FormData();for(const[k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:type,related_doc_id:id,client_req_id:key,...extra}))data.set(k,v);data.set('file',new File([contents],'synthetic.csv',{type:'text/csv'}));return data;
}
test('six independent cargo drafts upload concurrently and replay without duplicate material records',async t=>{
 const f=await materialReferenceFixture(t),draft='CGD-'+crypto.randomUUID(),keys=Array.from({length:6},()=> 'CGF-'+crypto.randomUUID()),results=await Promise.all(keys.map(key=>f.raw(form('cargo_draft',draft,key))));for(const r of results)assert.equal(r.ok,true,r.error);
 for(let i=0;i<keys.length;i++){const replay=await f.raw(form('cargo_draft',draft,keys[i]));assert.equal(replay.ok,true,replay.error);assert.equal(replay.attachment.id,keys[i]);assert.equal(replay.attachment.id,results[i].attachment.id);assert.equal(replay.attachment.file_key,results[i].attachment.file_key);}
 assert.equal((await f.first("SELECT count(*) n FROM sop_records WHERE kind='cargo_file' AND json_extract(state,'$.draft')=?",draft)).n,6);
 const changed=await f.raw(form('cargo_draft',draft,keys[0],{},'changed file'));assert.equal(changed.ok,false);assert.match(changed.error,/变化/);
});
test('lost saved-need acknowledgements reuse the original attachment and revision in sequence',async t=>{
 const f=await materialReferenceFixture(t),need=await f.need(),initial=(await f.current(need.id)).revision;let revision=initial;
 for(let i=0;i<6;i++){const data=form('sop_need',need.id,crypto.randomUUID(),{attachment_category:'work_material',material_kind:'shipping_document',revision}),r=await f.raw(data);assert.equal(r.ok,true,r.error);const replay=await f.raw(data);assert.equal(replay.ok,true,replay.error);assert.equal(replay.id,r.id);assert.equal(replay.revision,r.revision);revision=r.revision;}
 assert.equal(revision,initial+6);assert.equal((await f.first('SELECT count(*) n FROM v2_attachments WHERE related_doc_id=?',need.id)).n,6);
});
test('batch acknowledgements replay once; invalid files and unauthenticated writes create no rows',async t=>{
 const f=await materialReferenceFixture(t),before=(await f.first('SELECT count(*) n FROM v2_attachments WHERE related_doc_id=?',f.plan.id)).n;
 for(let i=0;i<6;i++){const data=form('inbound_plan',f.plan.id,crypto.randomUUID(),{need_id:f.a.id,attachment_category:'batch_work_material'}),r=await f.raw(data),replay=await f.raw(data);assert.equal(r.ok,true,r.error);assert.equal(replay.ok,true,replay.error);assert.equal(r.id,replay.id);}
 const total=(await f.first('SELECT count(*) n FROM v2_attachments WHERE related_doc_id=?',f.plan.id)).n;assert.equal(total,before+6);
 const invalid=form('cargo_draft','CGD-'+crypto.randomUUID(),'CGF-'+crypto.randomUUID());invalid.set('file',new File(['x'],'synthetic.exe'));assert.equal((await f.raw(invalid)).ok,false);
 const denied=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',body:form('cargo_draft','CGD-'+crypto.randomUUID(),'CGF-'+crypto.randomUUID())}),f.env);assert.equal((await denied.json()).ok,false);assert.equal((await f.first('SELECT count(*) n FROM v2_attachments WHERE related_doc_id=?',f.plan.id)).n,total);
});
