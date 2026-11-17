import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {changeBatchMaterial,readBatchMaterials} from '../worker-v2/batch-work-materials.js';
async function fixture(){
 const DB=database(),objects=new Map(),principal={id:'M',name:'QA manager',role:'manager',scope:'office'};
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ACCEPT_NEW:'true',SOP_USERS_JSON:JSON.stringify([{...principal,key:'batch-fixture-only'}]),R2_BUCKET:{async put(key,bytes,meta){objects.set(key,{body:bytes,httpMetadata:meta.httpMetadata});},async get(key){return objects.get(key);},async delete(){throw Error('Must not delete any object');}}};let cookie='';
 const raw=async b=>{const multi=b instanceof FormData,r=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{Cookie:cookie,...operationHeaders(cookie),...(!multi?{'Content-Type':'application/json'}:{})},body:multi?b:JSON.stringify(b)}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];return rememberOperation(await r.json(),cookie);};
 const call=(action,b={})=>raw({action,client_req_id:crypto.randomUUID(),...b});await call('sop_login',{sop_key:'batch-fixture-only'});await confirmOperation(call,principal.name);
 const plan=await call('v2_inbound_plan_create',{customer:'QA batch',biz_classes:['bulk']});assert.equal(plan.ok,true,plan.error);
 const create=async(department='bulk',source=plan.id)=>{const r=await call('sop_need_create',{customer:'QA batch',title:'QA shared materials',instructions:'QA instructions',source_type:'inbound',source_id:source,planned_quantity:10,planned_unit:'箱',department,owner:'QA office',reason:'QA additional department work'});assert.equal(r.ok,true,r.error);return (await call('sop_get',{id:r.id})).record;};
 const a=await create(),b=await create('direct_ship');
 const upload=async(name='shared.csv')=>{const form=new FormData();for(const[k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:plan.id,need_id:a.id,attachment_category:'batch_work_material',client_req_id:crypto.randomUUID()}))form.set(k,v);form.set('file',new File(['QA row'],name,{type:'text/csv'}));const r=await raw(form);assert.equal(r.ok,true,r.error);return r;};
 const file=await upload();const current=async id=>(await call('sop_get',{id})).record;
 const command=(remove=true,revision=0)=>({action:remove?'sop_batch_work_material_remove':'sop_batch_work_material_restore',id:a.id,attachment_id:file.id,material_revision:revision,client_req_id:crypto.randomUUID()});
 return {DB,env,objects,principal,raw,call,plan,a,b,file,create,upload,current,command};
}
test('shared removal and restore propagate to all linked views, counts, print revisions and immutable history without deleting blobs',async()=>{
 const s=await fixture(),n=await s.current(s.a.id),ob=await s.call('v2_outbound_order_create',{customer:n.customer,biz_class:'bulk',outbound_mode:'customer_pickup',expected_ship_at:'2026-10-03',sop_existing_need_id:n.id,sop_need_revision:n.revision,sop_link_quantity:4});assert.equal(ob.ok,true,ob.error);
 const beforeA=await s.current(s.a.id),beforeB=await s.current(s.b.id),detail=await s.call('v2_inbound_plan_detail',{id:s.plan.id}),revision=detail.plan.issue_state.revision;
 assert.equal((await s.call('v2_inbound_plan_confirm_issue',{id:s.plan.id,revision})).ok,true);
 const cmd=s.command(),r=await s.raw(cmd);assert.equal(r.ok,true,r.error);assert.equal(r.removed,true);assert.equal(s.objects.size,1);
 const group=await s.call('sop_batch_work_materials',{id:s.a.id});assert.equal(group.items.length,0);assert.equal(group.removed_items[0].id,s.file.id);assert.equal(group.removed_items[0].changed_by,'QA manager');
 for(const n of [s.a,s.b])assert.equal((await s.call('sop_work_materials',{id:n.id})).items.length,0);
 assert.equal((await s.call('v2_attachment_list',{related_doc_type:'inbound_plan',related_doc_id:s.plan.id})).items.length,0);
 assert.equal((await s.call('v2_outbound_order_detail',{id:ob.id})).attachments.length,0);
 assert.equal((await s.call('v2_outbound_order_list',{has_material:'0'})).items.find(x=>x.id===ob.id).material_count,0);
 assert.equal((await s.call('v2_outbound_order_list',{has_material:'1'})).items.length,0);
 assert.equal((await s.current(s.a.id)).requirement_version,beforeA.requirement_version+1);assert.equal((await s.current(s.b.id)).requirement_version,beforeB.requirement_version+1);
 assert.equal((await s.call('v2_inbound_plan_detail',{id:s.plan.id})).plan.issue_state.state,'needs_reissue');
 assert.equal((await s.call('v2_inbound_plan_confirm_issue',{id:s.plan.id,revision})).ok,false);
 const logs=s.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_batch_work_material_remove'").get().n;assert.equal(logs,2);
 assert.deepEqual(await s.raw(cmd),r);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,1);assert.equal(s.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_batch_work_material_remove'").get().n,logs);
 const newer=await s.create();assert.equal((await s.call('sop_work_materials',{id:newer.id})).items.length,0);
 const restore=await s.raw(s.command(false,1));assert.equal(restore.ok,true,restore.error);for(const n of [s.a,s.b,newer])assert.equal((await s.call('sop_work_materials',{id:n.id})).items[0].id,s.file.id);
 assert.equal((await s.call('v2_outbound_order_list',{has_material:'1'})).items[0].material_count,1);assert.equal(s.objects.size,1);
});
test('a shared blob under another attachment ID remains visible; generic physical delete is blocked; replacement upload is independent',async()=>{
 const s=await fixture(),other=await s.call('v2_inbound_plan_create',{customer:'QA batch',biz_classes:['bulk']}),n=await s.create('bulk',other.id);
 const f=s.DB.raw.prepare('SELECT * FROM v2_attachments WHERE id=?').get(s.file.id);
 s.DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at) VALUES('OTHER-SAME-BLOB','inbound_plan',?,'batch_work_material',?,?,?,?,?,?)").run(other.id,f.file_name,f.file_key,f.file_size,f.content_type,f.uploaded_by,f.created_at);
 assert.equal((await s.call('v2_attachment_delete',{id:s.file.id})).ok,false);
 assert.equal((await s.raw(s.command())).ok,true);
 assert.equal((await s.call('sop_work_materials',{id:n.id})).items[0].id,'OTHER-SAME-BLOB');assert.equal(s.objects.size,1);
 const fresh=await s.upload('replacement.csv');const list=await s.call('sop_batch_work_materials',{id:s.a.id});assert.deepEqual(list.items.map(x=>x.id),[fresh.id]);assert.equal(list.removed_items[0].id,s.file.id);assert.equal(s.objects.size,2);
});
test('permissions and plan membership are checked server-side, including field manager and cancelled plans',async()=>{
 const s=await fixture(),cmd=s.command();
 for(const u of [{...s.principal,scope:'field'},{id:'D',name:'QA dispatch',role:'dispatcher',departments:['bulk']},{id:'V',name:'QA viewer',role:'viewer',departments:['bulk']}])await assert.rejects(()=>changeBatchMaterial(cmd,s.env,u),/权限|办公室/);
 const view=await readBatchMaterials({action:'sop_batch_work_materials',id:s.a.id},s.env,{id:'V',role:'viewer',departments:['bulk']});assert.equal(view.can_manage,false);
 const other=await s.call('v2_inbound_plan_create',{customer:'QA batch',biz_classes:['bulk']}),n=await s.create('bulk',other.id);assert.equal((await s.raw({...cmd,id:n.id})).ok,false);
 s.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(s.plan.id);assert.equal((await s.raw(cmd)).ok,false);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,0);
});
test('concurrent revision claims permit one change; stale restores and conflicting request reuse cannot change state',async()=>{
 const s=await fixture(),first=s.command(),second=s.command(),results=await Promise.all([s.raw(first),s.raw(second)]);assert.equal(results.filter(x=>x.ok).length,1);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,1);
 assert.equal((await s.raw(s.command(false,0))).ok,false);const winner=results[0].ok?first:second;assert.equal((await s.raw({...winner,action:'sop_batch_work_material_restore'})).ok,false);
 const state=await s.call('sop_batch_work_materials',{id:s.a.id});assert.equal(state.removed_items[0].material_revision,1);
});
test('transaction failure rolls back file state, all child changes, downstream revisions and document version',async()=>{
 const s=await fixture(),beforeA=await s.current(s.a.id),beforeB=await s.current(s.b.id),beforeDoc=(await s.call('v2_inbound_plan_detail',{id:s.plan.id})).plan.issue_state.revision;
 s.DB.raw.exec(`CREATE TRIGGER qa_reject_material_change BEFORE UPDATE OF state ON sop_records WHEN NEW.id='${s.b.id}' BEGIN SELECT RAISE(ABORT,'QA injected failure'); END;`);
 const r=await s.raw(s.command());assert.equal(r.ok,false);assert.match(r.error,/QA injected/);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_state').get().n,0);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,0);
 assert.equal((await s.current(s.a.id)).revision,beforeA.revision);assert.equal((await s.current(s.b.id)).revision,beforeB.revision);assert.equal((await s.call('v2_inbound_plan_detail',{id:s.plan.id})).plan.issue_state.revision,beforeDoc);assert.equal(s.objects.size,1);
});
test('ambiguous committed response retries exactly once and removal with no open children still invalidates issued print',async()=>{
 const s=await fixture();s.DB.raw.prepare("UPDATE sop_records SET state=json_set(state,'$.status','closed') WHERE id IN (?,?)").run(s.a.id,s.b.id);
 const before=(await s.call('v2_inbound_plan_detail',{id:s.plan.id})).plan.issue_state;await s.call('v2_inbound_plan_confirm_issue',{id:s.plan.id,revision:before.revision});
 const original=s.DB.batch.bind(s.DB);let lost=false;s.DB.batch=async statements=>{const r=await original(statements);if(!lost&&statements.some(x=>x.sql.startsWith('INSERT INTO ck_batch_material_events'))){lost=true;throw Error('QA lost database acknowledgement');}return r;};
 const cmd=s.command(),r=await s.raw(cmd);assert.equal(r.ok,true,r.error);assert.deepEqual(await s.raw(cmd),r);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,1);assert.equal((await s.call('v2_inbound_plan_detail',{id:s.plan.id})).plan.issue_state.state,'needs_reissue');assert.equal(s.objects.size,1);
});
