import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const body={customer:'Synthetic reupload',biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:['301000007201'],planned_qty:1}],work_requests:[{title:'Synthetic work',instructions:'Keep source audit',planned_quantity:1,planned_unit:'箱'}]};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,JSON.stringify(r));return r;};
 await ok('sop_identity');
 const create=()=>ok('v2_inbound_plan_create',structuredClone(body));
 const need=()=>DB.raw.prepare("SELECT * FROM sop_records WHERE kind='need' ORDER BY rowid LIMIT 1").get();
 const cancel=id=>ok('v2_inbound_plan_cancel',{inbound_plan_id:id,reason:'Synthetic cancelled upload'});
 return {DB,env,body,call,ok,create,need,cancel};
}

test('same-file courier reupload archives only the unstarted requirement and preserves source data and audit',async t=>{
 const f=await fixture(t),old=await f.create(),need=f.need();assert.ok(need,'the actual bundled create must create a work requirement');
 const duplicate=await f.call('v2_inbound_plan_create',structuredClone(f.body));assert.equal(duplicate.ok,false);assert.match(duplicate.error||duplicate.message,/已关联/);
 f.DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,file_name,file_key) VALUES('SYNTHETIC-FILE','sop_need',?,'same-synthetic.csv','synthetic/immutable')").run(need.id);
 const attachment=f.DB.raw.prepare("SELECT * FROM v2_attachments WHERE id='SYNTHETIC-FILE'").get();
 await f.cancel(old.id);
 const plan=f.DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(old.id),lines=f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').all(old.id),events=f.DB.raw.prepare('SELECT * FROM sop_events WHERE record_id=?').all(need.id);
 const fresh=await f.create();assert.notEqual(fresh.id,old.id);
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(old.id),plan);
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').all(old.id),lines);
 const archived=f.DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(need.id),state=JSON.parse(archived.state);assert.equal(state.status,'cancelled');assert.equal(state.replacement_inbound_plan_id,fresh.id);assert.equal(archived.revision,need.revision+1);
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM sop_events WHERE record_id=? ORDER BY rowid').all(need.id).slice(0,events.length),events);
 const event=f.DB.raw.prepare("SELECT * FROM sop_events WHERE record_id=? AND action='sop_inbound_replaced'").get(need.id);assert.equal(event.before_json,need.state);assert.equal(event.after_json,archived.state);
 assert.deepEqual(f.DB.raw.prepare("SELECT * FROM v2_attachments WHERE id='SYNTHETIC-FILE'").get(),attachment);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_item_history WHERE plan_id=?').get(old.id).n,1);
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need' AND json_extract(state,'$.status')='pending'").get().n,1);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n,0);
});

for(const patch of [{task_id:'SYNTHETIC-TASK'},{task_ids:['SYNTHETIC-TASK']},{result:{quantity:1,unit:'箱'}},{links:[{outbound_id:'SYNTHETIC-ORDER'}]},{cargo_groups:{groups:[{outputs:[{quantity:1}]}]}},{cargo_groups:{groups:[{processes:[{status:'approved'}]}]}}])test('legacy pending requirement with execution evidence stays protected: '+Object.keys(patch)[0],async t=>{
 const f=await fixture(t),old=await f.create(),need=f.need();await f.cancel(old.id);f.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify({...JSON.parse(need.state),...patch}),need.id);
 const before=f.DB.raw.prepare('SELECT * FROM sop_records ORDER BY id').all(),failed=await f.call('v2_inbound_plan_create',structuredClone(f.body));assert.equal(failed.ok,false);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM sop_records ORDER BY id').all(),before);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM ck_courier_plan_item_history WHERE replacement_plan_id!=''").get().n,0);
});

test('execution evidence committed after preflight rejects the entire replacement without archiving the old requirement',async t=>{
 const f=await fixture(t),old=await f.create(),need=f.need();await f.cancel(old.id);const batch=f.DB.batch.bind(f.DB);let changed;
 f.DB.batch=async statements=>{if(!changed&&statements.some(s=>s.sql.includes('sop_inbound_replaced'))){changed={...JSON.parse(need.state),result:{quantity:1,unit:'箱'}};f.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(changed),need.id);}return batch(statements);};
 assert.equal((await f.call('v2_inbound_plan_create',structuredClone(f.body))).ok,false);assert.ok(changed);assert.deepEqual(JSON.parse(f.need().state),changed);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM ck_courier_plan_item_history WHERE replacement_plan_id!=''").get().n,0);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_inbound_replaced'").get().n,0);
});

test('a claim failure rolls back requirement archival and a corrected retry archives once',async t=>{
 const f=await fixture(t),old=await f.create(),need=f.need();await f.cancel(old.id);const batch=f.DB.batch.bind(f.DB);let failed=false;
 f.DB.batch=async statements=>{if(!failed&&statements.some(s=>s.sql.includes('sop_inbound_replaced'))){failed=true;return batch([...statements,f.DB.prepare('INSERT INTO table_that_does_not_exist VALUES(1)')]);}return batch(statements);};
 const first=await f.call('v2_inbound_plan_create',structuredClone(f.body));assert.equal(first.ok,false);assert.equal(failed,true);assert.deepEqual(f.need(),need);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM ck_courier_plan_item_history WHERE replacement_plan_id!=''").get().n,0);
 await f.create();assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_inbound_replaced'").get().n,1);
});

test('concurrent bundled reuploads have one replacement and one requirement archival',async t=>{
 const f=await fixture(t),old=await f.create();await f.cancel(old.id);const prepare=f.DB.prepare.bind(f.DB);let reads=0,release;const gate=new Promise(r=>release=r);
 f.DB.prepare=sql=>{const s=prepare(sql);if(sql.includes('SELECT p.display_no')&&sql.includes('ck_courier_plan_items')){const first=s.first.bind(s);s.first=async()=>{const r=await first();if(++reads===2)release();await gate;return r;};}return s;};
 const replies=await Promise.all([f.call('v2_inbound_plan_create',structuredClone(f.body)),f.call('v2_inbound_plan_create',structuredClone(f.body))]);assert.equal(replies.filter(r=>r.ok).length,1);assert.equal(reads,2);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,2);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_inbound_replaced'").get().n,1);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need' AND json_extract(state,'$.status')='pending'").get().n,1);
});
