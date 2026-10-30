import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('inbound-receipt-fixture')};let cookie='';
 const call=async(action,b={})=>{const r=await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,...b})}),env);const next=r.headers.getSetCookie().find(x=>x.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];return rememberOperation(await r.json(),cookie);};
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+r.error);return r;};await ok('sop_login',{sop_key:'inbound-receipt-fixture'});await confirmOperation(call,'合成入库回执经办员');
 const fresh=()=>({client_req_id:'IBC-'+crypto.randomUUID(),create_intent:'IBD-'+crypto.randomUUID(),customer:'合成两组预约',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:2}],work_requests:[{title:'合成一次两组各一单',instructions:'隔离回执验证',department:'bulk',planned_quantity:2,planned_unit:'箱',cargo_groups:{draft_id:'CGD-'+crypto.randomUUID(),groups:[1,2].map(i=>({range:'LU-'+i,box_count:1,reservations:[{schema:2,quantity:1,unit:'箱',expected_ship_at:'2026-10-23',outbound_mode:'milk_express',shipping_range:'LU-'+i}]}))}}]});
 const counts=()=>Object.fromEntries([['inbounds','v2_inbound_plans'],['needs','sop_records'],['orders','v2_outbound_orders']].map(([k,table])=>[k,DB.raw.prepare('SELECT count(*) n FROM '+table+(k==='needs'?" WHERE kind='need'":'')).get().n]));
 const status=b=>ok('v2_inbound_plan_create_status',{request_id:b.client_req_id,create_intent:b.create_intent});return {DB,env,call,ok,fresh,counts,status};
}
test('two groups create exactly two orders and lookup/same frozen replay keep original IDs; changed content cannot replace the intent',async t=>{
 const f=await fixture(t),body=f.fresh();assert.equal((await f.status(body)).state,'unknown');const saved=await f.ok('v2_inbound_plan_create',body);assert.deepEqual(f.counts(),{inbounds:1,needs:1,orders:2});assert.deepEqual((await f.status(body)).result,saved);assert.deepEqual(await f.ok('v2_inbound_plan_create',body),saved);
 for(const change of [{customer:'修改客户'},{client_req_id:'IBC-'+crypto.randomUUID(),customer:'改内容换提交号'}]){const r=await f.call('v2_inbound_plan_create',{...body,...change});assert.equal(r.ok,false);assert.match(r.error,/冻结/);assert.deepEqual(f.counts(),{inbounds:1,needs:1,orders:2});}
 const stripped={...body};delete stripped.create_intent;assert.equal((await f.call('v2_inbound_plan_create',stripped)).ok,false);
 await f.ok('v2_inbound_plan_create',f.fresh());assert.deepEqual(f.counts(),{inbounds:2,needs:2,orders:4},'explicit genuinely new intent remains legal');
});
test('explicit business rejection proves not saved and a corrected new attempt can save normally',async t=>{
 const f=await fixture(t),bad=f.fresh();bad.work_requests[0].cargo_groups.groups[1].reservations[0].expected_ship_at='invalid-date';const rejected=await f.call('v2_inbound_plan_create',bad);assert.equal(rejected.ok,false);assert.equal(rejected.not_saved,true);assert.equal(rejected.receipt_state,'rejected');assert.equal((await f.status(bad)).state,'rejected');assert.deepEqual(f.counts(),{inbounds:0,needs:0,orders:0});await f.ok('v2_inbound_plan_create',f.fresh());assert.deepEqual(f.counts(),{inbounds:1,needs:1,orders:2});
});
test('failure in the middle of the business batch rolls back inbound, groups, both orders and success receipt',async t=>{
 const f=await fixture(t),bad=f.fresh(),batch=f.DB.batch;let inject=true;f.DB.batch=async statements=>{if(inject&&statements.some(s=>s.sql.includes('INSERT INTO v2_outbound_orders'))){inject=false;const i=statements.findIndex(s=>s.sql.includes('INSERT INTO v2_outbound_orders'));return batch([...statements.slice(0,i+1),f.DB.prepare('INSERT INTO missing_receipt_fixture_table VALUES(1)'),...statements.slice(i+1)]);}return batch(statements);};
 const result=await f.call('v2_inbound_plan_create',bad);assert.equal(result.not_saved,true);assert.equal((await f.status(bad)).state,'rejected');assert.deepEqual(f.counts(),{inbounds:0,needs:0,orders:0});f.DB.batch=batch;await f.ok('v2_inbound_plan_create',f.fresh());assert.equal(f.counts().orders,2);
});
test('a transaction committed before a transport-like database response error is recovered as the original saved result',async t=>{
 const f=await fixture(t),body=f.fresh(),batch=f.DB.batch;let lost=true;f.DB.batch=async statements=>{const out=await batch(statements);if(lost&&statements.some(s=>s.sql.includes("SET state='committed'"))){lost=false;throw Error('synthetic committed batch response lost');}return out;};const saved=await f.ok('v2_inbound_plan_create',body);assert.deepEqual((await f.status(body)).result,saved);assert.deepEqual(await f.ok('v2_inbound_plan_create',body),saved);assert.deepEqual(f.counts(),{inbounds:1,needs:1,orders:2});
});
test('concurrent same-ID submissions run one business transaction; status query and second click do not create anything',async t=>{
 const f=await fixture(t),body=f.fresh(),batch=f.DB.batch;let entered,release,held=false;const started=new Promise(r=>entered=r),gate=new Promise(r=>release=r);f.DB.batch=async statements=>{if(!held&&statements.some(s=>s.sql.includes('inbound_create_receipt_changed'))){held=true;entered();await gate;}return batch(statements);};const first=f.call('v2_inbound_plan_create',body);await started;assert.equal((await f.status(body)).state,'pending');const duplicate=await f.call('v2_inbound_plan_create',body);assert.equal(duplicate.receipt_state,'pending');assert.deepEqual(f.counts(),{inbounds:0,needs:0,orders:0});release();const saved=await first;assert.equal(saved.ok,true);assert.deepEqual(await f.ok('v2_inbound_plan_create',body),saved);assert.equal(f.counts().orders,2);
});
test('expired explicit replay fences a delayed old execution out of the atomic business commit',async t=>{
 const f=await fixture(t),body=f.fresh(),batch=f.DB.batch;let entered,release,held=false;const started=new Promise(r=>entered=r),gate=new Promise(r=>release=r);f.DB.batch=async statements=>{if(!held&&statements.some(s=>s.sql.includes('inbound_create_receipt_changed'))){held=true;entered();await gate;}return batch(statements);};const first=f.call('v2_inbound_plan_create',body);await started;f.DB.raw.prepare('UPDATE ck_inbound_create_receipts SET lease_until=0 WHERE request_id=?').run(body.client_req_id);assert.equal((await f.status(body)).can_retry,true);const second=await f.ok('v2_inbound_plan_create',body);release();assert.deepEqual(await first,second);assert.deepEqual(f.counts(),{inbounds:1,needs:1,orders:2});
});
test('operator changes cannot query or replay another operation context or bypass protection by omitting intent metadata',async t=>{
 const f=await fixture(t),body=f.fresh();await f.ok('v2_inbound_plan_create',body);await confirmOperation(f.call,'另一合成经办员');assert.equal((await f.status(body)).state,'unknown');assert.equal((await f.call('v2_inbound_plan_create',body)).ok,false);const stripped={...body};delete stripped.create_intent;assert.equal((await f.call('v2_inbound_plan_create',stripped)).ok,false);assert.equal(f.counts().orders,2);
});
