import test from 'node:test';
import assert from 'node:assert/strict';
import {startAtomicInbound} from '../worker-v2/atomic-native-start.js';
import {partialArrivalFixture as fixture} from './partial-arrival-fixture.mjs';
const tally='inbound_direct_tally';
async function snapshot(DB){
 const tables=['v2_inbound_plans','v2_ops_jobs','v2_ops_job_workers','sop_records','sop_events','ck_inbound_arrival_batches','v2_idempotency_keys'];
 return Promise.all(tables.map(async table=>({table,rows:(await DB.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()).results})));
}
for(const type of [tally,'inbound_bulk'])test('rollout off rejects direct partial dispatch as well as recognition and candidates: '+type,async t=>{
 const f=await fixture(t,{authenticated:true}),bulk=type==='inbound_bulk',p=await f.create(bulk?{biz_classes:['bulk','bulk_putaway'],bulk_external_inbound_no:f.code}:{});await f.receive(0);
 const before=await snapshot(f.DB);f.env.SOP_UPGRADE_ENABLED='false';
 const recognized=await f.ok('v2_inbound_resolve_code',{code:f.code,biz_class:bulk?'bulk':'direct_ship',job_type:type});assert.notEqual(recognized.kind,'system');
 const candidates=await f.ok('v2_inbound_plan_ops_candidates',{scene:'putaway',biz_class:bulk?'bulk':'direct_ship'});assert.ok(!candidates.items.some(x=>x.id===p.id));
 const r=await f.start(p,type);assert.equal(r.ok,false,JSON.stringify(r));assert.deepEqual(await snapshot(f.DB),before,'rejected dispatch creates no job, batch, audit, receipt or labor');
});
test('rollout off keeps the original fully arrived first tally and creates no partial snapshot',async t=>{
 const f=await fixture(t,{authenticated:true}),p=await f.create();await f.receive(0);await f.receive(1);f.env.SOP_UPGRADE_ENABLED='false';
 const j=await f.start(p);assert.equal(j.ok,true,JSON.stringify(j));assert.equal(j.arrival_batch,undefined);assert.equal((await f.DB.prepare('SELECT COUNT(*) n FROM ck_inbound_arrival_batches').first()).n,0);
});
test('concurrent on and off request bindings cannot share partial-dispatch permission',async t=>{
 const f=await fixture(t,{authenticated:true}),p=await f.create();await f.receive(0);const off={...f.env,SOP_UPGRADE_ENABLED:'false'},id='SYNTH-OFF-CONTEXT';
 const request={action:'sop_native_start',client_req_id:crypto.randomUUID(),payload:{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:f.code,job_type:tally,biz_class:'direct_ship',client_req_id:crypto.randomUUID()},workers:[{id,name:'合成关闭配置操作员'}],lead_id:id,estimated_minutes:15};
 const [a,b]=await Promise.all([f.call('sop_native_start',request,off),f.start(p)]);
 assert.equal(a.ok,false,JSON.stringify(a));assert.equal(b.ok,true,JSON.stringify(b));assert.equal((await f.DB.prepare('SELECT COUNT(*) n FROM ck_inbound_arrival_batches').first()).n,1);assert.equal((await f.DB.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE worker_id=?').bind(id).first()).n,0);
});
test('rollout change after preparation is checked inside the committing business transaction',async t=>{
 const f=await fixture(t),p=await f.create();await f.receive(0);const before=await snapshot(f.DB),e={...f.env,SOP_REQUEST_USER:{id:'SYNTH-POLICY-OFFICE',name:'合成配置核验员'}},base=f.DB.batch.bind(f.DB);let changed=false;
 e.DB={prepare:f.DB.prepare.bind(f.DB),async batch(ss){if(!changed&&ss.some(s=>s.sql.includes('CREATE TRIGGER IF NOT EXISTS ck_dispatch_worker_exclusive'))){changed=true;e.SOP_UPGRADE_ENABLED='false';}return base(ss);}};
 const id='SYNTH-POLICY-W',staff={workers:[{id,name:'合成在途配置操作员'}],lead_id:id,estimated_minutes:15,department:'direct_ship'};
 await assert.rejects(()=>startAtomicInbound(e,{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:f.code,job_type:tally,biz_class:'direct_ship',client_req_id:crypto.randomUUID()},staff,()=>{throw Error('unexpected number allocation');}));
 assert.equal(changed,true);assert.deepEqual(await snapshot(f.DB),before,'commit-time refusal rolls back all business writes');
});
