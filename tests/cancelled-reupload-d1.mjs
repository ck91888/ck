// Real local workerd D1; synthetic records only, no remote database or account.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,JSON.stringify(r));return r;};
const payload={customer:'Synthetic local D1 reupload',biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:['301000007401','301000007402']}],work_requests:[{title:'Synthetic requirement',instructions:'Keep all history',planned_quantity:2,planned_unit:'箱'}]};
await ok('sop_identity');const old=await ok('v2_inbound_plan_create',payload);await ok('v2_inbound_plan_cancel',{inbound_plan_id:old.id,reason:'Synthetic cancellation'});
const d1=await localD1(DB.raw);env.DB=d1;
try{
 const need=await d1.prepare('SELECT * FROM sop_records WHERE id=?').bind(old.needs[0].id).first(),batch=d1.batch.bind(d1);let fail=true;
 d1.batch=async statements=>{if(fail&&statements.some(s=>s.sql.includes('sop_inbound_replaced'))){fail=false;return batch([...statements,d1.prepare('SELECT * FROM synthetic_missing_table')]);}return batch(statements);};
 assert.equal((await call('v2_inbound_plan_create',payload)).ok,false);assert.equal(fail,false);assert.deepEqual(await d1.prepare('SELECT * FROM sop_records WHERE id=?').bind(need.id).first(),need);assert.equal((await d1.prepare("SELECT count(*) n FROM ck_courier_plan_item_history WHERE replacement_plan_id!=''").first()).n,0);
 const request=crypto.randomUUID(),fresh=await ok('v2_inbound_plan_create',{...payload,client_req_id:request}),retry=await ok('v2_inbound_plan_create',{...payload,client_req_id:request});assert.equal(retry.id,fresh.id);assert.notEqual(fresh.id,old.id);
 const archived=await d1.prepare('SELECT * FROM sop_records WHERE id=?').bind(need.id).first();assert.equal(archived.revision,need.revision+1);assert.equal(JSON.parse(archived.state).status,'cancelled');
 const event=await d1.prepare("SELECT * FROM sop_events WHERE record_id=? AND action='sop_inbound_replaced'").bind(need.id).first();assert.equal(event.before_json,need.state);assert.equal(event.after_json,archived.state);
 assert.equal((await d1.prepare('SELECT count(*) n FROM ck_courier_plan_item_history').first()).n,2);assert.equal((await d1.prepare('SELECT count(*) n FROM v2_inbound_plans').first()).n,2);assert.equal((await d1.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_inbound_replaced'").first()).n,1);
 assert.equal((await d1.prepare('SELECT count(*) n FROM v2_ops_jobs').first()).n,0);assert.equal((await d1.prepare('SELECT count(*) n FROM v2_outbound_orders').first()).n,0);
 console.log('PASS local workerd D1: two-waybill claim and single audited requirement archival, transaction rollback, same-request retry, no duplicate jobs/orders.');
}finally{await d1.close();DB.raw.close();}
