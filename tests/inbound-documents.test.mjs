import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {confirmInboundIssue,ensureInboundDocuments} from '../worker-v2/inbound-documents.js';
function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const plan=()=>call('v2_inbound_plan_create',{customer:'QA-print-issue',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:3}]});
 const detail=id=>call('v2_inbound_plan_detail',{id});return{DB,env,call,plan,detail};
}
test('preview reads do not issue; explicit confirmation is idempotent, preserves logistics and records the server principal',async()=>{
 const s=setup(),p=await s.plan();assert.equal(p.ok,true,p.error);
 const before=await s.detail(p.id),version=before.plan.issue_state.revision;assert.equal(before.plan.issue_state.state,'unissued');
 await s.detail(p.id);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_inbound_document_issues').get().n,0);
 const payload={id:p.id,revision:version,actor_name:'forged',client_req_id:'QA-issue-replay'};
 assert.equal((await s.call('v2_inbound_plan_confirm_issue',payload)).ok,true);
 assert.equal((await s.call('v2_inbound_plan_confirm_issue',payload)).ok,true);
 const after=await s.detail(p.id);assert.equal(after.plan.status,'pending');assert.equal(after.plan.issue_state.state,'issued');assert.equal(after.plan.issue_state.actor_name,'测试负责人');assert.ok(after.plan.issue_state.confirmed_at);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_inbound_document_issues').get().n,1);
 const list=await s.call('v2_inbound_plan_list');assert.equal(list.items[0].issue_state.state,'issued');
});
test('key edits require reissue and reject stale confirmations even with the earlier request id',async()=>{
 const s=setup(),p=await s.plan(),d=await s.detail(p.id),revision=d.plan.issue_state.revision;
 const payload={id:p.id,revision,client_req_id:'QA-old-print-confirm'};assert.equal((await s.call('v2_inbound_plan_confirm_issue',payload)).ok,true);
 assert.equal((await s.call('v2_inbound_plan_update',{id:p.id,biz_classes:['bulk'],remark:'QA-new unloading instruction'})).ok,true);
 let current=await s.detail(p.id);assert.equal(current.plan.issue_state.state,'needs_reissue');assert.ok(current.plan.issue_state.revision>revision);
 const stale=await s.call('v2_inbound_plan_confirm_issue',payload);assert.equal(stale.ok,false);assert.match(stale.error,/重新打印/);
 const latest=current.plan.issue_state.revision;assert.equal((await s.call('v2_inbound_plan_confirm_issue',{id:p.id,revision:latest})).ok,true);
 s.DB.raw.prepare('UPDATE v2_inbound_plan_lines SET actual_qty=2 WHERE plan_id=?').run(p.id);
 current=await s.detail(p.id);assert.equal(current.plan.issue_state.revision,latest);assert.equal(current.plan.issue_state.state,'issued');
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_inbound_document_issues').get().n,2);
});
test('cancelled plans and field sessions cannot confirm issue; formal environment receives no new schema',async()=>{
 const s=setup(),p=await s.plan(),d=await s.detail(p.id);
 await assert.rejects(()=>confirmInboundIssue({...s.env,SOP_REQUEST_USER:{id:'F',name:'QA-field',scope:'field',role:'manager'}},{id:p.id,revision:d.plan.issue_state.revision}),/办公室/);
 await s.call('v2_inbound_plan_cancel',{inbound_plan_id:p.id,reason:'QA-cancel'});
 const r=await s.call('v2_inbound_plan_confirm_issue',{id:p.id,revision:d.plan.issue_state.revision});assert.equal(r.ok,false);assert.match(r.error,/取消/);
 assert.equal((await s.detail(p.id)).plan.issue_state.state,'cancelled');assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM ck_inbound_document_issues').get().n,0);
 const formal={DB:database(),SOP_ENVIRONMENT:'production',SOP_UPGRADE_ENABLED:'true'};await ensureInboundDocuments(formal);assert.equal(formal.DB.raw.prepare("SELECT count(*) n FROM sqlite_master WHERE name LIKE 'ck_inbound_document_%'").get().n,0);
});
