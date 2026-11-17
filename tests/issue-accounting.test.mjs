import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {handleSop,guardLegacy} from '../worker-v2/sop.js';
import {FIELD_ACTIONS} from '../worker-v2/access-control.js';
function setup(){
 const DB=database(),users=[{id:'M',key:'manager',name:'Synthetic manager',role:'manager'},{id:'S',key:'service',name:'Synthetic service',role:'service',departments:['bulk']},{id:'D',key:'field',name:'Synthetic field',role:'dispatcher',scope:'field',departments:['bulk']},{id:'V',key:'viewer',name:'Synthetic viewer',role:'viewer',departments:['bulk']},{id:'X',key:'other',name:'Synthetic other department',role:'service',departments:['import']}];
 const env={DB,SOP_UPGRADE_ENABLED:'true',SOP_USERS_JSON:JSON.stringify(users)};
 DB.raw.exec("INSERT INTO v2_issue_tickets(id,biz_class,issue_description) VALUES('ACCOUNTING-QA','bulk','Synthetic requirement')");
 const call=(action,data={},key='manager')=>handleSop({action,sop_key:key,client_req_id:crypto.randomUUID(),...data},env);
 const read=async()=>{const r=await call('sop_get',{id:'ISSUE-ACCOUNTING-QA'});assert.equal(r.ok,true,r.error);return r.record;};
 const mutate=async(action,data={},key)=>{const r=await read();return call(action,{id:r.id,revision:r.revision,...data},key);};
 return {DB,env,call,read,mutate};
}
test('reminder and service confirmation retain legacy filters, actor/time, revision audit and idempotence without charges',async()=>{
 const {DB,env,call,read,mutate}=setup();try{
  await call('sop_issue_adopt',{legacy_id:'ACCOUNTING-QA',native:true});
  assert.equal(FIELD_ACTIONS.has('sop_issue_request_accounting'),true);
  assert.equal(FIELD_ACTIONS.has('sop_issue_confirm_accounted'),false);
  assert.equal(FIELD_ACTIONS.has('v2_issue_mark_accounted'),false);
  for(const role of ['viewer','other'])assert.equal((await mutate('sop_issue_request_accounting',{},role)).ok,false);
  const before=await read(),key=crypto.randomUUID(),body={id:before.id,revision:before.revision,client_req_id:key,note:''};
  const first=await call('sop_issue_request_accounting',body,'field');assert.equal(first.ok,true,first.error);
  assert.deepEqual(await call('sop_issue_request_accounting',body,'field'),first);
  assert.equal((await call('sop_issue_request_accounting',{...body,client_req_id:crypto.randomUUID()},'field')).ok,false);
  let x=await read();assert.equal(x.accounting.accounted,0);assert.equal(x.accounting.accounting_required,1);assert.equal(x.accounting.accounting_required_by,'Synthetic field');
  const at=x.accounting.accounting_required_at;
  assert.equal((await mutate('sop_issue_request_accounting',{note:'Synthetic extra labels'},'service')).ok,true);
  assert.equal((await read()).accounting.accounting_required_at,at);
  assert.equal((await mutate('sop_issue_confirm_accounted',{},'service')).ok,false,'cannot confirm before warehouse feedback');
  assert.equal((await mutate('sop_issue_feedback',{message:'Synthetic completed work'},'field')).ok,true);
  assert.equal((await mutate('sop_issue_confirm_accounted',{},'field')).ok,false);
  x=await read();const confirm={id:x.id,revision:x.revision,client_req_id:crypto.randomUUID()};
  const done=await call('sop_issue_confirm_accounted',confirm,'service');assert.equal(done.ok,true,done.error);assert.deepEqual(await call('sop_issue_confirm_accounted',confirm,'service'),done);
  x=await read();assert.equal(x.accounting.accounted,1);assert.equal(x.accounting.accounted_by,'Synthetic service');
  for(const action of ['sop_issue_request_accounting','sop_issue_confirm_accounted'])assert.equal((await mutate(action,{note:'must not clear confirmation'},'service')).ok,false);
  assert.deepEqual((await read()).accounting,x.accounting);
  assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_issue_tickets WHERE accounting_required=1 AND accounted=0").get().n,0);
  assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_issue_tickets WHERE accounted=1").get().n,1);
  for(const action of ['v2_issue_mark_accounted','v2_issue_mark_accounting_required'])assert.match(await guardLegacy({action,id:'ACCOUNTING-QA'},env),/本问题详情/);
  const events=DB.raw.prepare("SELECT * FROM sop_events WHERE record_id='ISSUE-ACCOUNTING-QA' AND action='sop_issue_confirm_accounted'").all();assert.equal(events.length,1);assert.equal(JSON.parse(events[0].before_json).accounting.accounted,0);assert.equal(JSON.parse(events[0].after_json).accounting.accounted,1);
 }finally{DB.raw.close();}
});
test('existing paid tickets stay paid when adopted; stale competing confirmation cannot change audit or reminder',async()=>{
 const {DB,call,read,mutate}=setup();try{
  DB.raw.exec("UPDATE v2_issue_tickets SET status='closed',accounting_required=1,accounting_required_by='Legacy requester',accounting_required_at='2026-01-01',accounting_note='Legacy memo',accounted=1,accounted_by='Legacy service',accounted_at='2026-01-02'");
  await call('sop_issue_adopt',{legacy_id:'ACCOUNTING-QA',native:true});const old=(await read()).accounting;
  assert.equal((await mutate('sop_issue_request_accounting',{note:'new note'})).ok,false);assert.deepEqual((await read()).accounting,old);
  DB.raw.exec("UPDATE v2_issue_tickets SET accounted=0,accounted_by='',accounted_at=''");
  const x=await read(),stale={id:x.id,revision:x.revision};
  await mutate('sop_issue_request_accounting',{note:'Updated memo'},'service');
  assert.equal((await call('sop_issue_confirm_accounted',stale,'service')).ok,false);
  assert.equal((await read()).accounting.accounted,0);
 }finally{DB.raw.close();}
});
