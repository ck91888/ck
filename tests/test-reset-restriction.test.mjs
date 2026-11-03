import test from 'node:test';import assert from 'node:assert/strict';
import {restrictionFixture} from './test-reset-restriction-fixture.mjs';
import {resetAction,STAGING_HOSTS} from '../worker-v2/test-data-reset.js';
import entry from '../worker-v2/staging-entry.js';
const input=()=>({requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
test('restriction baseline: direct manager start is unavailable without any SQL',async t=>{
 const f=await restrictionFixture(t),before=await f.snapshot();await assert.rejects(resetAction('start',input(),f.env,{id:'synthetic-manager',role:'manager'}),e=>e.status===503);assert.equal(await f.snapshot(),before);
});
test('restriction baseline: an existing run cannot advance through its HTTP step',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot(),response=await f.request('/api/test-data/step',{body:{runId:f.runId}});assert.equal(response.status,503);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('restriction baseline: switching the old flag off cannot open business writes during a reset',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot(),response=await f.request('/api',{changes:{SOP_TEST_RESET_ENABLED:'false'},body:{action:'v2_inbound_plan_create',client_req_id:crypto.randomUUID(),customer:'must not write',biz_classes:['bulk']}});assert.equal(response.status,503);assert.equal(await f.snapshot(),before);
});
test('direct execution stays disabled for every flag and malformed or completed request without SQL',async()=>{
 const DB={prepare(){assert.fail('disabled execution must not prepare SQL');},batch(){assert.fail('disabled execution must not submit SQL');}};
 for(const flag of ['true','false',undefined])for(const action of ['start','step'])for(const body of [{},input(),{runId:'a'.repeat(32)}])await assert.rejects(resetAction(action,body,{DB,SOP_TEST_RESET_ENABLED:flag},{role:'manager'}),e=>e.status===503&&e.reset_disabled===true);
 await assert.rejects(resetAction('step',{}, {DB},{role:'viewer'}),e=>e.status===403);
});
test('both hosts reject direct reset execution for all feature/upgrade switch combinations',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot();
 for(const host of STAGING_HOSTS)for(const changes of [{},{SOP_TEST_RESET_ENABLED:'false'},{SOP_TEST_RESET_ENABLED:undefined},{SOP_UPGRADE_ENABLED:'false'},{SOP_TEST_RESET_ENABLED:'false',SOP_UPGRADE_ENABLED:'false'}])for(const action of ['start','step']){
  const response=await f.request('/api/test-data/'+action,{host,changes,body:action==='start'?input():{runId:f.runId}});assert.ok([404,503].includes(response.status));assert.equal((await response.json()).ok,false);
 }
 assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('active maintenance blocks office, field, kiosk, object and GET routes even with switches off',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot();
 const cases=[['/api','office',{action:'v2_inbound_plan_create',customer:'must not write',biz_classes:['bulk']}],['/001/api','A',{action:'sop_task_dispatch',need_id:f.need.id,department:'bulk',job_type:'bulk_op',workers:[{id:f.person.badgeId,name:f.person.name}],lead_id:f.person.badgeId,estimated_minutes:10}],['/attendance/api','kiosk',{action:'sop_attendance_checkin',name:'must not checkin',agency:'가온'}]];
 for(const host of [...STAGING_HOSTS,'synthetic-alias.invalid'])for(const changes of [{},{SOP_TEST_RESET_ENABLED:'false'},{SOP_UPGRADE_ENABLED:'false'},{SOP_TEST_RESET_ENABLED:'false',SOP_UPGRADE_ENABLED:'false'}])for(const [path,scope,body]of cases){const response=await f.request(path,{host,changes,scope,body:{client_req_id:crypto.randomUUID(),...body}});assert.equal(response.status,503);assert.equal((await response.json()).maintenance,true);}
 const form=new FormData();for(const [k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:f.plan.id,attachment_category:'arrival_photo',client_req_id:crypto.randomUUID()}))form.set(k,v);form.set('file',new File(['synthetic'],'arrival.csv',{type:'text/csv'}));assert.equal((await f.request('/api',{body:form,changes:{SOP_TEST_RESET_ENABLED:'false'}})).status,503);
 assert.equal((await f.request('/file?key=synthetic%2Fpreserved-object',{changes:{SOP_TEST_RESET_ENABLED:'false'}})).status,503);assert.equal((await f.request('/api?action=v2_inbound_plan_list')).status,503);assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('there is no cached inactive state across flag changes or DB bindings',async t=>{
 const f=await restrictionFixture(t,{active:true});await f.q('UPDATE ck_test_resets SET completed_at=? WHERE id=?',new Date().toISOString(),f.runId).run();assert.equal((await f.request('/api',{body:{action:'sop_identity'},changes:{SOP_TEST_RESET_ENABLED:'false'}})).status,200);
 await f.q("UPDATE ck_test_resets SET completed_at='' WHERE id=?",f.runId).run();const db=f.env.DB,before=await f.snapshot(),response=await f.request('/api',{changes:{DB:{prepare:db.prepare.bind(db),batch:db.batch.bind(db)},SOP_TEST_RESET_ENABLED:'false'},body:{action:'v2_inbound_plan_create',customer:'must not write',biz_classes:['bulk']}});assert.equal(response.status,503);assert.equal(await f.snapshot(),before);
});
test('cold identity cannot recreate partially cleared attendance while paused',async t=>{
 const f=await restrictionFixture(t,{active:true,partial:true}),before=await f.snapshot(),db=f.env.DB;
 for(const flag of ['true','false']){const response=await f.request('/api',{body:{action:'sop_identity'},changes:{SOP_TEST_RESET_ENABLED:flag,DB:{prepare:db.prepare.bind(db),batch:db.batch.bind(db)}}}),data=await response.json();assert.equal(data.ok,true,JSON.stringify(data));assert.equal(data.user.role,'manager');assert.ok(data.user.operation_context.name);}
 assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);assert.equal(await f.snapshot(),before);
});
test('scheduled maintenance pauses for active runs independently of the old switches',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot();let calls=0;t.mock.method(f.worker,'scheduled',async()=>{calls++;});
 for(const changes of [{},{SOP_TEST_RESET_ENABLED:'false'},{SOP_UPGRADE_ENABLED:'false'}])await entry.scheduled({scheduledTime:Date.now()+86400000},{...f.env,...changes});assert.equal(calls,0);assert.equal(await f.snapshot(),before);
 await f.q('UPDATE ck_test_resets SET completed_at=? WHERE id=?',new Date().toISOString(),f.runId).run();await entry.scheduled({scheduledTime:Date.now()},f.env);assert.equal(calls,1,'normal background behavior remains delegated');
});
test('unknown maintenance state fails closed for business and scheduled work',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot(),db=f.env.DB,broken={prepare(sql){if(sql.includes('sqlite_master')&&sql.includes('ck_test_resets'))throw Error('synthetic status unavailable');return db.prepare(sql);},batch:db.batch.bind(db)},response=await f.request('/api',{body:{action:'v2_inbound_plan_create'},changes:{DB:broken,SOP_TEST_RESET_ENABLED:'false'}});assert.equal(response.status,503);
 await assert.rejects(entry.scheduled({scheduledTime:Date.now()},{...f.env,DB:broken}),/synthetic status unavailable/);assert.equal(await f.snapshot(),before);
});
test('status with no reset schema is read-only and clearly reports execution unavailable',async t=>{
 const f=await restrictionFixture(t),before=await f.snapshot(),response=await f.request('/api/test-data/status'),data=await response.json();assert.equal(data.ok,true);assert.equal(data.disabled,true);assert.equal(data.run,null);assert.match(data.reason,/暂停/);assert.equal(await f.snapshot(),before);assert.deepEqual(await f.rows("SELECT name FROM sqlite_master WHERE name='ck_test_resets'"),[]);
});
test('without active maintenance, normal office, field and kiosk operations still succeed',async t=>{
 const f=await restrictionFixture(t);
 for(const flag of ['true','false']){const r=await f.request('/api',{body:{action:'v2_inbound_plan_create',client_req_id:crypto.randomUUID(),customer:'合成正常业务',biz_classes:['bulk']},changes:{SOP_TEST_RESET_ENABLED:flag}});assert.equal((await r.json()).ok,true);}
 const dispatched=await f.request('/001/api',{scope:'A',body:{action:'sop_task_dispatch',client_req_id:crypto.randomUUID(),need_id:f.need.id,department:'bulk',title:'合成正常派单',job_type:'bulk_op',workers:[{id:f.person.badgeId,name:f.person.name}],lead_id:f.person.badgeId,estimated_minutes:10}});assert.equal((await dispatched.json()).ok,true);
 const checked=await f.request('/attendance/api',{scope:'kiosk',body:{action:'sop_attendance_checkin',client_req_id:crypto.randomUUID(),name:'合成正常签到',agency:'가온'}});assert.equal((await checked.json()).ok,true);assert.deepEqual(await f.rows("SELECT name FROM sqlite_master WHERE name='ck_test_resets'"),[]);
});
