import test from 'node:test';import assert from 'node:assert/strict';
import {restrictionFixture} from './test-reset-restriction-fixture.mjs';
import {resetAction,STAGING_HOSTS} from '../worker-v2/test-data-reset.js';
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
