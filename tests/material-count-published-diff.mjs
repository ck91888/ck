// Prove the only intended API change from the published Worker, then freeze only
// affected hashes. Requires a separate, fixed published source checkout.
import assert from 'node:assert/strict';import fs from 'node:fs';import {pathToFileURL} from 'node:url';import {mock} from 'node:test';
import worker from '../worker-v2/index.js';
import {outboundFixture,outboundCases,outboundHash} from './outbound-page-material-fixture.mjs';
assert.ok(process.env.CK_MATERIAL_NULL_BASELINE_MODULE,'Provide the published 62bcf79 Worker module');
const {default:publishedWorker}=await import(pathToFileURL(process.env.CK_MATERIAL_NULL_BASELINE_MODULE).href);
const frozen=JSON.parse(fs.readFileSync(new URL('./outbound-page-material-baseline.json',import.meta.url)));
mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
const old=await outboundFixture(undefined,{worker:publishedWorker}),current=await outboundFixture(undefined,{worker}),out={source:'62bcf79f8e1be7417e7d6a9f9774779ecf6982c3',intent:'Only SYN-JSON-NULL changes from material_count 0 to 1; material filtering and paging follow this correction.',cases:{},modes:{}};
const hash=r=>({sha256:outboundHash(r),total:r.total,returned:r.items?.length});
async function check(name,body={},scope='office',section='cases'){
 const before=await old.callTarget(scope,'v2_outbound_order_list',body);assert.equal(outboundHash(before),frozen[section][name].sha256,'original frozen response '+name);
 let expected=structuredClone(before);
 if(before.ok&&old.env.SOP_WORK_CHAIN_ENABLED==='true'&&body.scene!=='load_trip'){
  const full=await old.okTarget(scope,'v2_outbound_order_list',{...body,has_material:'legacy',limit:200,offset:0});assert.equal(full.total,full.items.length,'synthetic comparison must cover every matching order');
  const corrected=full.items.map(row=>row.id===old.oid(6)?{...row,material_count:1}:row),filter=String(body.has_material??'').trim(),rows=corrected.filter(row=>filter==='1'?row.material_count>0:filter==='0'?row.material_count===0:true);
  expected={...before,items:rows.slice(before.offset,before.offset+before.limit),total:rows.length,page_count:Math.max(1,Math.ceil(rows.length/before.limit))};
 }
 const actual=await current.callTarget(scope,'v2_outbound_order_list',body);assert.deepEqual(actual,expected,'all unaffected fields/order/metadata remain published: '+name);
 if(outboundHash(expected)!==outboundHash(before))out[section][name]=hash(expected);
}
try{
 const beforeOld=await old.snapshot(),beforeNew=await current.snapshot();
 for(const [name,body]of outboundCases)await check(name,body);
 for(const [name,modify,body]of [['legacy',e=>e.SOP_WORK_CHAIN_ENABLED='false',{}],['legacy-has-1',()=>{},{has_material:'1'}],['legacy-has-0',()=>{},{has_material:'0'}],['non-staging',e=>{e.SOP_WORK_CHAIN_ENABLED='true';e.SOP_ENVIRONMENT='production';},{}],['load-trip',e=>e.SOP_ENVIRONMENT='staging',{scene:'load_trip'}]]){modify(old.env);modify(current.env);await check(name,body,'office','modes');}
 for(const scope of ['A','guest'])await check(scope,{},scope,'modes');
 for(const f of [old,current]){
  const p=(await f.ok('office','sop_attendance_register',{name:'合成限直发派审员',agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['direct_ship'],version:0,reason:'合成出库列表权限回归'});await f.ok('A','sop_login',{badge:p.badgeId});
 }
 await check('limited-field',{},'A','modes');
 assert.equal(await old.snapshot(),beforeOld);assert.equal(await current.snapshot(),beforeNew);
 const file=new URL('./material-count-corrected-baseline.json',import.meta.url);
 if(process.env.CK_MATERIAL_NULL_WRITE_BASELINE==='true')fs.writeFileSync(file,JSON.stringify(out,null,2)+'\n');else assert.deepEqual(JSON.parse(fs.readFileSync(file)),out);
 console.log(JSON.stringify({ok:true,full_response_comparisons:30,changed_cases:Object.keys(out.cases),changed_modes:Object.keys(out.modes),original_frozen_file_unchanged:true}));
}finally{await old.close();await current.close();mock.timers.reset();}
