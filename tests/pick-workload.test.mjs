// Real Worker, real access/attendance checks, synthetic SQLite :memory: only.
import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {historicalPickStart} from './pick-workload-fixture.mjs';
const plan=()=>({order_count:23,product_quantity:137,sku_count:11,location_count:7,forklift:false});
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:'Synthetic-Pick-Only'};let cookie='',context='';
 const call=async(action,data={})=>{const r=await worker.fetch(new Request('https://pick.fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:'https://pick.fixture.invalid',Cookie:cookie,'X-CK-Operation-Context':context},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(action==='sop_login'&&r.ok)cookie=r.headers.get('set-cookie')?.split(';')[0]||'';const result=await r.json();if(result.user?.operation_context)context=result.user.operation_context.id;return result;};
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
 await ok('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await ok('sop_identity');await ok('sop_operator_confirm',{name:'合成拣货派工员'});
 const employee=async(n,checkin=true)=>{const p=(await ok('sop_attendance_employee_register',{employeeNo:'PICK-'+n,name:'合成拣货 '+n,department:'direct_ship'})).person;return {...p,record:checkin?(await ok('sop_attendance_checkin',{badge:p.badgeId})).record:null};};
 const people=[await employee('A'),await employee('B'),await employee('C')];
 const crew=ps=>ps.map(p=>({id:p.badgeId,name:p.name}));
 const request=(value=plan(),ps=people.slice(0,2),codes=['PICK-0001','PICK-0002'])=>({payload:{action:'v2_pick_job_start_by_docs',client_req_id:crypto.randomUUID(),pick_doc_nos:codes,...(value===null?{}:{planned_pick_workload:value})},workers:crew(ps),lead_id:ps[0].badgeId,estimated_minutes:30,labor_department:'direct_ship'});
 const snapshot=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','v2_ops_job_pick_docs','v2_pick_worker_docs','v2_ops_job_results','sop_records','sop_events','v2_idempotency_keys'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 const state=id=>{const r=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(id);return {...r,data:JSON.parse(r.state)};};
 return {DB,env,call,ok,employee,people,crew,request,snapshot,state};
}
test('new picking rejects absent/invalid quantities and implicit forklift without creating work',async t=>{
 const f=await fixture(t),before=f.snapshot(),invalid=[null,{},[],{...plan(),forklift:undefined},{...plan(),forklift:null},{...plan(),forklift:'false'},{...plan(),forklift:0}];
 for(const key of ['order_count','product_quantity','sku_count','location_count'])for(const value of [undefined,null,'','abc',0,-1,1.5,'12',true,Number.MAX_SAFE_INTEGER+1])invalid.push({...plan(),[key]:value});
 for(const value of invalid){const r=await f.call('sop_native_start',f.request(value));assert.equal(r.ok,false,JSON.stringify(value));assert.deepEqual(f.snapshot(),before);}
});
test('multiple documents and workers save one task plan with server-owned scope and actor; false is explicit',async t=>{
 const f=await fixture(t),request=f.request({...plan(),scope:'per_person',recorded_by:'forged',pick_doc_nos:['FORGED']}),r=await f.ok('sop_native_start',request),s=f.state(r.job_id).data.planned_pick_workload;
 assert.deepEqual(Object.fromEntries(Object.keys(plan()).map(k=>[k,s[k]])),plan());assert.equal(s.scope,'whole_task');assert.equal(s.basis,'planned_at_start');assert.equal(s.recorded_by_id,'ck-office-admin');assert.notEqual(s.recorded_by,'forged');assert.deepEqual(s.pick_doc_nos,['PICK-0001','PICK-0002']);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers').get().n,2);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_pick_worker_docs').get().n,4);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM sop_records WHERE json_type(state,'$.planned_pick_workload')='object'").get().n,1);
 const detail=await f.ok('v2_ops_job_detail',{job_id:r.job_id});assert.equal(detail.job.planned_pick_workload.product_quantity,137);assert.equal(detail.job.planned_pick_workload.forklift,false);
});
test('forklift true and maximum safe positive integer are accepted without numeric coercion',async t=>{
 const f=await fixture(t),p={order_count:Number.MAX_SAFE_INTEGER,product_quantity:Number.MAX_SAFE_INTEGER,sku_count:1,location_count:1,forklift:true},r=await f.ok('sop_native_start',f.request(p));assert.equal(f.state(r.job_id).data.planned_pick_workload.order_count,p.order_count);assert.equal(f.state(r.job_id).data.planned_pick_workload.product_quantity,p.product_quantity);assert.equal(f.state(r.job_id).data.planned_pick_workload.forklift,true);
});
for(const target of ["BEFORE INSERT ON sop_events WHEN NEW.action='sop_native_start'","BEFORE INSERT ON v2_ops_job_workers WHEN NEW.worker_id='EMP-PICK-B'","BEFORE INSERT ON v2_pick_worker_docs"]){
 test('workload and complete task graph roll back on '+target,async t=>{
  const f=await fixture(t),request=f.request(),before=f.snapshot();const actual=target.replace('EMP-PICK-B',f.people[1].badgeId);f.DB.raw.exec("CREATE TRIGGER synthetic_failure "+actual+" BEGIN SELECT RAISE(ABORT,'synthetic_failure'); END");
  assert.equal((await f.call('sop_native_start',request)).ok,false);assert.deepEqual(f.snapshot(),before);f.DB.raw.exec('DROP TRIGGER synthetic_failure');const r=await f.ok('sop_native_start',request);assert.equal(f.state(r.job_id).data.planned_pick_workload.product_quantity,137);
 });
}
test('lost reply and simultaneous identical submissions keep one task; altered plan cannot reuse request',async t=>{
 const f=await fixture(t),request=f.request();const replies=await Promise.all([f.call('sop_native_start',request),f.call('sop_native_start',request)]);assert.ok(replies.every(r=>r.ok),JSON.stringify(replies));assert.equal(replies[0].job_id,replies[1].job_id);const saved=f.snapshot();
 await f.ok('sop_native_start',request);assert.deepEqual(f.snapshot(),saved);
 assert.equal((await f.call('sop_native_start',{...request,payload:{...request.payload,planned_pick_workload:{...plan(),order_count:24}}})).ok,false);assert.deepEqual(f.snapshot(),saved);
 assert.equal((await f.call('sop_native_start',f.request(plan(),[f.people[2]]))).ok,false);assert.deepEqual(f.snapshot(),saved);
});
test('unsigned and case-changed employee badges remain blocked; daily lower-case badge remains canonical',async t=>{
 const f=await fixture(t),unsigned=await f.employee('UNSIGNED',false),before=f.snapshot();
 for(const p of [unsigned,{...f.people[0],badgeId:f.people[0].badgeId.toLowerCase()}]){assert.equal((await f.call('sop_native_start',f.request(plan(),[p]))).ok,false);assert.deepEqual(f.snapshot(),before);}
 const daily=(await f.ok('sop_attendance_checkin',{name:'合成日当拣货',agency:'가온'})).record;const r=await f.ok('sop_native_start',f.request(plan(),[{...daily,badgeId:daily.badgeId.toLowerCase()}]));assert.equal(f.DB.raw.prepare('SELECT worker_id FROM v2_ops_job_workers WHERE job_id=?').get(r.job_id).worker_id,daily.badgeId);
});
test('crew changes, personal rest, whole-task pause/resume and completion retain plan with separate results',async t=>{
 const f=await fixture(t),r=await f.ok('sop_native_start',f.request()),id=r.job_id,saved=f.state(id).data.planned_pick_workload;
 const change=ps=>f.ok('sop_native_people',{job_id:id,revision:f.state(id).revision,workers:f.crew(ps),lead_id:ps[0].badgeId});
 await change([f.people[1],f.people[2]]);assert.deepEqual(f.state(id).data.planned_pick_workload,saved);
 await f.ok('sop_attendance_break_start',{id:f.people[1].record.id});await f.ok('sop_attendance_break_end',{id:f.people[1].record.id});
 await f.ok('sop_native_pause',{job_id:id,revision:f.state(id).revision,reason:'合成暂停'});await f.ok('sop_native_resume',{job_id:id,revision:f.state(id).revision,workers:f.crew([f.people[1]]),lead_id:f.people[1].badgeId});
 await f.ok('v2_pick_job_finalize',{job_id:id,worker_id:f.people[1].badgeId,result_note:'合成结束实绩备注'});assert.deepEqual(f.state(id).data.planned_pick_workload,saved);
 const results=f.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').all(id);assert.ok(results.length);assert.ok(results.every(r=>!r.result_json.includes('planned_pick_workload')));
});
test('pending registered trip requires a plan for all trip documents; legacy continue never fills zero',async t=>{
 const f=await fixture(t),codes=['PRE-001','PRE-002'],p=f.people[0],j=await f.ok('v2_pick_job_start',{pick_doc_nos:codes,worker_id:p.badgeId});
 assert.equal((await f.call('v2_pick_job_start_by_docs',{pick_doc_nos:codes,worker_id:p.badgeId,worker_name:p.name})).ok,false);
 assert.equal((await f.call('sop_native_start',f.request(null,[p],codes))).ok,false);
 const lookup=await f.ok('v2_pick_doc_lookup',{pick_doc_no:codes[0]});assert.equal(lookup.pick_workload_required,true);assert.deepEqual([...lookup.task_pick_doc_nos].sort(),codes);
 const started=await f.ok('sop_native_start',f.request(plan(),[p],[codes[0]]));assert.equal(started.job_id,j.job_id);assert.deepEqual([...f.state(j.job_id).data.planned_pick_workload.pick_doc_nos].sort(),codes);
 await f.ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:p.badgeId});
 const old=await f.ok('v2_pick_job_start',{pick_doc_nos:['LEGACY-001'],worker_id:p.badgeId});await historicalPickStart(f.env,f.ok,{pick_doc_nos:['LEGACY-001'],worker_id:p.badgeId,worker_name:p.name});await f.ok('v2_pick_job_finish',{job_id:old.job_id,worker_id:p.badgeId});
 const oldLookup=await f.ok('v2_pick_doc_lookup',{pick_doc_no:'LEGACY-001'});assert.equal(oldLookup.pick_workload_required,false);assert.equal(oldLookup.planned_pick_workload,null);
 const continued=await f.ok('sop_native_start',f.request(null,[p],['LEGACY-001']));assert.equal(continued.job_id,old.job_id);assert.equal(f.state(old.job_id).data.planned_pick_workload,undefined);
 await f.ok('sop_native_pause',{job_id:old.job_id,revision:1,reason:'合成历史任务'});await f.ok('sop_native_resume',{job_id:old.job_id,revision:2,workers:f.crew([p]),lead_id:p.badgeId});await f.ok('v2_pick_job_finalize',{job_id:old.job_id,worker_id:p.badgeId});assert.equal((await f.ok('v2_ops_job_detail',{job_id:old.job_id})).job.planned_pick_workload,null);
});
test('historical raw scan can add a participant but raw and generic first starts cannot bypass workload',async t=>{
 const f=await fixture(t),p=f.people[0],q=f.people[1],j=await f.ok('v2_pick_job_start',{pick_doc_nos:['LEGACY-RAW'],worker_id:p.badgeId});
 for(const job_type of ['pick_direct',' pick_direct '])assert.equal((await f.call('v2_ops_job_start',{job_type,flow_stage:'order_op',worker_id:p.badgeId,worker_name:p.name})).ok,false);
 await historicalPickStart(f.env,f.ok,{pick_doc_nos:['LEGACY-RAW'],worker_id:p.badgeId,worker_name:p.name});await f.ok('v2_pick_job_start_by_docs',{pick_doc_nos:['LEGACY-RAW'],worker_id:q.badgeId,worker_name:q.name});assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=?').get(j.job_id).n,2);assert.equal((await f.ok('v2_ops_job_detail',{job_id:j.job_id})).job.planned_pick_workload,null);
});
test('each employee sees the shared task plan once, with unchanged personal counts and no attributed output',async t=>{
 const f=await fixture(t),j=await f.ok('sop_native_start',f.request());
 for(const p of f.people.slice(0,2)){const d=await f.ok('sop_attendance_employee_work_detail',{date:p.record.day,person_id:p.id});assert.equal(d.sections.own.length,1);assert.equal(d.sections.own[0].id,j.job_id);assert.equal(d.sections.own[0].planned_pick_workload.order_count,23);assert.equal(d.sections.own[0].planned_pick_workload.product_quantity,137);assert.equal(d.sections.own[0].planned_pick_workload.scope,'whole_task');assert.deepEqual(d.sections.own[0].results,[]);assert.equal(d.workday.own.participated,1);assert.equal(d.workday.own.completed,0);assert.equal(d.workday.own.product_quantity,undefined);assert.equal(d.workday.own.order_count,undefined);}
});

test('task detail and existing export expose one shared plan for multiple people and documents',async t=>{
 const f=await fixture(t),r=await f.ok('sop_native_start',f.request()),before=f.snapshot();
 for(const action of ['v2_ops_job_detail','v2_dashboard_order_detail']){
  const d=await f.ok(action,{job_id:r.job_id});assert.equal(d.job.planned_pick_workload.order_count,23);assert.equal(d.job.planned_pick_workload.product_quantity,137);
 }
 const exported=await f.ok('v2_dashboard_order_export',{job_type:'pick_direct'});assert.equal(exported.rows.length,1);const row=exported.rows[0];
 assert.equal(row.job_id,r.job_id);assert.equal(row.worker_count,2);assert.equal(row.pick_doc_nos.split('、').length,2);
 assert.deepEqual(Object.fromEntries(Object.keys(plan()).map(k=>[k,row.planned_pick_workload[k]])),plan());
 assert.equal(row.planned_pick_workload.scope,'whole_task');assert.equal(row.order_count,undefined);assert.equal(row.box_count_sum,0);assert.deepEqual(f.snapshot(),before);
});
test('historical four-field plans keep their quantities, continue and complete without an order count or backfill',async t=>{
 const f=await fixture(t),r=await f.ok('sop_native_start',f.request()),id=r.job_id,p=f.people[0];
 // Synthetic pre-release state only; never touches a remote database.
 const state=f.state(id).data;delete state.planned_pick_workload.order_count;const saved=state.planned_pick_workload;
 f.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(state),id);const before=f.snapshot();
 for(const action of ['v2_ops_job_detail','v2_dashboard_order_detail']){const d=await f.ok(action,{job_id:id});assert.equal(d.job.planned_pick_workload.order_count,undefined);assert.equal(d.job.planned_pick_workload.product_quantity,137);}
 const row=(await f.ok('v2_dashboard_order_export',{job_type:'pick_direct'})).rows[0];assert.equal(row.planned_pick_workload.order_count,undefined);assert.equal(row.planned_pick_workload.forklift,false);assert.deepEqual(f.snapshot(),before);
 await f.ok('sop_native_people',{job_id:id,revision:f.state(id).revision,workers:[],lead_id:'',reason:'合成交接离岗'});
 await f.ok('sop_native_people',{job_id:id,revision:f.state(id).revision,workers:f.crew([p]),lead_id:p.badgeId});
 await f.ok('sop_native_pause',{job_id:id,revision:f.state(id).revision,reason:'历史合成暂停'});
 await f.ok('sop_native_resume',{job_id:id,revision:f.state(id).revision,workers:f.crew([p]),lead_id:p.badgeId});
 await f.ok('v2_pick_job_finalize',{job_id:id,worker_id:p.badgeId});assert.deepEqual(f.state(id).data.planned_pick_workload,saved);
 assert.equal((await f.ok('v2_dashboard_order_export',{job_type:'pick_direct'})).rows[0].planned_pick_workload.order_count,undefined);
});
