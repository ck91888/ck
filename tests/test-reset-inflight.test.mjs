import test from 'node:test';import assert from 'node:assert/strict';
import entry from '../worker-v2/staging-entry.js';import {STAGING_HOST,TABLES} from '../worker-v2/test-data-reset.js';
import {operationHeaders} from './office-operator-fixture.mjs';
import {resetLifecycleFixture} from './test-reset-lifecycle-fixture.mjs';import {holdMaterialBatch} from './batch-material-reference-fixture.mjs';
const create=async(f,body)=>{const r=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{Cookie:f.cookies.office,'Content-Type':'application/json',...operationHeaders(f.cookies.office)},body:JSON.stringify({action:'v2_inbound_plan_create',client_req_id:crypto.randomUUID(),customer:'合成在途入库',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:2}],...body})}),{...f.env});return {status:r.status,body:await r.json()};};
function holdLastZero(f){
 const db=f.env.DB;let reached,release,held=false;const ready=new Promise(r=>reached=r),gate=new Promise(r=>release=r);
 const prepare=sql=>{const wrap=s=>new Proxy(s,{get(target,key){if(key==='bind')return(...args)=>wrap(target.bind(...args));if(key==='first')return async(...args)=>{const result=await target.first(...args);if(!held&&sql.startsWith('SELECT (SELECT COUNT(*) FROM')&&sql.includes('v2_idempotency_keys')){held=true;reached();await gate;}return result;};const value=target[key];return typeof value==='function'?value.bind(target):value;}});return wrap(db.prepare(sql));};
 f.env.DB={prepare,batch:db.batch.bind(db)};return {ready,release,restore(){f.env.DB=db;}};
}
const remaining=async f=>{const names=(await f.rows("SELECT name FROM sqlite_master WHERE type='table'")).map(r=>r.name),result=[];for(const table of TABLES.filter(n=>names.includes(n))){const n=(await f.query('SELECT COUNT(*) n FROM '+table).first()).n;if(n)result.push({table,n});}return result;};
for(const boundary of ['after-zero-check','after-completion'])test('an admitted inbound create cannot write '+boundary,async t=>{
 const f=await resetLifecycleFixture(t),hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INSERT INTO v2_inbound_plans'))),pending=create(f);await hold.ready;let zero,finishing,result,done;
 try{
  if(boundary==='after-zero-check')zero=holdLastZero(f);
  const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});finishing=f.complete(run);
  if(zero)await zero.ready;else done=await finishing;
  hold.release();result=await pending;
 }finally{hold.release();zero?.release();}
 done||=await finishing;zero?.restore();hold.restore();
 const residual=await remaining(f);t.diagnostic(JSON.stringify({boundary,requestOk:result.body.ok,completed:!!done.run.completedAt,residual}));
 assert.equal(result.body.ok,false,'old admission must be refused, including after the active marker closes');assert.ok(done.run.completedAt);assert.deepEqual(residual,[],'completed reset must not conceal unarchived late business records');
});
test('cold identity during maintenance cannot bootstrap cleared attendance children',async t=>{
 const f=await resetLifecycleFixture(t),{run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
 for(let i=0;i<50&&!await f.query("SELECT 1 FROM ck_test_reset_items WHERE run_id=? AND table_name='ck_attendance_spans'",run.id).first();i++)await f.okReset('step',{runId:run.id});
 assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);const db=f.env.DB,cold={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};
 const response=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{Cookie:f.cookies.office,'Content-Type':'application/json'},body:JSON.stringify({action:'sop_identity'})}),{...f.env,DB:cold}),identity=await response.json();
 assert.equal(identity.ok,true,JSON.stringify(identity));assert.equal(identity.user.role,'manager');assert.ok(identity.user.operation_context.name);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[],'identity must not recreate the first span from remaining parents');
 const done=await f.complete(run);assert.ok(done.run.completedAt);assert.deepEqual(await remaining(f),[]);
});
