import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import entry from '../worker-v2/staging-entry.js';
import {digest} from '../worker-v2/access-control.js';
import {ensureSchema} from '../worker-v2/schema-ready.js';
import {STAGING_HOST,STAGING_DATABASE} from '../worker-v2/test-data-reset.js';

test('schema readiness is scoped to DB and version, retries failures, and retains no business data',async()=>{
 const a={},b={};let count=0;
 const init=async()=>{count++;};
 await ensureSchema(a,'v1',init);await ensureSchema(a,'v1',init);assert.equal(count,1);
 await ensureSchema(b,'v1',init);await ensureSchema(a,'v2',init);assert.equal(count,3);
 await assert.rejects(ensureSchema(a,'retry',async()=>{throw Error('unavailable');}));
 await ensureSchema(a,'retry',init);assert.equal(count,4);
});

test('warm requests avoid DDL; detail query count is fixed as history grows and preserves latest output',async()=>{
 const DB=database(),prepare=DB.prepare.bind(DB),batch=DB.batch.bind(DB);let calls=[];
 DB.prepare=sql=>{const s=prepare(sql);for(const method of ['first','all','run']){const run=s[method].bind(s);s[method]=async()=>{calls.push(sql);return run();};}return s;};
 DB.batch=async items=>{calls.push(items.map(x=>x.sql).join('\n'));return batch(items);};
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('fixture-only'),SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE};
 let cookie='';
 async function api(action,body={}){
  const r=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
  if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];
  const out=await r.json();assert.equal(out.ok,true,out.error);return out;
 }
 await api('sop_login',{sop_key:'fixture-only'});await api('sop_attendance_config');
 const p=await api('v2_inbound_plan_create',{customer:'Fixture',biz_classes:['direct_ship'],external_inbound_nos:['FIXTURE-A','FIXTURE-B'],lines:[{unit_type:'carton',planned_qty:20}]});
 calls=[];await api('sop_identity');assert.equal(calls.length,2);assert.ok(calls.every(s=>!s.includes('CREATE')));
 calls=[];await api('v2_inbound_plan_detail',{id:p.id});const emptyCount=calls.length;assert.ok(emptyCount<=7);
 for(let i=0;i<12;i++){
  const job='FIXTURE-JOB-'+i;
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,related_doc_type,related_doc_id,job_type,status,inbound_external_no,created_at,updated_at) VALUES(?,'inbound_plan',?,'inbound_direct','completed',?,'2026-01-01T01:00:00Z','2026-01-01T02:00:00Z')").run(job,p.id,i===0?'FIXTURE-A':'');
  for(let segment=0;segment<2;segment++)DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,minutes_worked,joined_at,left_at) VALUES(?,?,'FIXTURE-W','Fixture worker',5,'2026-01-01T01:00:00Z','2026-01-01T02:00:00Z')").run(job+'-'+segment,job);
  for(let revision=0;revision<2;revision++)DB.raw.prepare('INSERT INTO v2_ops_job_results(id,job_id,result_lines_json,remark,created_at) VALUES(?,?,?,?,?)').run(job+'-r'+revision,job,JSON.stringify([{unit_type:'carton',putaway_qty:revision+1}]),'revision '+revision,'2026-01-01T0'+(revision+2)+':00:00Z');
 }
 calls=[];const d=await api('v2_inbound_plan_detail',{id:p.id});assert.equal(calls.length,emptyCount,'no per-job database requests');assert.equal(d.jobs.length,12);
 for(const job of d.jobs){assert.deepEqual(job.worker_names,['Fixture worker']);assert.equal(job.total_minutes_worked,10);assert.equal(job.remark,'revision 1');assert.equal(job.result_lines[0].putaway_qty,2);}
 assert.equal(d.plan.inbound_progress.total,2);assert.equal(d.plan.inbound_progress.completed,1);assert.equal(d.plan.inbound_progress.items[1].status,'pending');
 // Legacy tasks still initialize once and subsequent reads see current DB state.
 DB.raw.prepare('DELETE FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').run(p.id);
 assert.equal((await api('v2_inbound_plan_detail',{id:p.id})).biz_tasks.length,1);
 DB.raw.prepare("UPDATE v2_inbound_plan_biz_tasks SET status='completed' WHERE plan_id=?").run(p.id);
 assert.deepEqual((await api('v2_inbound_plan_detail',{id:p.id})).pending_biz_classes,[]);
 DB.raw.prepare('DELETE FROM ck_access_sessions').run();
 const r=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body:JSON.stringify({action:'sop_identity'})}),env);
 assert.equal(r.status,401,'schema optimization must never cache a revoked session');
});
