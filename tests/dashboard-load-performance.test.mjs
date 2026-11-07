import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {pathToFileURL} from 'node:url';
import {database} from './d1-adapter.mjs';
const workerRoot=process.env.CK_PERF_REPO?pathToFileURL(process.env.CK_PERF_REPO+'/'):new URL('../',import.meta.url);
const {default:worker}=await import(new URL('worker-v2/index.js',workerRoot));
const delayMs=Number(process.env.CK_PERF_DB_DELAY_MS||0),measurements=[];

for(const size of [0,1,100,105])test('live dashboard preserves people and ordering at '+size+' jobs',async t=>{
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async action=>{const r=await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action})}),env);const body=await r.json();assert.equal(body.ok,true,body.error);return body;};
 await call('sop_identity');
 const job=DB.raw.prepare('INSERT INTO v2_ops_jobs(id,flow_stage,job_type,status,active_worker_count,created_at) VALUES(?,?,?,?,?,?)');
 const person=DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at) VALUES(?,?,?,?,?,?)');
 for(let i=0;i<size;i++){
  const id='QA-LOAD-'+String(i).padStart(3,'0');job.run(id,'unload','unload',i%2?'awaiting_close':'working',3,'2026-01-01T00:00:'+String(i%60).padStart(2,'0')+'Z');
  for(const [n,name] of ['Synthetic B','Synthetic A','Synthetic B','',null,'Departed'].entries())person.run(id+'-'+n,id,'QA-W-'+i+'-'+n,name,'2026-01-01T00:00:00Z',n===5?'2026-01-01T01:00:00Z':'');
 }
 job.run('QA-COMPLETED','unload','unload','completed',3,'2025-01-01T00:00:00Z');
 const prepare=DB.prepare.bind(DB),batch=DB.batch.bind(DB),calls=[];
 const record=async statements=>{calls.push(statements);if(delayMs)await new Promise(r=>setTimeout(r,delayMs));};
 DB.prepare=sql=>{const s=prepare(sql);for(const method of ['first','all','run']){const run=s[method].bind(s);s[method]=async()=>{await record([sql]);return run();};}return s;};
 DB.batch=async statements=>{await record(statements.map(s=>s.sql));return batch(statements);};
 const started=performance.now(),result=await call('v2_dashboard_live_docs'),elapsed=performance.now()-started;
 assert.equal(result.docs.length,Math.min(size,100));
 assert.ok(result.docs.every(d=>d.job_id!=='QA-COMPLETED'));
 for(const d of result.docs){
  const names=DB.raw.prepare("SELECT DISTINCT worker_name FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").all(d.job_id).map(r=>r.worker_name).filter(Boolean).join(', ');
  assert.equal(d.worker_names,names,'same DISTINCT, empty-name filter, order and departed-person exclusion');
  assert.equal(d.active_worker_count,3);
 }
 const dbCalls=calls.length,nameCalls=calls.filter(c=>c.some(sql=>sql.includes('SELECT DISTINCT worker_name')));
 if(!process.env.CK_PERF_REPO)assert.equal(nameCalls.length,size?1:0,'bounded crew reads make one remote call, not one per job');
 assert.ok(calls.flat().every(sql=>!/^\s*(UPDATE|INSERT|DELETE|CREATE|ALTER)/i.test(sql)),'warm dashboard must be read only');
 const first=result.docs[0];if(first){DB.raw.prepare("UPDATE v2_ops_job_workers SET worker_name='Changed now' WHERE job_id=? AND worker_name='Synthetic A'").run(first.job_id);assert.match((await call('v2_dashboard_live_docs')).docs.find(d=>d.job_id===first.job_id).worker_names,/Changed now/,'next read sees current crew; no response cache');}
 const measurement={jobs:size,returned:result.docs.length,dbCalls,crewCalls:nameCalls.length,crewSqlStatements:nameCalls.flat().length,elapsedMs:Math.round(elapsed),responseBytes:Buffer.byteLength(JSON.stringify(result))};
 measurements.push(measurement);console.log(JSON.stringify(measurement));
 if(process.env.CK_PERF_REPORT)fs.writeFileSync(process.env.CK_PERF_REPORT,JSON.stringify(measurements,null,2));
});
