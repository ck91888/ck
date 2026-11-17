// Local workerd D1 only. Full transaction SQL, rollback boundaries and audited correction.
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {runSystemCutoff,latestCutoff,correctSystemCutoff} from '../worker-v2/system-cutoff.js';
const DB=database(),cut=Date.parse(latestCutoff(Date.now())),start=new Date(cut-3600000).toISOString();
DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,created_at) VALUES('D1-OLD','verify_scan','working',?)").run(start);
DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at,minutes_worked) VALUES('D1-CLOCK','D1-OLD','SYNTHETIC-D1',?,7.5)").run(start);
DB.raw.prepare("INSERT INTO sop_records VALUES('D1-OLD','dispatch',1,'bulk',?,?)").run(JSON.stringify({status:'working',owner_id:'SYNTHETIC',workers:[{id:'SYNTHETIC-D1',name:'Synthetic D1'}]}),start);
const d1=await localD1(DB.raw),env={DB:d1,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true',SOP_REQUEST_USER:{id:'SYNTHETIC-MANAGER',role:'manager',scope:'office',name:'Synthetic manager'}};
try{
 await runSystemCutoff(env);assert.equal((await d1.prepare("SELECT status FROM v2_ops_jobs WHERE id='D1-OLD'").first()).status,'system_closed');
 const s=await d1.prepare('SELECT * FROM v2_ops_job_workers').first();assert.equal(s.left_at,new Date(cut).toISOString());assert.equal(s.minutes_worked,7.5);assert.equal((await d1.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').first()).n,3);
 await runSystemCutoff(env);assert.equal((await d1.prepare('SELECT count(*) n FROM sop_events').first()).n,1);
 const request={segment_id:s.id,expected_end:s.left_at,end_at:new Date(cut-1800000).toISOString(),reason:'Synthetic local verified log',client_req_id:crypto.randomUUID()};await correctSystemCutoff(request,env);await correctSystemCutoff(request,env);assert.equal((await d1.prepare('SELECT minutes_worked FROM v2_ops_job_workers').first()).minutes_worked,30);assert.equal((await d1.prepare('SELECT count(*) n FROM ck_system_cutoff_corrections').first()).n,1);
 console.log('PASS local workerd D1: cutoff transaction and exact snapshots, idempotent recovery, audited actual-end correction. No remote service.');
}finally{await d1.close();DB.raw.close();}
