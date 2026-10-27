// Real local workerd D1 with synthetic records; no account or remote database.
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {latestCutoff,runSystemCutoff} from '../worker-v2/system-cutoff.js';
const source=database(),cut=Date.parse(latestCutoff(Date.now())),old=new Date(cut-3600000).toISOString(),fresh=new Date(cut+60000).toISOString();
for(const [id,start]of [['SYNTHETIC-OLD',old],['SYNTHETIC-FRESH',fresh]]){
 source.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,created_at) VALUES(?,'bulk_op','working',?)").run(id,start);
 source.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at,minutes_worked) VALUES(?,?,?,?,7)').run(id+'-CLOCK',id,id+'-PERSON',start);
 source.raw.prepare("INSERT INTO sop_records VALUES(?,'task',1,'bulk',?,?)").run(id,JSON.stringify({status:'working',workers:[{id:id+'-PERSON',name:'Synthetic'}]}),start);
}
const DB=await localD1(source.raw),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true'};
try{
 await DB.prepare("CREATE TRIGGER synthetic_failure BEFORE UPDATE ON v2_ops_jobs WHEN NEW.status='system_closed' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END").run();
 assert.ok((await Promise.allSettled(Array.from({length:8},()=>runSystemCutoff(env,cut+3600000)))).every(r=>r.status==='rejected'));
 assert.equal((await DB.prepare('SELECT count(*) n FROM ck_system_cutoff_runs').first()).n,0);assert.equal((await DB.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').first()).n,0);
 await DB.prepare('DROP TRIGGER synthetic_failure').run();
 const batch=DB.batch.bind(DB);let batches=0;
 DB.batch=async ss=>{if(ss.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_system_cutoff_runs')))batches++;return batch(ss);};
 assert.ok((await Promise.all(Array.from({length:8},()=>runSystemCutoff(env,cut+3600000)))).every(r=>r.ok));assert.equal(batches,1);assert.equal((await DB.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').first()).n,3);
 assert.equal((await DB.prepare("SELECT minutes_worked FROM v2_ops_job_workers WHERE id='SYNTHETIC-OLD-CLOCK'").first()).minutes_worked,7);
 const next=cut+86400000,other={prepare:DB.prepare.bind(DB),batch:DB.batch.bind(DB)},prepare=DB.prepare.bind(DB);let interleaved=false;
 other.prepare=sql=>{const s=prepare(sql);if(sql.startsWith('SELECT cutoff_at FROM ck_system_cutoff_runs')){const first=s.first.bind(s);s.first=async()=>{const r=await first();if(!interleaved){interleaved=true;await runSystemCutoff(env,next+3600000);}return r;};}return s;};
 assert.equal((await runSystemCutoff({...env,DB:other},next+3600000)).replayed,true);assert.equal(interleaved,true);
 assert.equal((await DB.prepare('SELECT count(*) n FROM ck_system_cutoff_runs').first()).n,2);assert.equal((await DB.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').first()).n,6);
 assert.equal((await DB.prepare("SELECT left_at FROM v2_ops_job_workers WHERE id='SYNTHETIC-OLD-CLOCK'").first()).left_at,new Date(cut).toISOString());
 assert.equal((await DB.prepare("SELECT left_at FROM v2_ops_job_workers WHERE id='SYNTHETIC-FRESH-CLOCK'").first()).left_at,new Date(next).toISOString());
 assert.equal((await DB.prepare('SELECT count(*) n FROM v2_ops_job_results').first()).n,0);assert.equal((await DB.prepare('SELECT count(*) n FROM v2_outbound_orders').first()).n,0);
 console.log('PASS local workerd D1: shared recovery, early-claim rollback and retry, competing handle after preflight, separate day markers, first boundary and audit preserved, no results/orders.');
}finally{await DB.close();source.raw.close();}
