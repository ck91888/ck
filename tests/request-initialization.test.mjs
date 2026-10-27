import test from 'node:test';
import assert from 'node:assert/strict';
import {ensureSchema} from '../worker-v2/schema-ready.js';
import {ensureCutoffSchema,runSystemCutoff,latestCutoff} from '../worker-v2/system-cutoff.js';
import {database} from './d1-adapter.mjs';
import worker from '../worker-v2/index.js';
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
test('concurrent initialization shares only the same database and version, then retains success without retaining work',async()=>{
 const a={},b={},gate=deferred();let calls=0;
 const init=async()=>{calls++;await gate.promise;};
 const requests=Array.from({length:8},()=>ensureSchema(a,'v1',init));
 requests.push(ensureSchema(a,'v2',init),ensureSchema(b,'v1',init));await Promise.resolve();assert.equal(calls,3);
 gate.resolve();await Promise.all(requests);await ensureSchema(a,'v1',init);assert.equal(calls,3);
});
test('a rejected shared initialization rejects every waiter and can be retried once',async()=>{
 const db={},gate=deferred(),error=Error('synthetic failure');let calls=0;
 const init=async()=>{calls++;await gate.promise;throw error;};
 const requests=Array.from({length:8},()=>ensureSchema(db,'retry',init));await Promise.resolve();assert.equal(calls,1);gate.resolve();
 assert.ok((await Promise.allSettled(requests)).every(r=>r.status==='rejected'&&r.reason===error));
 await ensureSchema(db,'retry',async()=>{calls++;});await ensureSchema(db,'retry',async()=>{calls++;});assert.equal(calls,2);
});
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const cut=Date.parse(latestCutoff(Date.now())),old=new Date(cut-3600000).toISOString(),fresh=new Date(cut+60000).toISOString();
 for(const [id,start]of [['OLD',old],['FRESH',fresh]]){
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,created_at) VALUES(?,'bulk_op','working',?)").run(id,start);
  DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,joined_at,minutes_worked) VALUES(?,?,?,?,7)').run(id+'-CLOCK',id,'SYNTHETIC-'+id,start);
  DB.raw.prepare("INSERT INTO sop_records VALUES(?,'task',1,'bulk',?,?)").run(id,JSON.stringify({status:'working',workers:[{id:'SYNTHETIC-'+id,name:'Synthetic'}]}),start);
 }
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true'};await ensureCutoffSchema(env);
 return {DB,env,cut,run:()=>runSystemCutoff(env,cut+3600000),clock:id=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE id=?').get(id+'-CLOCK')};
}
test('eight simultaneous recovery callers execute one complete audit transaction; warm calls only read the marker',async t=>{
 const f=await fixture(t),batch=f.DB.batch.bind(f.DB),gate=deferred(),seen=deferred();let batches=0;
 f.DB.batch=async ss=>{if(ss.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_system_cutoff_runs'))){batches++;seen.resolve();await gate.promise;}return batch(ss);};
 const requests=Array.from({length:8},f.run);await seen.promise;assert.equal(batches,1);gate.resolve();const replies=await Promise.all(requests);assert.ok(replies.every(r=>r.ok));
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_runs').get().n,1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').get().n,3);assert.equal(f.clock('OLD').minutes_worked,7);assert.equal(f.clock('FRESH').left_at,'');
 assert.equal((await f.run()).replayed,true);assert.equal(batches,1);
});
test('failed cutoff rolls back its early claim and audit, releases all waiters and retries safely',async t=>{
 const f=await fixture(t);f.DB.raw.exec("CREATE TRIGGER synthetic_cutoff_failure BEFORE UPDATE ON v2_ops_jobs WHEN NEW.status='system_closed' BEGIN SELECT RAISE(ABORT,'synthetic cutoff failure'); END");
 const replies=await Promise.allSettled(Array.from({length:8},f.run));assert.ok(replies.every(r=>r.status==='rejected'));
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_runs').get().n,0);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').get().n,0);assert.equal(f.clock('OLD').left_at,'');
 f.DB.raw.exec('DROP TRIGGER synthetic_cutoff_failure');await f.run();assert.equal(f.clock('OLD').left_at,new Date(f.cut).toISOString());assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').get().n,3);
});
test('a competing database handle that wins after preflight owns the transaction; the loser does not evaluate row scans',async t=>{
 const f=await fixture(t),other={prepare:f.DB.prepare.bind(f.DB),batch:f.DB.batch.bind(f.DB)},prepare=f.DB.prepare.bind(f.DB);let injected=false,evaluations=0;
 f.DB.prepare=sql=>{const s=prepare(sql);if(sql.startsWith('SELECT cutoff_at FROM ck_system_cutoff_runs')){const first=s.first.bind(s);s.first=async()=>{const result=await first();if(!injected){injected=true;await runSystemCutoff({...f.env,DB:other},f.cut+3600000);f.DB.raw.function('julianday',()=>{evaluations++;return 0;});}return result;};}return s;};
 const reply=await f.run();assert.equal(reply.replayed,true);assert.equal(injected,true);assert.equal(evaluations,0,'a losing claim must skip row predicates, not just avoid duplicate writes');
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_runs').get().n,1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').get().n,3);assert.equal(f.clock('FRESH').left_at,'');
});
test('distinct boundaries are not coalesced or cached; first elapsed cutoff and the next day both execute',async t=>{
 const f=await fixture(t),next=f.cut+86400000;
 await Promise.all([f.run(),runSystemCutoff(f.env,next+3600000)]);
 assert.equal(f.clock('OLD').left_at,new Date(f.cut).toISOString());assert.equal(f.clock('FRESH').left_at,new Date(next).toISOString());assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_runs').get().n,2);
 assert.equal((await runSystemCutoff(f.env,next+3600000)).replayed,true);
});
test('an ambiguous committed response is recovered from the persisted marker without repeating audit or updates',async t=>{
 const f=await fixture(t),batch=f.DB.batch.bind(f.DB);let once=true;
 f.DB.batch=async ss=>{const result=await batch(ss);if(once&&ss.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_system_cutoff_runs'))){once=false;throw Error('synthetic lost response');}return result;};
 await assert.rejects(f.run(),/lost response/);assert.equal((await f.run()).replayed,true);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_system_cutoff_audit').get().n,3);assert.equal(f.clock('OLD').minutes_worked,7);
});

test('eight cold Worker reads initialize each shared schema and recover the boundary once; warm reads write nothing',async t=>{
 const DB=database();t.after(()=>DB.raw.close());DB.raw.exec("CREATE TABLE v2_schema_meta(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT);INSERT INTO v2_schema_meta VALUES('schema_version','v2.20261021-inbound-owner','synthetic')");
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'},batch=DB.batch.bind(DB);let statements=[];
 DB.batch=async ss=>{statements.push(...ss.map(s=>s.sql));return batch(ss);};
 const call=async()=>{const response=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_identity'})}),env);assert.equal((await response.json()).ok,true);};
 await Promise.all(Array.from({length:8},call));
 assert.equal(statements.filter(s=>s.startsWith('CREATE TABLE IF NOT EXISTS ck_courier_plan_item_history')).length,1);assert.equal(statements.filter(s=>s.startsWith('INSERT OR IGNORE INTO ck_inbound_document_versions(plan_id) SELECT')).length,1);assert.equal(statements.filter(s=>s.startsWith('INSERT OR IGNORE INTO ck_system_cutoff_runs')).length,1);
 statements=[];await Promise.all(Array.from({length:8},call));assert.deepEqual(statements,[],'warm reads must not repeat schema work or cutoff writes');
});
