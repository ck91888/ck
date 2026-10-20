import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

// Every request runs against an isolated in-memory database and synthetic data.
const C1='301000009101',C2='301000009102',C3='301000009103';
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data={})=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 const createBody=(codes=[C1],extra={})=>({customer:'QA courier cancellation race',biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:codes,planned_qty:codes.length}],...extra});
 const create=(codes,extra)=>ok('v2_inbound_plan_create',createBody(codes,extra));
 const cancel=(id,extra={})=>call('v2_inbound_plan_cancel',{inbound_plan_id:id,reason:'QA cancellation race',...extra});
 const plan=id=>DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(id);
 const jobs=id=>DB.raw.prepare('SELECT id,job_type,status FROM v2_inbound_plan_jobs WHERE plan_id=? ORDER BY id').all(id);
 const activeWorkers=()=>DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n;
 const snapshot=()=>Object.fromEntries(['v2_inbound_plans','v2_inbound_plan_lines','v2_inbound_plan_biz_tasks','ck_courier_plan_items','ck_courier_plan_item_history','ck_inbound_document_versions','v2_outbound_orders'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 const native=(payload,key=crypto.randomUUID())=>call('sop_native_start',{payload:{client_req_id:key,...payload},workers:[{id:'QA-CANCEL-WORKER',name:'QA synthetic worker'}],lead_id:'QA-CANCEL-WORKER',estimated_minutes:10,labor_department:'bulk'});
 const generic=id=>native({action:'v2_ops_job_start',flow_stage:'internal',job_type:'inventory',inventory_type:'precise',internal_metrics:{inventory_count:0,location_count:0,used_forklift:false},related_doc_type:'inbound_plan',related_doc_id:id});
 await ok('sop_identity');
 return {DB,env,call,ok,createBody,create,cancel,plan,jobs,activeWorkers,snapshot,native,generic};
}

// Inject a completed API request immediately before a real write executes.
// Both standalone writes and D1 batches are supported; query results are unchanged.
function beforeWrite(DB,matches,interleave){
 const prepare=DB.prepare.bind(DB),batch=DB.batch.bind(DB);let injected=false;
 const inject=async()=>{if(injected)return;injected=true;DB.prepare=prepare;DB.batch=batch;await interleave();};
 DB.prepare=sql=>{const statement=prepare(sql);for(const method of ['run','all','first']){const execute=statement[method].bind(statement);statement[method]=async(...args)=>{if(matches(statement))await inject();return execute(...args);};}return statement;};
 DB.batch=async statements=>{if(statements.some(matches))await inject();return batch(statements);};
 return ()=>assert.equal(injected,true,'the competing API request must commit after preflight and before the targeted write');
}
const cancelWrite=s=>/UPDATE\s+v2_inbound_plans\s+SET\s+status='cancelled'/i.test(s.sql);
const jobWrite=s=>/INSERT\s+INTO\s+v2_ops_jobs\b/i.test(s.sql);
const createWrite=s=>/INSERT\s+INTO\s+v2_inbound_plans\b/i.test(s.sql);

async function startCase(f,mode){
 const first=await f.create([C1]),second=mode==='native multi-plan unload'?await f.create([C2]):null;
 // Cancel the second linked plan, which is not v2_ops_jobs.related_doc_id.
 const target=second||first;
 const start=()=>mode==='legacy unload'
  ?f.call('v2_unload_job_start',{plan_id:first.id,worker_id:'QA-CANCEL-WORKER',worker_name:'QA synthetic worker'})
  :mode==='native generic'?f.generic(first.id)
  :f.native({action:'v2_unload_job_start',plan_ids:[first.id,second.id]});
 return {first,second,target,start};
}

for(const mode of ['legacy unload','native generic','native multi-plan unload']){
 test(mode+': cancellation committed after start preflight prevents all job and clock writes',async t=>{
  const f=await fixture(t),x=await startCase(f,mode);let cancelled;
  const injected=beforeWrite(f.DB,jobWrite,async()=>{cancelled=await f.cancel(x.target.id);assert.equal(cancelled.ok,true,cancelled.error);});
  const start=await x.start();injected();assert.equal(start.ok,false,'a stale start cannot create work for a cancelled courier plan');
  assert.equal(f.plan(x.target.id).status,'cancelled');assert.deepEqual(f.jobs(x.target.id),[]);assert.equal(f.activeWorkers(),0);
  assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);
  assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='dispatch'").get().n,0);
  assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_unload_trips').get().n,0);
  assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_unload_plan_links').get().n,0);
  assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_item_history').get().n,0);
  if(x.second)assert.equal(f.plan(x.first.id).status,'pending','the other plan in the failed trip must remain untouched');
 });

 test(mode+': a job committed after cancellation preflight makes cancellation fail without changing the plan',async t=>{
  const f=await fixture(t),x=await startCase(f,mode);let started,afterStart;
  const injected=beforeWrite(f.DB,cancelWrite,async()=>{started=await x.start();assert.equal(started.ok,true,started.error);afterStart=f.plan(x.target.id);});
  const cancelled=await f.cancel(x.target.id);injected();assert.equal(cancelled.ok,false,'cancellation must recheck the committed job at its write');
  assert.deepEqual(f.plan(x.target.id),afterStart);assert.equal(f.jobs(x.target.id)[0].status,'working');assert.equal(f.activeWorkers(),1);
  assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_item_history').get().n,0);
  const duplicate=await f.call('v2_inbound_plan_create',f.createBody([x.second?C2:C1]));assert.equal(duplicate.ok,false);
 });
}

test('a paused generic task retains the courier claim even with no open worker clocks',async t=>{
 const f=await fixture(t),p=await f.create(),j=await f.generic(p.id);assert.equal(j.ok,true,j.error);
 await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'QA pause preserves plan'});
 assert.equal(f.plan(p.id).status,'pending');assert.equal(f.activeWorkers(),0);assert.equal(f.jobs(p.id)[0].status,'paused');
 const before=f.snapshot();assert.equal((await f.cancel(p.id)).ok,false);assert.deepEqual(f.snapshot(),before);
 assert.equal((await f.call('v2_inbound_plan_create',f.createBody())).ok,false);
});

test('a task started and paused after cancellation preflight still blocks the cancellation commit',async t=>{
 const f=await fixture(t),p=await f.create();let afterPause;
 const injected=beforeWrite(f.DB,cancelWrite,async()=>{const j=await f.generic(p.id);assert.equal(j.ok,true,j.error);await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'QA interleaved pause'});afterPause=f.plan(p.id);});
 const cancelled=await f.cancel(p.id);injected();assert.equal(cancelled.ok,false);assert.deepEqual(f.plan(p.id),afterPause);
 assert.equal(f.jobs(p.id)[0].status,'paused');assert.equal(f.activeWorkers(),0);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_item_history').get().n,0);
});

for(const concurrent of [false,true])test((concurrent?'concurrent':'replayed')+' cancellation appends its reason only once and preserves history',async t=>{
 const f=await fixture(t),p=await f.create([C1],{remark:'QA original instruction'}),payload={client_req_id:'QA-CANCEL-RETRY',reason:'QA only one cancellation reason'};
 const replies=concurrent?await Promise.all([f.cancel(p.id,payload),f.cancel(p.id,payload)]):[await f.cancel(p.id,payload),await f.cancel(p.id,payload)];
 assert.ok(replies.every(r=>r.ok),JSON.stringify(replies));
 const after=f.plan(p.id);assert.equal(after.status,'cancelled');assert.equal(after.remark,'QA original instruction | 取消原因: '+payload.reason);
 const retry=await f.cancel(p.id,{reason:'QA later retry must not replace original reason'});assert.equal(retry.ok,true,retry.error);assert.deepEqual(f.plan(p.id),after);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plan_lines WHERE plan_id=?').get(p.id).n,1);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_items WHERE plan_id=?').get(p.id).n,1);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_item_history').get().n,0);
});

test('a legacy cancelled courier row is reusable on the first create without an intervening detail or list read',async t=>{
 const f=await fixture(t),old=await f.create([C1,C2]);
 // Model a cancellation saved by the old release: no claim archive was written.
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled',remark='QA legacy cancellation' WHERE id=?").run(old.id);
 const oldRow=f.plan(old.id),oldLines=f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').all(old.id);
 const replacement=await f.create([C1,C2]);assert.notEqual(replacement.id,old.id);assert.deepEqual(f.plan(old.id),oldRow);
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=?').all(old.id),oldLines);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_items WHERE plan_id=?').get(replacement.id).n,2);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_items WHERE plan_id=?').get(old.id).n,0);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_plan_item_history WHERE plan_id=? AND replacement_plan_id=?').get(old.id,replacement.id).n,2);
});

test('two concurrent creates with the same cancelled waybill leave one complete replacement and one archive',async t=>{
 const f=await fixture(t),old=await f.create();assert.equal((await f.cancel(old.id)).ok,true);
 // Both duplicate checks observe the same pre-commit state. Display numbering is
 // also raced; the separate multi-waybill case below isolates the claim conflict.
 const prepare=f.DB.prepare.bind(f.DB);let reads=0,release;const bothRead=new Promise(resolve=>{release=resolve;});
 f.DB.prepare=sql=>{const statement=prepare(sql);if(sql.includes('SELECT p.display_no')&&sql.includes('ck_courier_plan_items')){const first=statement.first.bind(statement);statement.first=async()=>{const result=await first();if(++reads===2)release();await bothRead;return result;};}return statement;};
 const replies=await Promise.all([f.call('v2_inbound_plan_create',f.createBody([C1],{customer:'QA concurrent A'})),f.call('v2_inbound_plan_create',f.createBody([C1],{customer:'QA concurrent B'}))]);
 assert.equal(reads,2);assert.equal(replies.filter(r=>r.ok).length,1,JSON.stringify(replies));
 const winner=replies.find(r=>r.ok);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,2);
 assert.equal(f.DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(C1).plan_id,winner.id);
 const history=f.DB.raw.prepare('SELECT * FROM ck_courier_plan_item_history WHERE tracking_no=?').all(C1);assert.equal(history.length,1);assert.equal(history[0].plan_id,old.id);assert.equal(history[0].replacement_plan_id,winner.id);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plan_lines').get().n,2);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plan_biz_tasks').get().n,2);
});

test('a later waybill conflict rolls back the earlier archive, release, new plan and all dependent rows',async t=>{
 const f=await fixture(t),old=await f.create([C1]),other=await f.create([C3]);assert.equal((await f.cancel(old.id)).ok,true);let afterWinner;
 const injected=beforeWrite(f.DB,createWrite,async()=>{
  // Existing plan edits have independent display numbers. This wins C2 after
  // the candidate passed preflight, so its failure must be the waybill claim.
  await f.ok('v2_inbound_plan_update',{id:other.id,...f.createBody([C2])});afterWinner=f.snapshot();
 });
 const failed=await f.call('v2_inbound_plan_create',f.createBody([C1,C2],{customer:'QA losing multi-waybill create'}));
 injected();assert.equal(failed.ok,false);assert.match(failed.error||failed.message,/tracking_no|快递|单号|关联|송장/);
 assert.deepEqual(f.snapshot(),afterWinner,'the failed create must roll back the first waybill archive and every candidate row');
 assert.equal(f.DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(C1).plan_id,old.id);
 assert.equal(f.DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(C2).plan_id,other.id);
});
