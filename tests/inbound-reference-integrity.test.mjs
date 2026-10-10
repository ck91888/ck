import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';

// Copy into <checkout>/tests or set CK_REVIEW_REPO to an isolated checkout.
// Optional prototype switch installs ONLY in-memory SQL guards, never online.
const repo=path.resolve(process.env.CK_REVIEW_REPO||fileURLToPath(new URL('../',import.meta.url)));
const {default:worker}=await import(pathToFileURL(path.join(repo,'worker-v2/index.js')));
const {database}=await import(pathToFileURL(path.join(repo,'tests/d1-adapter.mjs')));
const prototype=false;
async function fixture(t){
  const DB=database();t.after(()=>DB.raw.close());
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_WORK_CHAIN_ENABLED:'true'};
  const call=async(action,body={})=> (await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env)).json();
  const ok=async(action,body={})=>{const r=await call(action,body);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
  await ok('sop_identity');
  const create=(codes)=>call('v2_inbound_plan_create',{customer:'QA reference only',biz_classes:['direct_ship'],external_inbound_nos:codes,lines:[{unit_type:'carton',planned_qty:50}]});
  return{DB,env,call,ok,create};
}
function delaySecondNumberReadUntilFirstCommit(DB){
  const prepare=DB.prepare.bind(DB),batch=DB.batch.bind(DB);let reads=0,release;
  const committed=new Promise(r=>{release=r;});
  DB.prepare=statement=>{
    const q=prepare(statement);
    if(statement.startsWith('SELECT display_no FROM v2_inbound_plans WHERE plan_date=')){
      const bind=q.bind.bind(q);
      q.bind=(...values)=>{bind(...values);const first=q.first.bind(q);q.first=async(...args)=>{if(++reads===2)await committed;return first(...args);};return q;};
    }
    return q;
  };
  DB.batch=async statements=>{const result=await batch(statements);if(statements.some(s=>s.sql?.includes('INSERT INTO v2_inbound_plans')))release();return result;};
}

test('concurrent creates reserve one normalized external reference even when display numbers differ',async t=>{
  const f=await fixture(t);delaySecondNumberReadUntilFirstCommit(f.DB);
  // Both duplicate checks execute before either commit. The second number read
  // then observes the first committed RU number; no query results are fabricated.
  const replies=await Promise.all([f.create(['QA-CONCURRENT-CODE']),f.create(['QA-CONCURRENT-CODE'])]);
  assert.equal(replies.filter(r=>r.ok).length,1);
  assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_inbound_plans WHERE external_inbound_no='QA-CONCURRENT-CODE'").get().n,1);
});

test('two plans concurrently binding the same new external code have one winner',async t=>{
  const f=await fixture(t),a=await f.create(['QA-OLD-A']),b=await f.create(['QA-OLD-B']);assert.equal(a.ok,true);assert.equal(b.ok,true);
  const replies=await Promise.all([
    f.call('v2_inbound_plan_bind_external',{id:a.id,previous_code:'QA-OLD-A',external_inbound_no:'QA-NEW-SAME'}),
    f.call('v2_inbound_plan_bind_external',{id:b.id,previous_code:'QA-OLD-B',external_inbound_no:'QA-NEW-SAME'})
  ]);
  assert.equal(replies.filter(r=>r.ok).length,1);
  assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_inbound_plans WHERE external_inbound_no='QA-NEW-SAME'").get().n,1);
  const loser=replies[0].ok?b:a,old=replies[0].ok?'QA-OLD-B':'QA-OLD-A';
  assert.equal(f.DB.raw.prepare('SELECT external_inbound_no FROM v2_inbound_plans WHERE id=?').get(loser.id).external_inbound_no,old);
});

test('references containing quotes and literal backslash-n preserve exact token boundaries',async t=>{
  const f=await fixture(t),codes=['QA-QUOTE"A','QA-LITERAL\\nB'];
  const first=await f.create(codes);assert.equal(first.ok,true,first.error);
  const duplicate=await f.create(['QA-LITERAL\\nB']);assert.equal(duplicate.ok,false);
  const distinct=await f.create(['QA-LITERAL','nB']);assert.equal(distinct.ok,true,distinct.error);
  assert.equal(f.DB.raw.prepare('SELECT external_inbound_no FROM v2_inbound_plans WHERE id=?').get(first.id).external_inbound_no,codes.join('\n'));
});

test('fullwidth normalization and comma splitting keep sequential duplicate protection',async t=>{
  const f=await fixture(t),first=await f.create(['\uFF21\uFF22\uFF23-1,ABC-2']);assert.equal(first.ok,true);
  assert.equal((await f.create(['ABC-1'])).ok,false);
  assert.equal((await f.create(['ABC-2'])).ok,false);
});

test('cancelled reference is reusable but restoring the cancelled plan is atomically blocked',async t=>{
  const f=await fixture(t),a=await f.create(['QA-RESTORE']);assert.equal(a.ok,true);
  f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(a.id);
  const b=await f.create(['QA-RESTORE']);assert.equal(b.ok,true,b.error);
  assert.throws(()=>f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='pending' WHERE id=?").run(a.id));
  assert.equal(f.DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(a.id).status,'cancelled');
});

test('historical duplicate references do not block status-only completion of old plans',async t=>{
  const f=await fixture(t),a=await f.create(['QA-HISTORY-A']),b=await f.create(['QA-HISTORY-B']);assert.equal(a.ok,true);assert.equal(b.ok,true);
  // Construct history before the proposed migration, then install its guards.
  const guards=f.DB.raw.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND sql LIKE '%ck_external_reference_conflict%'").all();
  for(const guard of guards)f.DB.raw.exec('DROP TRIGGER '+JSON.stringify(guard.name));
  f.DB.raw.prepare("UPDATE v2_inbound_plans SET external_inbound_no='QA-HISTORY-A' WHERE id=?").run(b.id);
  for(const guard of guards)f.DB.raw.exec(guard.sql);
  f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='completed' WHERE id=?").run(a.id);
  assert.equal(f.DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(a.id).status,'completed');
});
