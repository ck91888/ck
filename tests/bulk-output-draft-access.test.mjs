// Authenticated Worker requests and synthetic in-memory data. No online writes.
import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {bulkOutputDraft} from '../worker-v2/bulk-output-draft.js';
import {database} from './d1-adapter.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

async function fixture(t,kind,{member=false}={}){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_DISPATCH_SHARE_ENABLED:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};
 const cookies={office:'',field:''};
 async function call(scope,action,body={}){
  const r=await worker.fetch(new Request('https://draft-access.fixture.invalid'+(scope==='field'?'/001/api':'/api'),{
   method:'POST',headers:{'Content-Type':'application/json',Cookie:cookies[scope],...operationHeaders(cookies[scope])},
   body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})
  }),env);
  if(r.headers.has('set-cookie'))cookies[scope]=r.headers.get('set-cookie').split(';')[0];
  return rememberOperation(await r.json(),cookies[scope]);
 }
 const ok=async(scope,action,body)=>{const r=await call(scope,action,body);assert.equal(r.ok,true,action+': '+r.error);return r;};
 await ok('office','sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});
 await confirmOperation((a,b)=>call('office',a,b),'合成草稿权限管理员');
 const people=[];
 for(const name of ['合成草稿所有者','合成草稿协作者','合成草稿操作员']){
  const p=(await ok('office','sop_attendance_register',{name,agency:'가온'})).person;
  await ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});
  await ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,version:0,departments:['bulk'],reason:'合成草稿权限测试'});
  people.push(p);
 }
 const [owner,partner,person]=people,login=p=>ok('field','sop_login',{badge:p.badgeId});
 await login(owner);const pair=(await ok('field','sop_dispatch_share_invite',{target_badge:partner.badgeId})).pair;
 await login(partner);await ok('field','sop_dispatch_share_accept',{pair_id:pair.id});
 const crew=[person,...(member?[partner]:[])].map(p=>({id:p.badgeId,name:p.name}));
 await login(owner);let id;
 if(kind==='dispatch'){
  id=(await ok('field','sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:'SYNTH-DRAFT-ACCESS-'+crypto.randomUUID(),customer:'合成权限客户',client_req_id:crypto.randomUUID()},workers:crew,lead_id:person.badgeId,estimated_minutes:30,labor_department:'bulk'})).job_id;
 }else{
  const need=await ok('office','sop_need_create',{source_type:'inventory',department:'bulk',supply_chain_no:'SYNTH-DRAFT-ACCESS-'+crypto.randomUUID(),customer:'合成权限客户',title:'合成权限任务',instructions:'合成数据'});
  id=(await ok('field','sop_task_dispatch',{need_id:need.id,title:'合成权限任务',department:'bulk',job_type:'bulk_op',workers:crew,lead_id:person.badgeId,estimated_minutes:30})).id;
 }
 await login(partner);
 const endPair=()=>DB.raw.prepare("UPDATE ck_dispatch_share_pairs SET status='ended',ended_at=?,ended_by='synthetic-race' WHERE id=?").run(new Date().toISOString(),pair.id);
 const snapshot=()=>Object.fromEntries(['sop_records','sop_events','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 const read=(scope='field')=>ok(scope,'sop_bulk_output_draft_read',{id});
 const body=r=>({id,revision:r.revision,version:r.version,scope_version:r.scope_version,values:kind==='dispatch'?{packed_count:'42'}:{quantity:'42',unit:'箱'}});
 return {DB,env,call,ok,id,pair,owner,partner,login,endPair,snapshot,read,body};
}

function leaveAfterFirstAuthorization(f){
 const prepare=f.DB.prepare.bind(f.DB);let ended=false;
 f.DB.prepare=sql=>{
  const statement=prepare(sql);
  if(!String(sql).includes("FROM ck_dispatch_share_pairs WHERE status='active'"))return statement;
  const all=statement.all.bind(statement);
  statement.all=async(...args)=>{
   const result=await all(...args);
   // Return the successful authorization snapshot, then commit the other user's leave.
   if(!ended&&result.results.length){ended=true;f.endPair();}
   return result;
  };
  return statement;
 };
 return {restore(){f.DB.prepare=prepare;},get ended(){return ended;}};
}

for(const kind of ['task','dispatch']){
 test(kind+': leaving immediately after the first successful shared authorization rejects the entire draft save',async t=>{
  const f=await fixture(t,kind),r=await f.read(),before=f.snapshot(),race=leaveAfterFirstAuthorization(f);
  let result;try{result=await f.call('field','sop_bulk_output_draft_save',f.body(r));}finally{race.restore();}
  assert.equal(race.ended,true);assert.equal(f.DB.raw.prepare('SELECT status FROM ck_dispatch_share_pairs WHERE id=?').get(f.pair.id).status,'ended');
  assert.equal(result.ok,false,JSON.stringify(result));assert.deepEqual(f.snapshot(),before);
  assert.equal((await f.call('field','sop_bulk_output_draft_read',{id:f.id})).ok,false);
 });
 test(kind+': leaving immediately before the transaction also rejects the entire draft save',async t=>{
  const f=await fixture(t,kind),r=await f.read(),before=f.snapshot(),batch=f.DB.batch.bind(f.DB);let ended=false;
  f.DB.batch=async statements=>{
   if(!ended&&statements.some(s=>s.sql==='INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)'&&s.args[3]==='sop_bulk_output_draft_save')){ended=true;f.endPair();}
   return batch(statements);
  };
  let result;try{result=await f.call('field','sop_bulk_output_draft_save',f.body(r));}finally{f.DB.batch=batch;}
  assert.equal(ended,true);assert.equal(result.ok,false,JSON.stringify(result));assert.deepEqual(f.snapshot(),before);
 });
 test(kind+': ending collaboration preserves owner and office-manager draft authority',async t=>{
  const f=await fixture(t,kind);f.endPair();
  for(const scope of ['field','office']){
   if(scope==='field')await f.login(f.owner);
   const r=await f.read(scope),saved=await f.ok(scope,'sop_bulk_output_draft_save',f.body(r));
   assert.equal(saved.version,r.version+1);
  }
 });
}

test('a native live crew member retains the existing independent authority after collaboration ends',async t=>{
 const f=await fixture(t,'dispatch',{member:true});f.endPair();
 const r=await f.read(),saved=await f.ok('field','sop_bulk_output_draft_save',f.body(r));
 assert.equal(saved.draft.actor_id,f.partner.id);assert.equal(saved.draft.values.packed_count,'42');
});

test('regular-task valid delegates and the existing reviewer-owner rule remain independent of collaboration',async t=>{
 const f=await fixture(t,'task');f.endPair();
 f.DB.raw.prepare("UPDATE sop_records SET state=json_set(state,'$.delegates',json(?)) WHERE id=?").run(JSON.stringify([f.partner.id]),f.id);
 let r=await f.read();await f.ok('field','sop_bulk_output_draft_save',f.body(r));
 f.DB.raw.prepare("UPDATE sop_records SET state=json_set(state,'$.owner_transfers',json(?)) WHERE id=?").run(JSON.stringify([{from_id:f.partner.id,to_id:f.owner.id}]),f.id);
 assert.equal((await f.call('field','sop_bulk_output_draft_read',{id:f.id})).ok,false,'transferred-away delegate remains revoked');
 const reviewer={id:f.owner.id,name:'合成审核负责人',role:'reviewer',scope:'office',departments:['bulk']};
 r=await bulkOutputDraft({action:'sop_bulk_output_draft_read',id:f.id},{...f.env,SOP_REQUEST_USER:reviewer},reviewer);
 const saved=await bulkOutputDraft({action:'sop_bulk_output_draft_save',client_req_id:crypto.randomUUID(),...f.body(r)},{...f.env,SOP_REQUEST_USER:reviewer},reviewer);
 assert.equal(saved.draft.actor_id,reviewer.id);assert.equal(saved.draft.by,reviewer.name);
});
