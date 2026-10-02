// Candidate regressions only; stored outside the business repository.
// Uses the actual selected Worker and SQLite :memory:. Never calls online URLs.
import test from 'node:test';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';

const root=process.env.CK_REVIEW_REPO
 ?pathToFileURL(process.env.CK_REVIEW_REPO.replace(/\\/g,'/').replace(/\/$/,'')+'/')
 :new URL('../',import.meta.url);
const {default:worker}=await import(new URL('worker-v2/index.js',root));
const {database}=await import(new URL('tests/d1-adapter.mjs',root));
const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);
const fixtureStamp=new Date(day+'T00:00:00+09:00').toISOString();

async function setup(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const raw=async(action,data={})=>{
  const response=await worker.fetch(new Request('https://fixture.invalid/api',{
   method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})
  }),env);
  return {status:response.status,body:await response.json()};
 };
 const ok=async(action,data={})=>{const r=await raw(action,data);assert.equal(r.body.ok,true,JSON.stringify(r));return r.body;};
 await ok('sop_identity');
 function result(id,jobId,qty,stamp=fixtureStamp,extra={}){
  DB.raw.prepare('INSERT INTO v2_ops_job_results(id,job_id,box_count,result_json,created_at,source,previous_result_id) VALUES(?,?,?,?,?,?,?)')
   .run(id,jobId,qty,JSON.stringify({box_count:qty}),stamp,extra.source||'',extra.previous_result_id||'');
 }
 function completed(jobId,{native=false}={}){
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,display_no,created_at,updated_at,finished_at) VALUES(?,'bulk_op','completed',?,?,?,?)")
   .run(jobId,jobId,fixtureStamp,fixtureStamp,fixtureStamp);
  if(native){
   DB.raw.prepare("INSERT INTO sop_records(id,kind,revision,department,state,updated_at) VALUES(?,'dispatch',1,'bulk',?,?)")
    .run(jobId,JSON.stringify({id:jobId,kind:'dispatch',department:'bulk',owner_id:'staging-demo-manager',title:'QA synthetic native task'}),fixtureStamp);
   DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at) VALUES(?,?,?,?,?,?)')
    .run(jobId+'-SEG',jobId,'QA-'+jobId,'QA synthetic worker',fixtureStamp,fixtureStamp);
  }
 }
 const correct=(jobId,qty,extra={})=>ok('v2_ops_job_result_update',{
  job_id:jobId,reason:'QA synthetic correction',by:'QA-only',result_json:{box_count:qty},...extra
 });
 async function currentQuantities(jobId){
  const list=await ok('v2_dashboard_order_list',{doc_no:jobId});
  const detail=await ok('v2_dashboard_order_detail',{job_id:jobId});
  const csv=await ok('v2_dashboard_order_export',{doc_no:jobId});
  return {list:list.items.find(x=>x.id===jobId)?.box_count_sum,detail:detail.parsed.box_count_sum,csv:csv.rows.find(x=>x.job_id===jobId||x['单号']===jobId)?.box_count_sum};
 }
 return {DB,env,raw,ok,result,completed,correct,currentQuantities};
}

test('old completed output correction replaces 10 with 8 in list, detail and export; audit history remains',async t=>{
 const s=await setup(t),id='QA-REPLACE';s.completed(id);s.result(id+'-ORIG',id,10);
 const corrected=await s.correct(id,8);
 assert.equal(corrected.previous_result_id,id+'-ORIG');
 assert.deepEqual(await s.currentQuantities(id),{list:8,detail:8,csv:8});
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(id).n,2);
});

test('successive corrections 10 to 8 to 6 follow their replacement chain and retain three audit rows',async t=>{
 const s=await setup(t),id='QA-CHAIN';s.completed(id);s.result(id+'-ORIG',id,10);
 const first=await s.correct(id,8);
 // Keep order deterministic without a sleep or relying on same-millisecond SQL ties.
 s.DB.raw.prepare('UPDATE v2_ops_job_results SET created_at=? WHERE id=?').run(new Date(Date.parse(fixtureStamp)+1).toISOString(),first.result_id);
 const second=await s.correct(id,6);
 assert.equal(second.previous_result_id,first.result_id);
 assert.deepEqual(await s.currentQuantities(id),{list:6,detail:6,csv:6});
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(id).n,3);
});

test('correcting only the latest independent result preserves earlier genuine outputs',async t=>{
 const s=await setup(t),id='QA-INDEPENDENT';s.completed(id);
 s.result(id+'-FIRST',id,10);s.result(id+'-SECOND',id,5,new Date(Date.parse(fixtureStamp)+1).toISOString());
 const corrected=await s.correct(id,8);
 assert.equal(corrected.previous_result_id,id+'-SECOND');
 assert.deepEqual(await s.currentQuantities(id),{list:18,detail:18,csv:18});
 assert.equal(s.DB.raw.prepare('SELECT box_count FROM v2_ops_job_results WHERE id=?').get(id+'-FIRST').box_count,10);
});

test('retrying the same correction request returns the same result and adds no history row',async t=>{
 const s=await setup(t),id='QA-RETRY';s.completed(id);s.result(id+'-ORIG',id,10);
 const request={client_req_id:crypto.randomUUID()},first=await s.correct(id,8,request),retry=await s.correct(id,8,request);
 assert.deepEqual(retry,first);
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(id).n,2);
 assert.deepEqual(await s.currentQuantities(id),{list:8,detail:8,csv:8});
});

test('native dispatch reported output and rankings also use the effective corrected result',async t=>{
 const s=await setup(t),id='QA-RANK';s.completed(id,{native:true});s.result(id+'-ORIG',id,10);
 await s.correct(id,8);
 const dashboard=await s.ok('sop_dashboard',{date:day});
 for(const bucket of ['rankings','reported_outputs']){
  const total=dashboard[bucket].filter(x=>x.metric==='完成量'&&x.tasks.includes(id)).reduce((n,x)=>n+x.quantity,0);
  assert.equal(total,8,bucket);
 }
});

test('reviewed SOP tasks reject the old result-update route without changing either record',async t=>{
 const s=await setup(t),task=await s.ok('sop_task_create',{
  department:'bulk',title:'QA synthetic approved task',job_type:'bulk_op',workers:[{id:'QA-SOP-W',name:'QA worker'}],lead_id:'QA-SOP-W',estimated_minutes:5
 });
 await s.ok('sop_task_start',{id:task.id,revision:1});
 await s.ok('sop_task_complete_review',{id:task.id,revision:2,decision:'pass',reason:'QA review',result:{quantity:10,unit:'箱',description:'QA actual ten',location:'QA-only',operated_box_count:10}});
 const before=(await s.ok('sop_get',{id:task.id})).record;
 const resultRows=s.DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY id').all(task.id);
 const legacy=await s.raw('v2_ops_job_result_update',{job_id:task.id,reason:'QA old route boundary',result_json:{box_count:8},by:'QA-only'});
 assert.equal(legacy.body.ok,false,JSON.stringify(legacy));assert.equal(legacy.status,409);
 assert.deepEqual((await s.ok('sop_get',{id:task.id})).record,before);
 assert.deepEqual(s.DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY id').all(task.id),resultRows);
});

test('two concurrent corrections reading the same prior result commit at most one replacement',async t=>{
 const s=await setup(t),id='QA-CONCURRENT';s.completed(id);s.result(id+'-ORIG',id,10);
 const prepare=s.DB.prepare.bind(s.DB);let arrivals=0,release;
 const barrier=new Promise(resolve=>release=resolve);
 // Both real routes observe the same old result before either inserts its correction.
 s.DB.prepare=sql=>{
  const statement=prepare(sql),first=statement.first.bind(statement);
  statement.first=async()=>{
   const row=await first();
   if(/FROM\s+v2_ops_job_results/i.test(sql)&&/ORDER\s+BY\s+created_at\s+DESC/i.test(sql)&&statement.args.includes(id)){
    arrivals++;if(arrivals===2)release();await barrier;
   }
   return row;
  };
  return statement;
 };
 const responses=await Promise.all([
  s.raw('v2_ops_job_result_update',{job_id:id,reason:'QA correction A',result_json:{box_count:8},by:'QA-only'}),
  s.raw('v2_ops_job_result_update',{job_id:id,reason:'QA correction B',result_json:{box_count:6},by:'QA-only'})
 ]);
 assert.equal(arrivals,2,'fixture must rendezvous both real previous-result reads');
 assert.equal(responses.filter(r=>r.body.ok).length,1,JSON.stringify(responses));
 const winner=responses.find(r=>r.body.ok),rows=s.DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=?').all(id);
 assert.equal(rows.filter(x=>x.source==='manual_correction'&&x.previous_result_id===id+'-ORIG').length,1);
 const expected=winner.body.summary.includes('8')?8:6;
 assert.deepEqual(await s.currentQuantities(id),{list:expected,detail:expected,csv:expected});
});
