import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {validateOperationInput,readOperation} from '../shared/internal-operations.js';
function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,body={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env)).json();
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
 const payload=(type,start={},request=crypto.randomUUID())=>({payload:{action:type==='inbound_return'?'v2_inbound_job_start':'v2_ops_job_start',flow_stage:'internal',biz_class:type==='inbound_return'?'return':'bulk',job_type:type,...start,client_req_id:request},workers:[{id:'SYNTH-A',name:'合成甲'},{id:'SYNTH-B',name:'合成乙'}],lead_id:'SYNTH-A',estimated_minutes:15,labor_department:type==='inbound_return'?'direct_ship':'bulk'});
 const start=(type,data)=>ok('sop_native_start',payload(type,data)),finish=(j,data={})=>call(j.type==='inbound_return'?'v2_inbound_job_finish':'v2_ops_job_finish',{job_id:j.job_id,worker_id:'SYNTH-A',complete_job:true,...data});
 const snap=id=>({job:DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(id),workers:DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id').all(id),results:DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY id').all(id)});
 return {DB,env,call,ok,payload,start,finish,snap};
}
const cases=[
 ['inventory',{inventory_type:'blind'},{inventory_count:0,location_count:0,used_forklift:false},'finish'],
 ['inventory',{inventory_type:'precise',internal_metrics:{inventory_count:14,location_count:2,used_forklift:true}},{},'start'],
 ['disposal',{internal_metrics:{pick_count:0,location_count:1,used_forklift:false}},{},'start'],
 ['qc',{}, {qc_count:0},'finish'],
 ['other_internal',{}, {organizing_content:'  合成库位整理 <测试>  '},'finish'],
 ['inbound_return',{}, {return_order_count:2,return_product_count:0},'finish']
];
for(const [type,input,metrics,phase] of cases)test(type+' '+(input.inventory_type||'')+' records at required phase; two workers, retries and export count once',async t=>{
 const f=fixture(t),j={...await f.start(type,input),type};const before=f.snap(j.job_id),d=readOperation(before.job);
 assert.equal(d.version,1);assert.equal(d.inventory_type,input.inventory_type);assert.equal(before.results.length,0);
 const list=(await f.ok('v2_dashboard_order_list',{})).items.find(x=>x.id===j.job_id);assert.match(list.result_summary,/开始|库内|盲盘|精盘|退件入库结果/);
 if(Object.keys(metrics).length){assert.equal((await f.finish(j)).ok,false);assert.deepEqual(f.snap(j.job_id),before);}
 else assert.equal((await f.finish(j,{internal_metrics:{location_count:5}})).ok,false,'cannot replace start metrics at finish');
 const replies=await Promise.all([f.finish(j,{internal_metrics:metrics}),f.finish(j,{internal_metrics:metrics})]);assert.ok(replies.every(x=>x.ok),JSON.stringify(replies));
 const after=f.snap(j.job_id);assert.equal(after.job.status,'completed');assert.equal(after.workers.filter(w=>!w.left_at).length,0);assert.equal(after.results.length,1);
 const operation=readOperation(after.job);assert.deepEqual(operation.start,d.start);for(const [k,v] of Object.entries(phase==='start'?input.internal_metrics:metrics))assert.equal(operation[phase][k],typeof v==='string'?v.trim():v);
 assert.equal((await f.finish(j,{internal_metrics:{qc_count:999}})).ok,true);assert.deepEqual(f.snap(j.job_id),after);
 const detail=await f.ok('v2_dashboard_order_detail',{job_id:j.job_id}),row=(await f.ok('v2_dashboard_order_export',{})).rows.find(x=>x.job_id===j.job_id);
 assert.equal(row.worker_count,2);assert.equal(row.result_count,1);assert.equal(row.internal_record_phase,phase);assert.equal(row.result_summary,detail.parsed.result_summary);assert.deepEqual(JSON.parse(row.internal_operation_json),operation);
 for(const [key,value] of Object.entries(phase==='start'?input.internal_metrics:metrics)){const column={pick_count:'disposal_pick_count',location_count:'internal_location_count',used_forklift:'internal_used_forklift'}[key]||key;assert.equal(row[column],typeof value==='string'?value.trim():value);}
 if(type==='inbound_return'){assert.equal(row.return_order_count,2);assert.equal(row.return_product_count,0);assert.equal(f.DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(after.job.related_doc_id).status,'completed');}
});
test('strict shared quantity validation distinguishes zero from missing; forklift and content are explicit',()=>{
 for(const value of [undefined,null,'','  ',true,false,-1,'bad',1.2,100000001,Infinity,[],{},'NaN'])assert.throws(()=>validateOperationInput('qc',undefined,'finish',{qc_count:value}),undefined,JSON.stringify(value));
 for(const value of [0,'0',100000000])assert.equal(validateOperationInput('qc',undefined,'finish',{qc_count:value}).qc_count,Number(value));
 for(const value of [undefined,null,'yes','no',0,1])assert.throws(()=>validateOperationInput('inventory','precise','start',{inventory_count:0,location_count:0,used_forklift:value}));
 for(const value of [undefined,null,'','  \n\t',1,'x'.repeat(2001)])assert.throws(()=>validateOperationInput('other_internal',undefined,'finish',{organizing_content:value}));
});
test('new start cannot omit or forge inventory type, timing or explicit forklift',async t=>{
 const f=fixture(t);
 for(const input of [{},{inventory_type:'legacy'},{inventory_type:'blind',internal_metrics:{inventory_count:1}},{inventory_type:'precise'},{inventory_type:'precise',internal_metrics:{inventory_count:0,location_count:0}},{inventory_type:'precise',internal_metrics:{inventory_count:0,location_count:-1,used_forklift:false}}])assert.equal((await f.call('sop_native_start',f.payload('inventory',input))).ok,false);
 for(const [type,input] of [['qc',{internal_metrics:{qc_count:1}}],['other_internal',{internal_metrics:{organizing_content:'early'}}],['disposal',{}],['inbound_return',{internal_metrics:{return_order_count:1,return_product_count:2}}]])assert.equal((await f.call('sop_native_start',f.payload(type,input))).ok,false);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);
 for(const type of ['inventory','qc','disposal','other_internal','inbound_return'])assert.equal((await f.call(type==='inbound_return'?'v2_inbound_job_start':'v2_ops_job_start',{job_type:type,worker_id:'SYNTH-A',biz_class:type==='inbound_return'?'return':'bulk'})).ok,false);
});
test('start transaction rollback and identical concurrent requests create one dispatch; changed retry is rejected',async t=>{
 const f=fixture(t),p=f.payload('inventory',{inventory_type:'precise',internal_metrics:{inventory_count:0,location_count:0,used_forklift:false}});
 await f.ok('v2_dashboard_order_list');f.DB.raw.exec("CREATE TRIGGER synthetic_start_fail BEFORE UPDATE OF shared_result_json ON v2_ops_jobs BEGIN SELECT RAISE(ABORT,'synthetic fail'); END");
 assert.equal((await f.call('sop_native_start',p)).ok,false);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,0);
 f.DB.raw.exec('DROP TRIGGER synthetic_start_fail');const replies=await Promise.all([f.call('sop_native_start',p),f.call('sop_native_start',p)]);assert.ok(replies.every(x=>x.ok));assert.equal(replies[0].job_id,replies[1].job_id);
 const before=f.snap(replies[0].job_id);assert.equal((await f.call('sop_native_start',{...p,payload:{...p.payload,internal_metrics:{...p.payload.internal_metrics,inventory_count:1}}})).ok,false);assert.deepEqual(f.snap(replies[0].job_id),before);
});
test('finish failure is atomic; pause/resume keeps start snapshot and work segments; generic bypasses fail',async t=>{
 const f=fixture(t),j=await f.start('inventory',{inventory_type:'blind'}),id=j.job_id;
 const detail=()=>f.ok('v2_ops_job_detail',{job_id:id});let d=await detail();await f.ok('sop_native_pause',{job_id:id,revision:d.dispatch.revision,reason:'合成暂停'});
 assert.equal((await f.finish(j,{internal_metrics:{inventory_count:0,location_count:0,used_forklift:false}})).ok,false);
 d=await detail();await f.ok('sop_native_resume',{job_id:id,revision:d.dispatch.revision,workers:[{id:'SYNTH-A',name:'合成甲'}],lead_id:'SYNTH-A'});const before=f.snap(id);
 for(const action of ['v2_ops_job_manual_finalize','v2_ops_job_result_update','v2_import_delivery_job_finish','v2_inbound_job_finish'])assert.equal((await f.call(action,{job_id:id,worker_id:'SYNTH-A',result_json:{inventory_count:2},complete_job:true})).ok,false,action);
 for(const body of [{inventory_type:'precise'},{shared_result:{internal_operation:{}}},{box_count:999},{pallet_count:999},{internal_metrics:{inventory_count:'bad',location_count:0,used_forklift:false}}])assert.equal((await f.finish(j,body)).ok,false);
 f.DB.raw.exec("CREATE TRIGGER synthetic_finish_fail BEFORE UPDATE OF status ON v2_ops_jobs WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'synthetic finish fail'); END");
 const input={internal_metrics:{inventory_count:0,location_count:0,used_forklift:false},client_req_id:'SYNTH-FINISH'};assert.equal((await f.finish(j,input)).ok,false);assert.deepEqual(f.snap(id),before);
 f.DB.raw.exec('DROP TRIGGER synthetic_finish_fail');assert.equal((await f.finish(j,input)).ok,true);assert.equal(f.snap(id).results.length,1);assert.equal(f.snap(id).workers.length,3);
});
test('last-worker leave and admin cleanup cannot finish new result tasks',async t=>{
 const f=fixture(t),j=await f.start('other_internal');await f.ok('v2_ops_job_leave',{job_id:j.job_id,worker_id:'SYNTH-A'});
 assert.notEqual(f.snap(j.job_id).job.status,'completed');await f.ok('v2_ops_job_leave',{job_id:j.job_id,worker_id:'SYNTH-B'});assert.equal(f.snap(j.job_id).job.status,'awaiting_close');
 await f.ok('v2_admin_cleanup_job_statuses',{dry_run:false});assert.equal(f.snap(j.job_id).job.status,'awaiting_close');assert.equal((await f.finish(j,{internal_metrics:{organizing_content:'合成整理'}})).ok,true);
});
test('old ongoing jobs retain original contract; old completed records and missing types are never backfilled',async t=>{
 const f=fixture(t);await f.ok('v2_dashboard_order_list');
 for(const type of ['inventory','disposal','qc','other_internal','inbound_return']){
  const id='SYNTH-LEGACY-'+type,time='2026-10-01T01:00:00Z';f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,shared_result_json,created_at) VALUES(?,?,'working',?,?)").run(id,type,JSON.stringify({remark:'合成旧备注'}),time);
  f.DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)").run(id+'-SEG',id,'SYNTH-OLD','合成旧人员',time);
  assert.equal((await f.finish({job_id:id,type},{worker_id:'SYNTH-OLD'})).ok,true);const after=f.snap(id);assert.equal(readOperation(after.job),null);assert.equal(JSON.parse(after.job.shared_result_json).remark,'合成旧备注');
  const row=(await f.ok('v2_dashboard_order_export')).rows.find(x=>x.job_id===id);assert.equal(row.internal_record_phase,'legacy');assert.equal(row.inventory_type,null);assert.equal(row.inventory_count,null);
  await f.finish({job_id:id,type},{worker_id:'SYNTH-OLD'});await f.ok('v2_dashboard_order_detail',{job_id:id});assert.deepEqual(f.snap(id),after);
 }
});
