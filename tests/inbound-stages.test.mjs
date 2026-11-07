import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
const tally='inbound_direct_tally',putaway='inbound_direct_putaway';
export function fixture(t){
 const DB=database();t?.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
 const create=async(extra={})=>{const p=await ok('v2_inbound_plan_create',{customer:'Synthetic stage fixture',biz_classes:['direct_ship'],external_inbound_no:'SYNTH-A',lines:[{unit_type:'carton',planned_qty:50}],...extra});return p;};
 const unload=async p=>{const u=await ok('v2_unload_job_start',{plan_id:p.id,worker_id:'SYNTH-U'});await ok('v2_unload_job_finish',{job_id:u.job_id,worker_id:'SYNTH-U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]});};
 const start=(p,code,type,workers=[{id:'SYNTH-W-'+code,name:'合成人员 '+code}])=>call('sop_native_start',{payload:{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:code,job_type:type,biz_class:type==='inbound_bulk'?'bulk':'direct_ship',client_req_id:crypto.randomUUID()},workers,lead_id:workers[0].id,estimated_minutes:15});
 const finish=(j,extra={})=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,...extra});
 const detail=p=>ok('v2_inbound_plan_detail',{id:p.id});
 return {DB,env,call,ok,create,unload,start,finish,detail};
}
test('independent stages preserve all tally fields and record shared putaway workload once',async t=>{
 const f=fixture(t),p=await f.create();await f.unload(p);
 assert.equal((await f.start(p,'SYNTH-A',putaway)).ok,false);
 const a=await f.start(p,'SYNTH-A',tally);assert.equal(a.ok,true,JSON.stringify(a));assert.equal((await f.detail(p)).plan.status,'putting_away');
 const tallyData={result_lines:[{unit_type:'carton',putaway_qty:50}],extra_ops:{sort_qty:0,label_qty:2,repair_box_qty:0,other_op_remark:'合成操作说明'},result_note:'合成入库说明',remark:'合成备注'};
 assert.equal((await f.finish(a,tallyData)).ok,true);
 let d=await f.detail(p);assert.equal(d.plan.status,'tallied_pending_putaway');assert.equal(d.plan.inbound_progress.completed,0);assert.equal(d.lines[0].putaway_qty,50);assert.equal(d.biz_tasks[0].status,'pending');
 assert.equal((await f.start(p,'SYNTH-A',tally)).ok,false);
 const staff=[{id:'SYNTH-P1',name:'合成上架甲'},{id:'SYNTH-P2',name:'合成上架乙'}],b=await f.start(p,'SYNTH-A',putaway,staff);assert.equal(b.ok,true,JSON.stringify(b));assert.notEqual(a.job_id,b.job_id);assert.equal((await f.detail(p)).plan.status,'direct_putting_away');
 for(const extra of [{},{location_count:2},{location_count:'',used_forklift:false},{location_count:-1,used_forklift:true},{location_count:1.5,used_forklift:false},{location_count:true,used_forklift:false},{location_count:2,used_forklift:'no'}])assert.equal((await f.finish(b,extra)).ok,false,JSON.stringify(extra));
 const payload={location_count:3,used_forklift:false,result_lines:[{unit_type:'carton',putaway_qty:999}],extra_ops:{sort_qty:999}};
 const replies=await Promise.all([f.finish(b,payload),f.finish(b,payload)]);assert.ok(replies.some(r=>r.ok),JSON.stringify(replies));
 assert.equal((await f.finish(b,payload)).ok,true);
 d=await f.detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,50);assert.equal(d.plan.inbound_progress.completed,1);assert.deepEqual(d.jobs.find(j=>j.id===b.job_id).putaway_workload,{location_count:3,used_forklift:false});
 const dashboard=await f.ok('v2_dashboard_order_export',{});const putawayRows=dashboard.rows.filter(r=>r.job_id===b.job_id);assert.equal(putawayRows.length,1);assert.match(putawayRows[0].result_summary,/货位数 3.*是否使用叉车 否/);
 const exported=await f.ok('v2_inbound_plan_export',{});assert.equal(exported.rows[0].上架货位数,3);assert.match(exported.rows[0].上架叉车使用明细,/SYNTH-A：否/);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(b.job_id).n,1);
 const saved=JSON.parse(f.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(a.job_id).result_json);for(const [k,v] of Object.entries(tallyData))assert.deepEqual(saved[k],v);
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id IN (?,?) AND left_at='' ").get(a.job_id,b.job_id).n,0);
});
test('blank/zero tally fields remain allowed, pause/resume changes crew without changing stage or clock history',async t=>{
 const f=fixture(t),p=await f.create();await f.unload(p);const a=await f.start(p,'SYNTH-A',tally);assert.equal(a.ok,true,JSON.stringify(a));
 await f.ok('sop_native_pause',{job_id:a.job_id,revision:1,reason:'合成等待'});
 assert.equal((await f.finish(a)).ok,false);assert.equal((await f.start(p,'SYNTH-A',tally)).ok,false);assert.equal((await f.start(p,'SYNTH-A',putaway)).ok,false);
 const staff=[{id:'SYNTH-REPLACEMENT',name:'合成接班'}];await f.ok('sop_native_resume',{job_id:a.job_id,revision:2,workers:staff,lead_id:staff[0].id});a.lead=staff[0];
 assert.equal((await f.finish(a,{extra_ops:{sort_qty:'',label_qty:0,repair_box_qty:0}})).ok,true);
 const b=await f.start(p,'SYNTH-A',putaway);assert.equal(b.ok,true,JSON.stringify(b));
 await f.ok('sop_native_pause',{job_id:b.job_id,revision:1,reason:'合成等待上架'});assert.equal((await f.finish(b,{location_count:0,used_forklift:true})).ok,false);
 await f.ok('sop_native_resume',{job_id:b.job_id,revision:2,workers:[b.lead],lead_id:b.lead.id});assert.equal((await f.finish(b,{location_count:0,used_forklift:true})).ok,true);
 assert.equal((await f.detail(p)).plan.status,'completed');assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=?').get(a.job_id).n,2);
});
test('multiple bills and mixed departments finish only after every final stage',async t=>{
 const f=fixture(t),p=await f.create({biz_classes:['direct_ship','bulk_putaway','bulk'],external_inbound_no:'SYNTH-A\nSYNTH-B\nSYNTH-BULK',bulk_external_inbound_no:'SYNTH-BULK'});await f.unload(p);
 const a=await f.start(p,'SYNTH-A',tally),b=await f.start(p,'SYNTH-B',tally),bulk=await f.start(p,'SYNTH-BULK','inbound_bulk');for(const r of [a,b,bulk])assert.equal(r.ok,true,JSON.stringify(r));
 for(const [j,qty] of [[a,20],[b,20],[bulk,10]])assert.equal((await f.finish(j,{result_lines:[{unit_type:'carton',putaway_qty:qty}]})).ok,true);
 let d=await f.detail(p);assert.notEqual(d.plan.status,'completed');assert.equal(d.plan.inbound_progress.completed,1);assert.equal(d.lines[0].putaway_qty,50);
 const pa=await f.start(p,'SYNTH-A',putaway),pb=await f.start(p,'SYNTH-B',putaway);assert.equal(pa.ok,true,JSON.stringify(pa));assert.equal(pb.ok,true,JSON.stringify(pb));
 assert.equal((await f.finish(pa,{location_count:2,used_forklift:true})).ok,true);d=await f.detail(p);assert.notEqual(d.plan.status,'completed');assert.equal(d.plan.inbound_progress.completed,2);
 assert.equal((await f.finish(pb,{location_count:4,used_forklift:false})).ok,true);d=await f.detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,50);
});
test('unload completion gate, atomic rollback and immutable tallied references',async t=>{
 const f=fixture(t),p=await f.create(),u=await f.ok('v2_unload_job_start',{plan_id:p.id,worker_id:'SYNTH-U'}),a=await f.start(p,'SYNTH-A',tally);assert.equal(a.ok,true,JSON.stringify(a));
 assert.equal((await f.finish(a)).error,'unload_not_finished');await f.ok('v2_unload_job_finish',{job_id:u.job_id,worker_id:'SYNTH-U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]});
 f.DB.raw.exec("CREATE TRIGGER synth_fail BEFORE UPDATE OF status ON v2_inbound_plans WHEN NEW.status='tallied_pending_putaway' BEGIN SELECT RAISE(ABORT,'synthetic interruption'); END");
 assert.equal((await f.finish(a,{result_lines:[{unit_type:'carton',putaway_qty:50}]})).ok,false);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(a.job_id).n,0);assert.equal((await f.detail(p)).lines[0].putaway_qty,0);
 f.DB.raw.exec('DROP TRIGGER synth_fail');assert.equal((await f.finish(a,{result_lines:[{unit_type:'carton',putaway_qty:50}]})).ok,true);
 assert.equal((await f.call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'SYNTH-A',external_inbound_no:'SYNTH-OTHER'})).ok,false);
 assert.equal((await f.call('v2_inbound_plan_force_complete',{id:p.id,reason:'不能跳过上架'})).ok,false);
 const b=await f.start(p,'SYNTH-A',putaway);assert.equal(b.ok,true,JSON.stringify(b));
 f.DB.raw.exec("CREATE TRIGGER synth_putaway_fail BEFORE UPDATE OF status ON v2_inbound_plan_biz_tasks WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'synthetic interruption'); END");assert.equal((await f.finish(b,{location_count:1,used_forklift:false})).ok,false);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(b.job_id).n,0);f.DB.raw.exec('DROP TRIGGER synth_putaway_fail');assert.equal((await f.finish(b,{location_count:1,used_forklift:false})).ok,true);
});
test('legacy in-progress and completed combined jobs retain original contract',async t=>{
 const f=fixture(t),p=await f.create();await f.unload(p);const old=await f.start(p,'SYNTH-A','inbound_direct');assert.equal(old.ok,true,JSON.stringify(old));assert.equal((await f.start(p,'SYNTH-A',tally)).ok,false);assert.equal((await f.start(p,'SYNTH-A',putaway)).ok,false);
 assert.equal((await f.finish(old,{result_lines:[{unit_type:'carton',putaway_qty:50}]})).ok,true);assert.equal((await f.detail(p)).plan.status,'completed');assert.equal((await f.start(p,'SYNTH-A',putaway)).ok,false);assert.equal((await f.finish(old)).ok,true);assert.equal((await f.detail(p)).lines[0].putaway_qty,50);
});
test('simultaneous staged finishes and competing starts preserve one crew and whole-plan completion',async t=>{
 const f=fixture(t),p=await f.create({external_inbound_no:'SYNTH-A\nSYNTH-B'});await f.unload(p);
 const staff=i=>[{id:'SYNTH-RACE-'+i,name:'合成并发 '+i}];
 const raced=await Promise.all([f.start(p,'SYNTH-A',tally,staff(1)),f.start(p,'SYNTH-A',tally,staff(2))]);assert.equal(raced.filter(r=>r.ok).length,1);const a=raced.find(r=>r.ok),b=await f.start(p,'SYNTH-B',tally,staff(3));assert.equal(b.ok,true);
 for(const r of await Promise.all([f.finish(a,{result_lines:[{unit_type:'carton',putaway_qty:20}]}),f.finish(b,{result_lines:[{unit_type:'carton',putaway_qty:30}]})]))assert.equal(r.ok,true,JSON.stringify(r));
 assert.throws(()=>f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='completed' WHERE id=?").run(p.id),/putaway stage/);
 assert.throws(()=>f.DB.raw.prepare("UPDATE v2_inbound_plans SET external_inbound_no='SYNTH-B' WHERE id=?").run(p.id),/reference cannot change/);
 const pa=await f.start(p,'SYNTH-A',putaway,staff(4)),pb=await f.start(p,'SYNTH-B',putaway,staff(5));for(const j of [pa,pb])assert.equal(j.ok,true,JSON.stringify(j));
 for(const r of await Promise.all([f.finish(pa,{location_count:2,used_forklift:false}),f.finish(pb,{location_count:3,used_forklift:true})]))assert.equal(r.ok,true,JSON.stringify(r));
 const d=await f.detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,50);assert.equal(d.plan.inbound_progress.completed,2);const ex=await f.ok('v2_inbound_plan_export',{});assert.equal(ex.rows[0].上架货位数,5);
});
test('courier completion gate and putaway start rollback preserve the tally result for retry',async t=>{
 const f=fixture(t),p=await f.create({lines:[{unit_type:'carton',planned_qty:50},{unit_type:'courier',planned_qty:1,tracking_nos:['999001990001']}]});await f.unload(p);const a=await f.start(p,'SYNTH-A',tally);assert.equal(a.ok,true);
 assert.equal((await f.finish(a)).error,'courier_not_received');
 f.env.SOP_ATTENDANCE_ENABLED='true';const receiver=(await f.ok('sop_attendance_checkin',{name:'合成收货员',agency:'가온'})).record;
 const batch=await f.ok('sop_courier_receive',{operation:'start',workers:[{id:receiver.badgeId,name:receiver.name}],lead_id:receiver.badgeId});
 await f.ok('sop_courier_receive',{tracking_no:'999001990001',owner:'8-1',batch_id:batch.batch.id,scanner_badge:receiver.badgeId});f.env.SOP_ATTENDANCE_ENABLED='false';
 assert.equal((await f.finish(a)).ok,true);
 f.DB.raw.exec("CREATE TRIGGER synth_stage_start_failure BEFORE INSERT ON v2_ops_job_workers WHEN NEW.worker_id='SYNTH-FAIL' BEGIN SELECT RAISE(ABORT,'synthetic start interruption'); END");
 const staff=[{id:'SYNTH-PASS',name:'合成甲'},{id:'SYNTH-FAIL',name:'合成乙'}],body={payload:{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:'SYNTH-A',job_type:putaway,biz_class:'direct_ship',client_req_id:'SYNTH-RETRY-PUTAWAY'},workers:staff,lead_id:staff[0].id,estimated_minutes:10};
 assert.equal((await f.call('sop_native_start',body)).ok,false);assert.equal((await f.detail(p)).plan.status,'tallied_pending_putaway');assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_jobs WHERE job_type=?").get(putaway).n,0);
 f.DB.raw.exec('DROP TRIGGER synth_stage_start_failure');const j=await f.ok('sop_native_start',body);assert.equal((await f.ok('sop_native_start',body)).job_id,j.job_id);assert.equal((await f.finish(j,{location_count:1,used_forklift:false})).ok,true);
});
