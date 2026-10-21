import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {executionFixture} from './work-execution-fixture.mjs';
const type='inbound_direct_putaway';
function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
 const body=(refs,who='SYNTH-P')=>({payload:{action:'v2_inbound_job_start',job_type:type,biz_class:'direct_ship',putaway_refs:refs,client_req_id:crypto.randomUUID()},workers:[{id:who,name:'合成上架人员'}],lead_id:who,estimated_minutes:10});
 const start=(refs,who)=>call('sop_native_start',body(refs,who));
 const finish=(j,extra={})=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,location_count:7,used_forklift:false,...extra});
 const ready=async(code,{plan=false,done=true}={})=>{
  let p;if(plan){p=await ok('v2_inbound_plan_create',{customer:'合成客户',external_inbound_no:code,biz_classes:['direct_ship'],lines:[{unit_type:'carton',planned_qty:6}]});const u=await ok('v2_unload_job_start',{plan_id:p.id,worker_id:'SYNTH-U'});await ok('v2_unload_job_finish',{job_id:u.job_id,worker_id:'SYNTH-U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:6}]});}
  const b=body([],'SYNTH-T-'+code);delete b.payload.putaway_refs;Object.assign(b.payload,{job_type:'inbound_direct_tally',external_inbound_no:code});const j=await ok('sop_native_start',b);
  if(done)await ok('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,result_lines:plan?[{unit_type:'carton',putaway_qty:6}]:[],result_note:'合成理货历史'});
  return {id:j.plan_id,code,tally:j,display_no:p?.display_no};
 };
 const count=table=>DB.raw.prepare('SELECT count(*) n FROM '+table).get().n;
 const status=p=>DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(p.id).status;
 return {DB,env,call,ok,body,start,finish,ready,count,status};
}
test('mixed RU/external/independent references deduplicate aliases and finish with one crew, one result, task quantity and clocks',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-PLAN',{plan:true}),b=await f.ready('9998810022');
 const tallyBefore=f.DB.raw.prepare("SELECT * FROM v2_ops_job_results WHERE job_id IN (?,?)").all(a.tally.job_id,b.tally.job_id);
 const body=f.body([a.display_no,a.code,a.id,{plan_id:b.id,external_inbound_no:b.code}]);body.workers.push({id:'SYNTH-P2',name:'合成乙'});
 const j=await f.ok('sop_native_start',body);assert.equal(j.putaway_refs.length,2);assert.equal(f.count('ck_putaway_links'),2);
 for(const p of [a,b]){assert.equal(f.status(p),'direct_putting_away');const d=await f.ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.plan.inbound_progress.items[0].status,'putting_away');assert.equal(d.jobs.filter(x=>x.id===j.job_id).length,1);}
 f.DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-5*60000).toISOString(),j.job_id);
 const responses=await Promise.all([f.finish(j),f.finish(j)]);assert.ok(responses.every(x=>x.ok),JSON.stringify(responses));assert.equal((await f.finish(j)).ok,true);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,1);
 for(const p of [a,b])assert.equal(f.status(p),'completed');
 assert.deepEqual(f.DB.raw.prepare("SELECT * FROM v2_ops_job_results WHERE job_id IN (?,?)").all(a.tally.job_id,b.tally.job_id),tallyBefore);
 assert.equal(f.DB.raw.prepare('SELECT putaway_qty FROM v2_inbound_plan_lines WHERE plan_id=?').get(a.id).putaway_qty,6);
 const detail=await f.ok('v2_ops_job_detail',{job_id:j.job_id});assert.equal(detail.putaway_refs.length,2);assert.ok(detail.job.business_no.includes(a.code)&&detail.job.business_no.includes(b.code));assert.equal(detail.workers.length,2);
 const rows=(await f.ok('v2_dashboard_order_export')).rows.filter(r=>r.job_id===j.job_id);assert.equal(rows.length,1);assert.equal(rows[0].total_minutes,10);assert.equal(rows[0].putaway_location_count,7);assert.equal(rows[0].putaway_used_forklift,false);assert.equal(rows[0].putaway_quantity_scope,'task');assert.match(rows[0].result_summary,/货位数 7（整任务）/);assert.ok(rows[0].单号.includes(b.code));
 const plans=(await f.ok('v2_inbound_plan_export')).rows;assert.equal(plans[0].上架货位数,'');assert.match(plans[0]['共同上架任务（整任务数量，未分摊）'],/货位数 7/);
 assert.ok(f.DB.raw.prepare('SELECT total_minutes FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').get(a.id).total_minutes<5,'shared clock is not copied to per-plan totals');
});
test('two external bills from one plan complete each reference and leave unique job history',async t=>{
 const f=fixture(t),p=await f.ok('v2_inbound_plan_create',{customer:'合成两单',external_inbound_no:'SYNTH-A\nSYNTH-B',biz_classes:['direct_ship'],lines:[{unit_type:'carton',planned_qty:6}]});const u=await f.ok('v2_unload_job_start',{plan_id:p.id,worker_id:'SYNTH-U'});await f.ok('v2_unload_job_finish',{job_id:u.job_id,worker_id:'SYNTH-U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:6}]});
 await f.ready('SYNTH-A');await f.ready('SYNTH-B');const j=await f.start(['SYNTH-A','SYNTH-B']);assert.equal(j.ok,true,JSON.stringify(j));assert.equal((await f.finish(j)).ok,true);const d=await f.ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.plan.status,'completed');assert.equal(d.plan.inbound_progress.completed,2);assert.equal(d.jobs.filter(x=>x.id===j.job_id).length,1);
});
test('unknown, untallied, tally-in-progress and mixed department members reject the entire group',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-READY'),working=await f.ready('SYNTH-WORKING',{done:false}),pending=await f.ok('v2_inbound_plan_create',{customer:'合成未理货',external_inbound_no:'SYNTH-PENDING',biz_classes:['direct_ship']}),bulk=await f.ok('v2_inbound_plan_create',{customer:'合成大货',external_inbound_no:'SYNTH-BULK',biz_classes:['bulk_putaway']});
 const before=f.count('v2_ops_jobs');for(const ref of ['SYNTH-UNKNOWN',working.code,pending.id,bulk.id]){assert.equal((await f.start([a.code,ref])).ok,false);assert.equal(f.status(a),'tallied_pending_putaway');assert.equal(f.count('v2_ops_jobs'),before);}
 const wrong=f.body([a.code]);wrong.labor_department='bulk';assert.equal((await f.call('sop_native_start',wrong)).ok,false);
});
test('occupied secondary member, completed member and legacy single starts cannot race into another shared task',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),c=await f.ready('SYNTH-C');const j=await f.start([a.code,b.code]);assert.equal(j.ok,true);
 for(const refs of [[c.code,b.code],[b.code],[a.id]])assert.equal((await f.start(refs,'SYNTH-OTHER')).ok,false);
 const single=f.body([],'SYNTH-OTHER');delete single.payload.putaway_refs;single.payload.external_inbound_no=b.code;assert.equal((await f.call('sop_native_start',single)).ok,false);
 assert.equal((await f.finish(j)).ok,true);assert.equal((await f.start([c.code,b.code],'SYNTH-OTHER')).ok,false);assert.equal(f.status(c),'tallied_pending_putaway');
});
test('overlapping concurrent starts claim all members or none; replay uses exact request',async t=>{
 const f=fixture(t);for(const c of ['SYNTH-A','SYNTH-B','SYNTH-C'])await f.ready(c);
 const a=f.body(['SYNTH-A','SYNTH-B'],'SYNTH-X'),b=f.body(['SYNTH-C','SYNTH-B'],'SYNTH-Y');const r=await Promise.all([f.call('sop_native_start',a),f.call('sop_native_start',b)]);assert.equal(r.filter(x=>x.ok).length,1,JSON.stringify(r));assert.equal(f.count('ck_putaway_links'),2);
 const winner=r[0].ok?a:b,j=r.find(x=>x.ok);assert.equal((await f.ok('sop_native_start',winner)).job_id,j.job_id);winner.payload.putaway_refs=['SYNTH-C'];assert.equal((await f.call('sop_native_start',winner)).ok,false);
});
test('last-member start failure rolls back job, links, dispatch, clocks and request; same request can retry',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),body=f.body([a.code,b.code]);const before=f.count('sop_records');
 f.DB.raw.exec("CREATE TRIGGER synth_link_fail BEFORE INSERT ON ck_putaway_links WHEN NEW.position=1 BEGIN SELECT RAISE(ABORT,'synthetic last-member failure'); END");assert.equal((await f.call('sop_native_start',body)).ok,false);assert.equal(f.count('ck_putaway_links'),0);assert.equal(f.count('sop_records'),before);assert.equal(f.status(a),'tallied_pending_putaway');assert.equal(f.status(b),'tallied_pending_putaway');assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_idempotency_keys WHERE idem_key=?').get(body.payload.client_req_id).n,0);
 f.DB.raw.exec('DROP TRIGGER synth_link_fail');assert.equal((await f.call('sop_native_start',body)).ok,true);
});
test('completion failure on second plan rolls back every plan, one result and all worker clocks, then retry succeeds',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),j=await f.start([a.code,b.code]);assert.equal(j.ok,true);
 f.DB.raw.exec("CREATE TRIGGER synth_finish_fail BEFORE UPDATE OF status ON v2_inbound_plans WHEN NEW.external_inbound_no='SYNTH-B' AND NEW.status='completed' BEGIN SELECT RAISE(ABORT,'synthetic final member failure'); END");assert.equal((await f.finish(j)).ok,false);
 for(const p of [a,b])assert.equal(f.status(p),'direct_putting_away');assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).n,0);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(j.job_id).n,1);
 f.DB.raw.exec('DROP TRIGGER synth_finish_fail');assert.equal((await f.finish(j)).ok,true);
});
test('secondary member revalidation during completion, plan lock and pause/resume preserve membership',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),j=await f.start([a.code,b.code]);assert.equal(j.ok,true);
 assert.throws(()=>f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(b.id));
 await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'合成暂停'});assert.equal((await f.finish(j)).ok,false);assert.equal((await f.start([b.code],'SYNTH-NEW')).ok,false);
 await f.ok('sop_native_resume',{job_id:j.job_id,revision:2,workers:[j.lead],lead_id:j.lead.id});
 f.DB.raw.prepare("UPDATE v2_ops_jobs SET status='cancelled' WHERE id=?").run(b.tally.job_id);assert.equal((await f.finish(j)).ok,false);assert.equal(f.status(a),'direct_putting_away');f.DB.raw.prepare("UPDATE v2_ops_jobs SET status='completed' WHERE id=?").run(b.tally.job_id);
 assert.equal((await f.finish(j)).ok,true);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=?').get(j.job_id).n,2);
});
test('cancel releases all bills, preserves histories and clocks, is idempotent and allows a fresh assignment',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),j=await f.start([a.code,b.code]);assert.equal(j.ok,true);
 const request={job_id:j.job_id,revision:1,reason:'合成安排变更',client_req_id:crypto.randomUUID()};await f.ok('sop_putaway_cancel',request);await f.ok('sop_putaway_cancel',request);
 for(const p of [a,b])assert.equal(f.status(p),'tallied_pending_putaway');assert.equal(f.count('ck_putaway_links'),2);assert.equal((await f.finish(j)).ok,false);
 const next=await f.start([b.code,a.code]);assert.equal(next.ok,true,JSON.stringify(next));assert.notEqual(next.job_id,j.job_id);assert.equal((await f.finish(next)).ok,true);assert.equal(f.count('ck_putaway_links'),4);
});
test('cancel versus finish has one terminal outcome without partial state',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),j=await f.start([a.code,b.code]);
 const replies=await Promise.all([f.call('sop_putaway_cancel',{job_id:j.job_id,revision:1,reason:'合成并发取消'}),f.finish(j)]);assert.equal(replies.filter(x=>x.ok).length,1,JSON.stringify(replies));assert.equal(f.status(a),f.status(b));const s=f.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status;assert.equal(s==='completed',f.status(a)==='completed');
});
test('real field sessions reject unauthorized and cross-department dispatch, finish and cancellation',async t=>{
 const f=await executionFixture(t);await f.daily('A',['direct_ship']);await f.daily('B',['import']);const w=await f.person();
 const body=(code,job_type='inbound_direct_tally')=>({payload:{action:'v2_inbound_job_start',job_type,biz_class:'direct_ship',external_inbound_no:code,client_req_id:crypto.randomUUID()},workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId,estimated_minutes:10});
 for(const code of ['SYNTH-A','SYNTH-B']){const j=await f.ok('A','sop_native_start',body(code));await f.ok('A','v2_inbound_job_finish',{job_id:j.job_id,worker_id:w.badgeId,complete_job:true});}
 const multi=body('');delete multi.payload.external_inbound_no;multi.payload.job_type=type;multi.payload.putaway_refs=['SYNTH-A','SYNTH-B'];assert.equal((await f.call('B','sop_native_start',multi)).ok,false);
 const j=await f.ok('A','sop_native_start',multi);for(const action of ['v2_inbound_job_finish','sop_putaway_cancel'])assert.equal((await f.call('B',action,{job_id:j.job_id,worker_id:w.badgeId,complete_job:true,location_count:2,used_forklift:true,revision:1,reason:'越权测试'})).ok,false);
 await f.ok('A','sop_putaway_cancel',{job_id:j.job_id,revision:1,reason:'合成关闭'});
});
test('pre-upgrade single putaway without member links can finish or cancel without rewriting old clocks',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-OLD-SINGLE'),j=await f.start([a.code]);assert.equal(j.ok,true);f.DB.raw.prepare('DELETE FROM ck_putaway_links WHERE job_id=?').run(j.job_id);
 const before=f.DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').all(j.job_id);assert.equal((await f.finish(j)).ok,true);assert.equal(f.status(a),'completed');assert.equal(f.DB.raw.prepare('SELECT joined_at FROM v2_ops_job_workers WHERE job_id=?').get(j.job_id).joined_at,before[0].joined_at);
 const b=await f.ready('SYNTH-OLD-CANCEL'),old=await f.start([b.code]);f.DB.raw.prepare('DELETE FROM ck_putaway_links WHERE job_id=?').run(old.job_id);await f.ok('sop_putaway_cancel',{job_id:old.job_id,revision:1,reason:'合成旧任务取消'});assert.equal(f.status(b),'tallied_pending_putaway');assert.equal((await f.start([b.code])).ok,true);
});
test('cancel failure rolls back crew and all members, paused cancellation can then retry safely',async t=>{
 const f=fixture(t),a=await f.ready('SYNTH-A'),b=await f.ready('SYNTH-B'),j=await f.start([a.code,b.code]);await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'合成暂停'});
 const body={job_id:j.job_id,revision:2,reason:'合成取消',client_req_id:crypto.randomUUID()};f.DB.raw.exec("CREATE TRIGGER synth_cancel_fail BEFORE UPDATE OF status ON v2_inbound_plans WHEN NEW.external_inbound_no='SYNTH-B' AND NEW.status='tallied_pending_putaway' BEGIN SELECT RAISE(ABORT,'synthetic cancel failure'); END");assert.equal((await f.call('sop_putaway_cancel',body)).ok,false);assert.equal(f.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'paused');assert.equal(f.status(a),'direct_putting_away');assert.equal(f.status(b),'direct_putting_away');f.DB.raw.exec('DROP TRIGGER synth_cancel_fail');await f.ok('sop_putaway_cancel',body);for(const p of [a,b])assert.equal(f.status(p),'tallied_pending_putaway');
});
