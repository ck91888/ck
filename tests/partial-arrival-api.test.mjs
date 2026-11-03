import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
const tally='inbound_direct_tally',putaway='inbound_direct_putaway';
async function fixture(t){
 const seed=database();let DB=seed;if(process.env.CK_PARTIAL_NATIVE_D1==='true'){DB=await localD1(seed.raw);seed.raw.close();t.after(()=>DB.close());}else t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+JSON.stringify(r));return r;};
 const codes=['999001770001','999001770002'],code='SYNTH-PARTIAL-EXT';
 const create=extra=>ok('v2_inbound_plan_create',{customer:'合成部分到仓客户',biz_classes:['direct_ship'],external_inbound_no:code,lines:[{unit_type:'courier',tracking_nos:codes}],...extra});
 let batch,receiver;
 async function receive(index){
  env.SOP_ATTENDANCE_ENABLED='true';
  try{
   if(!receiver){receiver=(await ok('sop_attendance_checkin',{name:'合成快递扫描员',agency:'가온'})).record;batch=(await ok('sop_courier_receive',{operation:'start',workers:[{id:receiver.badgeId,name:receiver.name}],lead_id:receiver.badgeId})).batch;}
   return await ok('sop_courier_receive',{tracking_no:codes[index],owner:'8-1',batch_id:batch.id,scanner_badge:receiver.badgeId});
  }finally{env.SOP_ATTENDANCE_ENABLED='false';}
 }
 let serial=0;
 const start=(p,type=tally,external=code,request)=>{const id='SYNTH-TALLY-'+(++serial);return call('sop_native_start',{payload:{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:external,job_type:type,biz_class:type==='inbound_bulk'?'bulk':'direct_ship',client_req_id:request||crypto.randomUUID()},workers:[{id,name:'合成理货员 '+serial}],lead_id:id,estimated_minutes:15});};
 const finish=(j,quantity,extra={})=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,result_lines:quantity===undefined?[]:[{unit_type:'courier',putaway_qty:quantity}],...extra});
 const detail=p=>ok('v2_inbound_plan_detail',{id:p.id});
 return {DB,env,call,ok,codes,code,create,receive,start,finish,detail};
}
test('first arrived parcel can be recognized, selected and tallied, later arrivals get a new bounded job, final putaway waits',async t=>{
 const f=await fixture(t),p=await f.create();
 assert.equal((await f.ok('v2_inbound_resolve_code',{code:f.code,biz_class:'direct_ship',job_type:tally})).kind,'status_not_allowed');
 assert.equal((await f.start(p)).ok,false);
 await f.receive(0);let d=await f.detail(p);assert.equal(d.plan.status,'pending');assert.equal(d.lines[0].actual_qty,1);
 assert.equal((await f.ok('v2_inbound_resolve_code',{code:f.code,biz_class:'direct_ship',job_type:tally})).kind,'system');
 assert.ok((await f.ok('v2_inbound_plan_ops_candidates',{scene:'putaway',biz_class:'direct_ship'})).items.some(x=>x.id===p.id));
 const a=await f.start(p);assert.equal(a.ok,true,JSON.stringify(a));assert.equal(a.arrival_batch.received,1);assert.equal(a.arrival_batch.lines[0].available,1);
 assert.equal((await f.finish(a,2)).ok,false);assert.equal((await f.finish(a)).ok,false);
 assert.equal((await f.finish(a,1)).ok,true);assert.equal((await f.finish(a,1)).ok,true);
 d=await f.detail(p);assert.notEqual(d.plan.status,'completed');assert.equal(d.plan.inbound_progress.completed,0);assert.equal(d.lines[0].putaway_qty,1);assert.ok(d.biz_tasks.every(x=>x.status!=='completed'));
 assert.equal((await f.start(p)).ok,false);assert.equal((await f.start(p,putaway)).ok,false);
 assert.equal((await f.call('v2_inbound_plan_force_complete',{id:p.id,reason:'合成禁止提前完结'})).ok,false);
 await f.receive(1);const b=await f.start(p);assert.equal(b.ok,true,JSON.stringify(b));assert.notEqual(b.job_id,a.job_id);assert.equal(b.arrival_batch.lines[0].available,1);
 assert.equal((await f.finish(b,2)).ok,false);assert.equal((await f.finish(b,1)).ok,true);
 d=await f.detail(p);assert.equal(d.lines[0].putaway_qty,2);assert.equal(d.plan.status,'tallied_pending_putaway');assert.equal(d.plan.inbound_progress.completed,0);
 const c=await f.start(p,putaway);assert.equal(c.ok,true,JSON.stringify(c));assert.equal((await f.finish(c,undefined,{location_count:1,used_forklift:false})).ok,true);
 d=await f.detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,2);assert.equal(d.plan.inbound_progress.completed,1);
 assert.equal((await f.start(p)).ok,false);
});
test('new receipt during an open first batch cannot enlarge its allowance or count twice',async t=>{
 const f=await fixture(t),p=await f.create();await f.receive(0);const a=await f.start(p);assert.equal(a.ok,true,JSON.stringify(a));
 await f.receive(1);assert.equal((await f.finish(a,2)).ok,false);assert.equal((await f.finish(a,1)).ok,true);
 const b=await f.start(p);assert.equal(b.ok,true,JSON.stringify(b));const replies=await Promise.all([f.finish(b,1),f.finish(b,1)]);assert.ok(replies.some(x=>x.ok));
 const d=await f.detail(p);assert.equal(d.lines[0].actual_qty,2);assert.equal(d.lines[0].putaway_qty,2);assert.equal(d.jobs.filter(x=>x.job_type===tally&&x.status==='completed').length,2);
 const results=await f.DB.prepare('SELECT COUNT(*) AS n FROM v2_ops_job_results WHERE job_id IN (?,?)').bind(a.job_id,b.job_id).first();assert.equal(results.n,2);
});
test('pre-scanned partial receipt before plan creation links original receipt and permits first tally without fake unload',async t=>{
 const f=await fixture(t);const receipt=await f.receive(0),p=await f.create(),d=await f.detail(p);
 assert.equal(d.plan.status,'pending');assert.equal(d.plan.unload_completed_at,'');assert.equal(d.lines[0].actual_qty,1);assert.equal(d.plan.courier_progress.items.find(x=>x.tracking_no===f.codes[0]).received_at,receipt.item.received_at);
 const a=await f.start(p);assert.equal(a.ok,true,JSON.stringify(a));assert.equal((await f.finish(a,1)).ok,true);
 assert.equal((await f.detail(p)).jobs.filter(x=>x.job_type==='unload').length,0);
});
test('first courier tally preserves pending truck eligibility, then truck quantities and later courier remain bounded',async t=>{
 const f=await fixture(t),p=await f.create({lines:[{unit_type:'courier',tracking_nos:f.codes},{unit_type:'carton',planned_qty:3}]});
 await f.receive(0);const a=await f.start(p);assert.equal(a.ok,true,JSON.stringify(a));
 assert.equal((await f.finish(a,1,{result_lines:[{unit_type:'carton',putaway_qty:1}]})).ok,false);
 assert.equal((await f.finish(a,1)).ok,true);assert.equal((await f.detail(p)).plan.status,'pending');
 const lookup=await f.ok('v2_inbound_plan_find_by_code',{code:p.id,scene:'unload_trip'});assert.equal(lookup.plan.id,p.id);
 const u=await f.ok('v2_unload_job_start',{plan_id:p.id,worker_id:'SYNTH-UNLOAD'});
 await f.ok('v2_unload_job_finish',{job_id:u.job_id,worker_id:'SYNTH-UNLOAD',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:3}]});
 const b=await f.start(p);assert.equal(b.ok,true,JSON.stringify(b));assert.equal((await f.finish(b,undefined,{result_lines:[{unit_type:'carton',putaway_qty:3}]})).ok,true);
 assert.equal((await f.start(p,putaway)).ok,false);
 await f.receive(1);const c=await f.start(p);assert.equal(c.ok,true,JSON.stringify(c));assert.equal((await f.finish(c,1)).ok,true);
 const d=await f.detail(p);assert.equal(d.lines.find(l=>l.unit_type==='courier').putaway_qty,2);assert.equal(d.lines.find(l=>l.unit_type==='carton').putaway_qty,3);assert.equal(d.plan.status,'tallied_pending_putaway');
});
test('competing batch starts and different-reference finishes cannot consume the same arrived unit twice',async t=>{
 const f=await fixture(t),p=await f.create({external_inbound_no:f.code+'\nSYNTH-PARTIAL-B'});await f.receive(0);
 const started=await Promise.all([f.start(p),f.start(p)]);assert.equal(started.filter(x=>x.ok).length,1);
 const a=started.find(x=>x.ok),b=await f.start(p,tally,'SYNTH-PARTIAL-B');assert.equal(b.ok,true,JSON.stringify(b));
 const replies=await Promise.all([f.finish(a,1),f.finish(b,1)]);assert.equal(replies.filter(x=>x.ok).length,1,JSON.stringify(replies));
 const d=await f.detail(p);assert.equal(d.lines[0].putaway_qty,1);assert.notEqual(d.plan.status,'completed');
});
test('bulk putaway also tallies received portions without completing the external reference or business task early',async t=>{
 const f=await fixture(t),p=await f.create({biz_classes:['bulk_putaway'],bulk_external_inbound_no:f.code});await f.receive(0);
 const a=await f.start(p,'inbound_bulk');assert.equal(a.ok,true,JSON.stringify(a));assert.equal((await f.finish(a,1)).ok,true);
 let d=await f.detail(p);assert.equal(d.plan.inbound_progress.completed,0);assert.equal(d.biz_tasks.find(x=>x.biz_class==='bulk_putaway').status,'pending');assert.notEqual(d.plan.status,'completed');
 assert.equal((await f.start(p,'inbound_bulk')).ok,false);await f.receive(1);const b=await f.start(p,'inbound_bulk');assert.equal(b.ok,true,JSON.stringify(b));assert.equal((await f.finish(b,1)).ok,true);
 d=await f.detail(p);assert.equal(d.plan.status,'completed');assert.equal(d.plan.inbound_progress.completed,1);assert.equal(d.lines[0].putaway_qty,2);
});
test('pause, new arrival and resume preserve the original allowance and labor history, failed completion rolls back',async t=>{
 const f=await fixture(t),p=await f.create();await f.receive(0);const a=await f.start(p);assert.equal(a.ok,true,JSON.stringify(a));
 await f.ok('sop_native_pause',{job_id:a.job_id,revision:1,reason:'合成暂候'});await f.receive(1);assert.equal((await f.finish(a,1)).ok,false);
 await f.ok('sop_native_resume',{job_id:a.job_id,revision:2,workers:[a.lead],lead_id:a.lead.id});assert.equal((await f.finish(a,2)).ok,false);
 await f.DB.prepare(`CREATE TRIGGER synth_arrival_failure BEFORE UPDATE OF status ON v2_ops_jobs WHEN NEW.id=? AND NEW.status='completed' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END`.replace('?',"'"+a.job_id+"'")).run();
 const before=await f.DB.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id').bind(a.job_id).all();assert.equal((await f.finish(a,1)).ok,false);
 assert.equal((await f.DB.prepare('SELECT COUNT(*) AS n FROM v2_ops_job_results WHERE job_id=?').bind(a.job_id).first()).n,0);assert.equal((await f.detail(p)).lines[0].putaway_qty,0);assert.deepEqual((await f.DB.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id').bind(a.job_id).all()).results,before.results);
 await f.DB.prepare('DROP TRIGGER synth_arrival_failure').run();assert.equal((await f.finish(a,1)).ok,true);assert.equal((await f.detail(p)).lines[0].putaway_qty,1);
});
test('old plan-wide unique index and start trigger upgrade safely without rewriting completed evidence',async t=>{
 const f=await fixture(t);
 for(const name of ['ck_inbound_arrival_completion_guard','ck_inbound_arrival_scope_guard','ck_inbound_arrival_immutable','ck_inbound_stage_start','ck_inbound_combined_completed'])await f.DB.prepare('DROP TRIGGER '+name).run();
 await f.DB.prepare('DROP INDEX ck_inbound_stage_active').run();await f.DB.prepare('DROP TABLE ck_inbound_arrival_batches').run();
 await f.DB.prepare(`CREATE UNIQUE INDEX ck_inbound_stage_unique ON v2_ops_jobs(related_doc_id,inbound_external_no,job_type) WHERE related_doc_type='inbound_plan' AND job_type IN ('inbound_direct_tally','inbound_direct_putaway') AND status!='cancelled'`).run();
 await f.DB.prepare('DROP INDEX idx_v2_inbound_job_active_reference').run();await f.DB.prepare(`CREATE UNIQUE INDEX idx_v2_inbound_job_reference ON v2_ops_jobs(related_doc_id,inbound_external_no) WHERE related_doc_type='inbound_plan' AND inbound_external_no!='' AND job_type IN ('inbound_direct','inbound_bulk') AND status IN ('pending','working','awaiting_close','completed')`).run();
 await f.DB.prepare(`CREATE TRIGGER ck_inbound_stage_start BEFORE INSERT ON v2_ops_jobs WHEN NEW.job_type IN ('inbound_direct_tally','inbound_direct_putaway') AND EXISTS(SELECT 1 FROM v2_inbound_plans WHERE id=NEW.related_doc_id AND status='pending') BEGIN SELECT RAISE(ABORT,'legacy pending gate'); END`).run();
 await f.DB.prepare(`CREATE TABLE IF NOT EXISTS v2_schema_meta(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)`).run();await f.DB.prepare(`INSERT OR REPLACE INTO v2_schema_meta VALUES('schema_version','v2.20261021-inbound-owner','2026-11-01T00:00:00Z')`).run();
 await f.DB.prepare(`INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,status,shared_result_json) VALUES('SYNTH-OLD-J','inbound_direct','inbound_plan','SYNTH-OLD-P','completed','{"preserved":true}')`).run();
 await f.DB.prepare(`INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES('SYNTH-OLD-W','SYNTH-OLD-J','SYNTH-OLD-BADGE','合成历史操作员','2026-11-01T00:00:00Z','2026-11-01T01:00:00Z',60)`).run();
 const before=(await f.DB.prepare(`SELECT * FROM v2_ops_job_workers WHERE job_id='SYNTH-OLD-J'`).all()).results;
 const p=await f.create();await f.receive(0);const a=await f.start(p);assert.equal(a.ok,true,JSON.stringify(a));assert.equal((await f.finish(a,1)).ok,true);await f.receive(1);const b=await f.start(p);assert.equal(b.ok,true,JSON.stringify(b));assert.equal((await f.finish(b,1)).ok,true);
 assert.deepEqual((await f.DB.prepare(`SELECT * FROM v2_ops_job_workers WHERE job_id='SYNTH-OLD-J'`).all()).results,before);assert.equal((await f.DB.prepare(`SELECT shared_result_json FROM v2_ops_jobs WHERE id='SYNTH-OLD-J'`).first()).shared_result_json,'{"preserved":true}');
 assert.equal(await f.DB.prepare(`SELECT 1 FROM sqlite_master WHERE name='ck_inbound_stage_unique'`).first(),null);
 assert.ok(await f.DB.prepare(`SELECT 1 FROM sqlite_master WHERE name='ck_inbound_stage_active'`).first());
});
