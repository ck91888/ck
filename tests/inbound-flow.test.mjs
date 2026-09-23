import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
function setup(){const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={},success=true)=>{const r=await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,k:env.ADMINKEY||'',client_req_id:crypto.randomUUID(),...data})}),env);const x=await r.json();if(success)assert.equal(x.ok,true,x.message||x.error);return x;};
 const plan=(classes,extra={})=>call('v2_inbound_plan_create',{customer:'虚拟入库客户',biz_classes:classes,lines:[{unit_type:'carton',planned_qty:50}],...extra});
 const unload=async p=>{const j=await call('v2_unload_job_start',{plan_id:p.id,worker_id:'UNLOAD',worker_name:'卸货甲',biz_class:'bulk'});await call('v2_unload_job_finish',{job_id:j.job_id,worker_id:'UNLOAD',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}],box_count:50});return j;};
 const detail=p=>call('v2_inbound_plan_detail',{id:p.id});return {DB,env,call,plan,unload,detail};}

test('putaway code is required, normalized, editable and rejects duplicate references before saving',async()=>{
 const {plan,call,DB,detail}=setup();
 const missing=await call('v2_inbound_plan_create',{customer:'测试',biz_classes:['direct_ship']},false);assert.equal(missing.ok,false);assert.match(missing.error,/外部/);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,0);
 const p=await plan(['direct_ship'],{external_inbound_no:'  WMS-TEST-001  '});assert.equal((await detail(p)).plan.external_inbound_no,'WMS-TEST-001');
 const dup=await call('v2_inbound_plan_create',{customer:'重复',biz_classes:['direct_ship'],external_inbound_no:'WMS-TEST-001'},false);assert.equal(dup.ok,false);
 await call('v2_inbound_plan_update',{id:p.id,biz_classes:['direct_ship'],external_inbound_no:'WMS-TEST-002'});assert.equal((await detail(p)).plan.external_inbound_no,'WMS-TEST-002');
 const bad=await call('v2_inbound_plan_update',{id:p.id,biz_classes:['direct_ship'],external_inbound_no:''},false);assert.equal(bad.ok,false);assert.equal((await detail(p)).plan.external_inbound_no,'WMS-TEST-002');
});
test('cross dock, pallet return and relabel plans complete on unloading with no fictional inbound jobs',async()=>{
 const {plan,unload,detail,DB,call}=setup();
 for(const biz of ['bulk','return','change_order']){const p=await plan([biz]),j=await unload(p),d=await detail(p);assert.equal(d.plan.status,'completed',biz);assert.equal(d.biz_tasks[0].completion_source,'unload');assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs WHERE related_doc_id=?').get(p.id).n,1);assert.equal(DB.raw.prepare('SELECT total_minutes FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').get(p.id).total_minutes,0);
 const again=await call('v2_unload_job_finish',{job_id:j.job_id,worker_id:'UNLOAD',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]});assert.equal(again.already_completed,true);assert.equal((await detail(p)).plan.status,'completed');}
});
test('external scan and direct API start bind the original plan and completion writes back to it',async()=>{
 const {plan,unload,call,detail,DB}=setup();const p=await plan(['direct_ship'],{external_inbound_no:'WMS-FLOOR-001'});await unload(p);assert.equal((await detail(p)).plan.status,'arrived_pending_putaway');
 const scan=await call('v2_inbound_resolve_code',{code:' WMS-FLOOR-001 ',biz_class:'direct_ship'});assert.equal(scan.kind,'system');assert.equal(scan.plan.id,p.id);
 const j=await call('v2_inbound_job_start',{external_inbound_no:'WMS-FLOOR-001',job_type:'inbound_direct',biz_class:'direct_ship',worker_id:'PUT',worker_name:'理货甲'});assert.equal(j.plan_id,p.id);
 await call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'PUT',complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:50}]});
 assert.equal((await detail(p)).plan.status,'completed');assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);
 const done=await call('v2_inbound_resolve_code',{code:'WMS-FLOOR-001',biz_class:'direct_ship'});assert.equal(done.kind,'biz_already_completed');
 const unknown=await call('v2_inbound_job_start',{external_inbound_no:'WRONG-NO',customer_name:'误扫',job_type:'inbound_direct',biz_class:'direct_ship',worker_id:'PUT'},false);assert.equal(unknown.ok,false);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);
});
test('mixed plan auto-completes only unload dispositions and leaves work requests pending until performed',async()=>{
 const {plan,call,detail,DB}=setup();const p=await plan(['direct_ship','bulk','return','change_order'],{external_inbound_no:'WMS-MIX',work_requests:[{title:'另行换单',instructions:'卸货后更换标签',department:'bulk'}]});
 const u=await call('v2_unload_job_start',{plan_id:p.id,worker_id:'U'});
 const j=await call('v2_inbound_job_start',{plan_id:p.id,job_type:'inbound_bulk',biz_class:'bulk',worker_id:'B',worker_name:'大货理货甲'});
 const premature=await call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'B',complete_job:true},false);assert.equal(premature.error,'unload_not_finished');
 await call('v2_unload_job_finish',{job_id:u.job_id,worker_id:'U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:50}]});
 const half=await detail(p);assert.notEqual(half.plan.status,'completed');assert.deepEqual(half.pending_biz_classes,['direct_ship']);
 await call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'B',complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:20}]});
 assert.equal((await detail(p)).plan.status,'completed');assert.equal(DB.raw.prepare('SELECT biz_class FROM v2_ops_jobs WHERE id=?').get(j.job_id).biz_class,'bulk');
 assert.equal(JSON.parse(DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(p.needs[0].id).state).status,'pending');
});
test('putaway candidate lists exclude unload-only plans in either labor department',async()=>{
 const {plan,unload,call}=setup();const p=await plan(['direct_ship','bulk'],{external_inbound_no:'WMS-CANDIDATE'}),cross=await plan(['bulk']);await unload(p);await unload(cross);
 for(const biz of ['direct_ship','bulk']){const list=await call('v2_inbound_plan_ops_candidates',{scene:'putaway',required_biz_class:biz});assert.deepEqual(list.items.map(x=>x.id),[p.id]);}
 const removed=await call('v2_inbound_job_start',{plan_id:p.id,job_type:'inbound_change_order',biz_class:'change_order',worker_id:'C'},false);assert.equal(removed.error,'inbound_change_order_removed');
});
test('return inbound remains an independent labor session and cannot attach to pallet-return plans',async()=>{
 const {plan,unload,call,detail,DB}=setup();const pallet=await plan(['return']);await unload(pallet);
 const j=await call('v2_inbound_job_start',{job_type:'inbound_return',biz_class:'return',worker_id:'R',worker_name:'退件甲'});assert.notEqual(j.plan_id,pallet.id);assert.equal(DB.raw.prepare('SELECT source_type FROM v2_inbound_plans WHERE id=?').get(j.plan_id).source_type,'return_session');
 await call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:'R',complete_job:true});assert.equal((await detail(pallet)).plan.status,'completed');assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(j.job_id).status,'completed');
});
test('legacy arrived plans can add a reference; stale edits, duplicates and rebinds during putaway are rejected',async()=>{
 const {plan,unload,call,DB,detail}=setup();const p=await plan(['direct_ship'],{external_inbound_no:'OLD-CODE'});await unload(p);DB.raw.prepare("UPDATE v2_inbound_plans SET external_inbound_no='' WHERE id=?").run(p.id);
 await call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'',external_inbound_no:'FIXED-CODE'});assert.equal((await detail(p)).plan.external_inbound_no,'FIXED-CODE');
 const stale=await call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'',external_inbound_no:'OTHER'},false);assert.equal(stale.ok,false);
 await call('v2_inbound_job_start',{plan_id:p.id,job_type:'inbound_direct',biz_class:'direct_ship',worker_id:'P'});
 const busy=await call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'FIXED-CODE',external_inbound_no:'OTHER'},false);assert.equal(busy.ok,false);assert.match(busy.error,/正在进行/);
});
test('ambiguous historical external numbers do not select an arbitrary plan',async()=>{
 const {plan,unload,call,DB}=setup();const a=await plan(['direct_ship'],{external_inbound_no:'DUP-A'}),b=await plan(['direct_ship'],{external_inbound_no:'DUP-B'});await unload(a);await unload(b);DB.raw.prepare("UPDATE v2_inbound_plans SET external_inbound_no='DUP-A' WHERE id=?").run(b.id);
 const resolved=await call('v2_inbound_resolve_code',{code:'DUP-A',biz_class:'direct_ship'});assert.equal(resolved.kind,'status_not_allowed');assert.match(resolved.message,/多张/);
});

test('unplanned unloading finalized by office follows the same putaway and automatic completion rules',async()=>{
 const {call,detail,DB}=setup();
 for(const [biz,code,status] of [['direct_ship','WMS-FEEDBACK','arrived_pending_putaway'],['bulk','','completed']]){
  const u=await call('v2_unplanned_unload_start',{worker_id:'FB-U',worker_name:'反馈卸货甲'});
  await call('v2_unplanned_unload_finish',{job_id:u.job_id,worker_id:'FB-U',result_lines:[{unit_type:'carton',actual_qty:10}]});
  if(code){const missing=await call('v2_feedback_finalize_to_inbound',{feedback_id:u.feedback_id,customer:'反馈客户',biz_classes:[biz]},false);assert.equal(missing.ok,false);assert.equal(DB.raw.prepare('SELECT status FROM v2_field_feedbacks WHERE id=?').get(u.feedback_id).status,'unloaded_pending_info');}
  const result=await call('v2_feedback_finalize_to_inbound',{feedback_id:u.feedback_id,customer:'反馈客户',biz_classes:[biz],external_inbound_no:code});
  const d=await detail({id:result.inbound_plan_id});assert.equal(d.plan.status,status);assert.equal(d.plan.external_inbound_no,code);assert.ok(d.plan.unload_completed_at);
 }
});

test('legacy dynamic and generic feedback forms save external references instead of bypassing putaway',async()=>{
 const {plan,call,DB,detail}=setup();const p=await plan(['bulk']);
 DB.raw.prepare("UPDATE v2_inbound_plans SET source_type='field_dynamic',status='unloaded_pending_info',unload_completed_at=? WHERE id=?").run(new Date().toISOString(),p.id);
 await call('v2_inbound_dynamic_finalize',{id:p.id,biz_classes:['direct_ship','bulk'],external_inbound_no:'WMS-OLD-DYNAMIC'});
 const d=await detail(p);assert.equal(d.plan.external_inbound_no,'WMS-OLD-DYNAMIC');assert.equal(d.plan.status,'partially_completed');assert.deepEqual(d.pending_biz_classes,['direct_ship']);
 DB.raw.prepare("INSERT INTO v2_field_feedbacks(id,feedback_type,title,status) VALUES('OLD-FB','unload_no_doc','旧反馈','open')").run();
 const converted=await call('v2_feedback_convert_to_inbound',{feedback_id:'OLD-FB',customer:'旧反馈客户',biz_classes:['direct_ship'],external_inbound_no:'WMS-OLD-FB'});
 assert.equal((await detail({id:converted.inbound_plan_id})).plan.external_inbound_no,'WMS-OLD-FB');
});

test('A1 and A2 complete independently; the plan closes only after both and keeps actual quantities',async()=>{
 const {plan,unload,call,detail,DB}=setup();
 const p=await plan(['direct_ship'],{external_inbound_nos:['A1','A2']});await unload(p);
 const initial=await detail(p);assert.deepEqual(initial.plan.external_inbound_nos,['A1','A2']);assert.equal(initial.plan.inbound_progress.completed,0);
 const ambiguous=await call('v2_inbound_resolve_code',{code:p.display_no,biz_class:'direct_ship'});assert.equal(ambiguous.kind,'status_not_allowed');assert.match(ambiguous.message,/多个/);
 const scan=await call('v2_inbound_resolve_code',{code:'A1',biz_class:'direct_ship'});assert.equal(scan.plan.id,p.id);assert.equal(scan.plan.selected_external_inbound_no,'A1');
 const a=await call('sop_native_start',{payload:{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:'A1',job_type:'inbound_direct',biz_class:'direct_ship',client_req_id:'A1-start'},workers:[{id:'A1-W',name:'测试甲'}],lead_id:'A1-W',estimated_minutes:10});
 await call('v2_inbound_job_finish',{job_id:a.job_id,worker_id:'A1-W',complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:20}]});
 const half=await detail(p);assert.equal(half.plan.status,'partially_completed');assert.equal(half.plan.inbound_progress.completed,1);assert.equal(half.plan.inbound_progress.total,2);assert.notEqual(half.biz_tasks[0].status,'completed');assert.equal(half.lines[0].putaway_qty,20);
 const doneScan=await call('v2_inbound_resolve_code',{code:'A1',biz_class:'direct_ship'});assert.equal(doneScan.kind,'biz_already_completed');
 const doneStart=await call('v2_inbound_job_start',{plan_id:p.id,external_inbound_no:'A1',job_type:'inbound_direct',biz_class:'direct_ship',worker_id:'REPEAT'},false);assert.equal(doneStart.ok,false);
 const forced=await call('v2_inbound_plan_force_complete',{id:p.id,reason:'测试不能跳过A2'},false);assert.equal(forced.error,'external_inbounds_pending');
 const b=await call('v2_inbound_job_start',{external_inbound_no:'A2',job_type:'inbound_direct',biz_class:'direct_ship',worker_id:'A2-W',worker_name:'测试乙'});assert.notEqual(a.job_id,b.job_id);
 await call('v2_inbound_job_finish',{job_id:b.job_id,worker_id:'A2-W',complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:30}]});
 const all=await detail(p);assert.equal(all.plan.status,'completed');assert.equal(all.plan.inbound_progress.completed,2);assert.equal(all.biz_tasks[0].status,'completed');assert.equal(all.lines[0].putaway_qty,50);assert.match(all.biz_tasks[0].worker_names,/测试甲/);assert.match(all.biz_tasks[0].worker_names,/测试乙/);
 await call('v2_inbound_job_finish',{job_id:b.job_id,worker_id:'A2-W',complete_job:true,result_lines:[{unit_type:'carton',putaway_qty:30}]});assert.equal((await detail(p)).lines[0].putaway_qty,50);
 assert.equal(DB.raw.prepare("SELECT count(DISTINCT inbound_external_no) n FROM v2_ops_jobs WHERE related_doc_id=? AND job_type='inbound_direct'").get(p.id).n,2);
});
test('multiple references normalize paste, reject cross-plan conflicts, and preserve finished codes during amendments',async()=>{
 const {plan,unload,call,detail}=setup();const p=await plan(['direct_ship','bulk'],{external_inbound_no:' Ａ1， A2;A3\nA2 '});assert.deepEqual((await detail(p)).plan.external_inbound_nos,['A1','A2','A3']);
 const duplicate=await call('v2_inbound_plan_create',{customer:'重复',biz_classes:['direct_ship'],external_inbound_nos:['OTHER','A2']},false);assert.equal(duplicate.ok,false);assert.match(duplicate.error,/A2/);
 await unload(p);const a=await call('v2_inbound_job_start',{plan_id:p.id,external_inbound_no:'A1',biz_class:'direct_ship',job_type:'inbound_direct',worker_id:'A'});
 const busy=await call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'A1\nA2\nA3',external_inbound_nos:['A1','A2']},false);assert.equal(busy.ok,false);
 await call('v2_inbound_job_finish',{job_id:a.job_id,worker_id:'A',complete_job:true});
 const removeDone=await call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'A1\nA2\nA3',external_inbound_nos:['A2','A3']},false);assert.equal(removeDone.ok,false);assert.match(removeDone.error,/已完成/);
 await call('v2_inbound_plan_bind_external',{id:p.id,previous_code:'A1\nA2\nA3',external_inbound_nos:['A1','A2','A4']});
 const updated=await detail(p);assert.equal(updated.plan.inbound_progress.completed,1);assert.deepEqual(updated.plan.external_inbound_nos,['A1','A2','A4']);
 assert.equal((await call('v2_inbound_resolve_code',{code:'A3',biz_class:'direct_ship'})).kind,'status_not_allowed');
});
test('parallel external bills never share a job; missing or mismatched bill cannot start',async()=>{
 const {plan,unload,call,detail,DB}=setup();const p=await plan(['direct_ship','bulk'],{external_inbound_nos:['A1','A2']});await unload(p);
 const missing=await call('v2_inbound_job_start',{plan_id:p.id,biz_class:'direct_ship',job_type:'inbound_direct',worker_id:'X'},false);assert.equal(missing.ok,false);
 const wrong=await call('v2_inbound_job_start',{plan_id:p.id,external_inbound_no:'WRONG',biz_class:'direct_ship',job_type:'inbound_direct',worker_id:'X'},false);assert.equal(wrong.ok,false);
 const start=(code,worker,type='inbound_direct',biz='direct_ship')=>call('v2_inbound_job_start',{plan_id:p.id,external_inbound_no:code,job_type:type,biz_class:biz,worker_id:worker});
 const a=await start('A1','A'),b=await start('A2','B');assert.notEqual(a.job_id,b.job_id);
 const join=await start('A1','C');assert.equal(join.job_id,a.job_id);
 const other=await call('v2_inbound_job_start',{plan_id:p.id,external_inbound_no:'A1',job_type:'inbound_bulk',biz_class:'bulk',worker_id:'D'},false);assert.equal(other.error,'external_code_working');
 const candidates=await call('v2_inbound_plan_ops_candidates',{scene:'putaway',biz_class:'direct_ship'});assert.equal(candidates.items[0].inbound_progress.items.filter(x=>x.status==='working').length,2);
 assert.equal((await detail(p)).plan.inbound_progress.completed,0);
 assert.throws(()=>DB.raw.prepare("INSERT INTO v2_ops_jobs(id,related_doc_type,related_doc_id,job_type,status,inbound_external_no) VALUES('RACE','inbound_plan',?,'inbound_direct','working','A1')").run(p.id),/UNIQUE/);
 assert.throws(()=>DB.raw.prepare("INSERT INTO v2_ops_jobs(id,related_doc_type,related_doc_id,job_type,status,inbound_external_no) VALUES('STALE','inbound_plan',?,'inbound_direct','working','OLD')").run(p.id),/reference changed/);
});
test('new inbound rules are gated off outside staging upgrade',async()=>{
 const {env,plan,call,detail,unload}=setup();env.SOP_UPGRADE_ENABLED='false';env.ADMINKEY='local-fixture-only';
 // Use a local fixture key when the staging upgrade authentication wrapper is disabled.
 const p=await plan(['direct_ship']);assert.equal((await detail(p)).plan.external_inbound_no,'');
 const cross=await plan(['bulk']);await unload(cross);assert.equal((await detail(cross)).plan.status,'arrived_pending_putaway');
 const code=await call('v2_inbound_resolve_code',{code:'UNREGISTERED',biz_class:'direct_ship'});assert.equal(code.kind,'external');
});
