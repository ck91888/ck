import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {database} from './d1-adapter.mjs';
import {handleSop} from '../worker-v2/sop.js';
import {handleBulkBatch} from '../worker-v2/bulk-work-batch.js';
import {FIELD_ACTIONS} from '../worker-v2/access-control.js';

function setup(){
 const DB=database();
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_USERS_JSON:JSON.stringify([
  {key:'manager-test',id:'M',name:'经理',role:'manager'},
  {key:'service-test',id:'S',name:'客服',role:'service',departments:['bulk']},
  {key:'dispatcher-test',id:'D',name:'派审',role:'dispatcher',departments:['bulk']},
  {key:'reviewer-test',id:'R',name:'审核',role:'reviewer',departments:['bulk']},
  {key:'viewer-test',id:'V',name:'只读',role:'viewer',departments:['bulk']}
 ])};
 const call=(action,data={},key='service-test')=>handleSop({action,sop_key:key,client_req_id:crypto.randomUUID(),...data},env);
 const get=async id=>(await call('sop_get',{id})).record;
 const mutate=async(action,id,fields={},key='service-test')=>call(action,{id,revision:(await get(id)).revision,...fields},key);
 const need=async(fields={})=>{const r=await call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'STOCK-'+crypto.randomUUID(),customer:'牛奶快递',title:'牛奶快递',instructions:'写错了',owner:'工单处理员待接单',planned_quantity:4,planned_unit:'箱',...fields});assert.equal(r.ok,true,r.error);return get(r.id);};
 return {DB,env,call,get,mutate,need};
}
const ids=groups=>groups.items.flatMap(g=>g.items.map(x=>x.id));

test('office can void an unstarted pending plan without deleting it, and lists, stats, print and dispatch drop it',async()=>{
 const {DB,env,call,get,mutate,need}=setup();
 try{
  assert.equal(FIELD_ACTIONS.has('sop_need_cancel'),false);
  const plan=await need({supply_chain_no:'SAME-BATCH',title:'牛奶快递'});
  const sibling=await need({supply_chain_no:'SAME-BATCH',title:'正确的作业',instructions:'保留'});
  assert.equal((await call('sop_dashboard')).alerts.some(x=>x.id===plan.id),true);
  const candidates=await handleBulkBatch({action:'sop_bulk_batch_candidates'},{...env,SOP_REQUEST_USER:{id:'D',name:'派审',role:'dispatcher',departments:['bulk'],scope:'office'}},{findOutbound:async()=>null,linkedNeeds:async()=>[]});
  assert.equal(candidates.ok,true,candidates.error);assert.ok(candidates.items.some(x=>x.id===plan.id));
  for(const key of ['viewer-test','reviewer-test'])assert.match((await mutate('sop_need_cancel',plan.id,{reason:'写错了'},key)).error,/无此操作权限/);
  const field=await handleSop({action:'sop_need_cancel',id:plan.id,revision:plan.revision,reason:'写错了',client_req_id:crypto.randomUUID()},{...env,SOP_REQUEST_USER:{id:'F',name:'现场',role:'service',departments:['bulk'],scope:'field'}});
  assert.match(field.error,/办公室/);
  assert.match((await mutate('sop_need_cancel',plan.id,{reason:'   '})).error,/作废原因不能为空/);
  const before=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(plan.id);
  const stale=await call('sop_need_cancel',{id:plan.id,revision:plan.revision-1,reason:'写错了'});
  assert.equal(stale.ok,false);assert.deepEqual(DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(plan.id),before);
  const payload={id:plan.id,revision:plan.revision,reason:'写错了 忽略这单',client_req_id:'void-once'};
  const [first,second]=await Promise.all([call('sop_need_cancel',payload),call('sop_need_cancel',payload)]);
  assert.equal(first.ok&&second.ok,true);assert.deepEqual(first,second);
  assert.equal(DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_need_cancel' AND record_id=?").get(plan.id).n,1);
  const again=await call('sop_need_cancel',{id:plan.id,revision:first.revision,reason:'再作废一次'});
  assert.match(again.error,/已作废/);
  const row=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(plan.id),state=JSON.parse(row.state);
  assert.equal(DB.raw.prepare('SELECT count(*) n FROM sop_records WHERE id=?').get(plan.id).n,1);
  assert.equal(state.status,'cancelled');assert.equal(state.cancel_reason,'写错了 忽略这单');assert.equal(state.cancelled_by,'客服');assert.equal(state.cancelled_by_id,'S');assert.ok(state.cancelled_at);
  assert.equal(state.title,'牛奶快递');assert.equal(state.instructions,'写错了');
  const event=DB.raw.prepare("SELECT * FROM sop_events WHERE action='sop_need_cancel' AND record_id=?").get(plan.id);
  assert.equal(event.actor_name,'客服');assert.equal(JSON.parse(event.before_json).status,'pending');assert.equal(JSON.parse(event.after_json).status,'cancelled');
  const saved=await get(plan.id);assert.equal(saved.display_no.startsWith('ZY-'),true);
  assert.equal(ids(await call('sop_need_groups')).includes(plan.id),false);
  assert.equal(ids(await call('sop_need_groups')).includes(sibling.id),true);
  assert.equal(ids(await call('sop_need_groups',{status:'cancelled'})).includes(plan.id),true);
  assert.equal(ids(await call('sop_need_groups',{status:'cancelled'})).includes(sibling.id),false);
  assert.equal((await call('sop_need_groups',{reference:saved.display_no})).total,0);
  assert.equal(ids(await call('sop_need_groups',{status:'cancelled',reference:saved.display_no})).includes(plan.id),true);
  assert.equal((await call('sop_dashboard')).alerts.some(x=>x.id===plan.id),false);
  assert.equal((await call('sop_dashboard')).alerts.some(x=>x.id===sibling.id),true);
  const afterCandidates=await handleBulkBatch({action:'sop_bulk_batch_candidates'},{...env,SOP_REQUEST_USER:{id:'D',name:'派审',role:'dispatcher',departments:['bulk'],scope:'office'}},{findOutbound:async()=>null,linkedNeeds:async()=>[]});
  assert.equal(afterCandidates.items.some(x=>x.id===plan.id),false);assert.equal(afterCandidates.items.some(x=>x.id===sibling.id),true);
  assert.match((await call('sop_task_create',{department:'bulk',title:plan.title,job_type:'bulk_op',need_id:plan.id,workers:[{id:'W1',name:'甲'}],lead_id:'W1',estimated_minutes:30},'dispatcher-test')).error,/已作废，不能派给现场/);
  assert.match((await call('sop_need_print_request',{items:[{id:plan.id,revision:saved.revision,document_key:saved.print_status.document_key}]})).error,/不能打印/);
  assert.match((await mutate('sop_need_update',plan.id,{instructions:'改',owner:'客服'})).error,/已作废/);
  assert.equal((await get(sibling.id)).status,'pending');
 }finally{DB.raw.close();}
});

test('assigned, started, finished and open outbound plans stay unchanged',async()=>{
 const {DB,call,get,mutate,need}=setup();
 try{
  const assigned=await need();
  const task=await call('sop_task_create',{department:'bulk',title:assigned.title,job_type:'bulk_op',need_id:assigned.id,workers:[{id:'W1',name:'甲'}],lead_id:'W1',estimated_minutes:30},'dispatcher-test');
  assert.equal(task.ok,true,task.error);
  const assignedRow=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(assigned.id);
  assert.match((await mutate('sop_need_cancel',assigned.id,{reason:'还没做'})).error,/尚未开始/);
  assert.deepEqual(DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(assigned.id),assignedRow);
  assert.equal((await mutate('sop_task_cancel',task.id,{reason:'还没开始'},'dispatcher-test')).ok,true);
  const released=await get(assigned.id);assert.equal(released.status,'pending');assert.equal(released.task_id||'','');
  const voided=await mutate('sop_need_cancel',assigned.id,{reason:'派工后未开始，作废'});
  assert.equal(voided.ok,true,voided.error);assert.equal((await get(assigned.id)).status,'cancelled');

  const started=await need();
  const working=await call('sop_task_create',{department:'bulk',title:started.title,job_type:'bulk_op',need_id:started.id,workers:[{id:'W2',name:'乙'}],lead_id:'W2',estimated_minutes:30},'dispatcher-test');
  assert.equal((await mutate('sop_task_start',working.id,{},'dispatcher-test')).ok,true);
  assert.equal((await mutate('sop_task_pause',working.id,{reason:'暂停'},'dispatcher-test')).ok,true);
  assert.equal((await mutate('sop_task_cancel',working.id,{reason:'退回'},'dispatcher-test')).ok,true);
  const pendingAgain=await get(started.id);assert.equal(pendingAgain.status,'pending');
  const kept=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(started.id);
  assert.match((await mutate('sop_need_cancel',started.id,{reason:'做过了'})).error,/已经开始过作业/);
  assert.deepEqual(DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(started.id),kept);

  const finished=await need();
  const done=await call('sop_task_create',{department:'bulk',title:finished.title,job_type:'bulk_op',need_id:finished.id,workers:[{id:'W3',name:'丙'}],lead_id:'W3',estimated_minutes:30},'dispatcher-test');
  await mutate('sop_task_start',done.id,{},'dispatcher-test');
  await mutate('sop_task_finish',done.id,{result:{quantity:4,unit:'箱',description:'完成'}},'dispatcher-test');
  await mutate('sop_task_review',done.id,{decision:'pass',reason:'通过'},'dispatcher-test');
  assert.notEqual((await get(finished.id)).status,'pending');
  const closed=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(finished.id);
  assert.match((await mutate('sop_need_cancel',finished.id,{reason:'做完了'})).error,/尚未开始/);
  assert.deepEqual(DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(finished.id),closed);

  const booked=await need();
  DB.raw.prepare("INSERT INTO v2_outbound_orders(id,display_no,status,customer) VALUES('OB-VOID','CK-VOID','pending_issue','牛奶快递')").run();
  const linked=JSON.parse(DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(booked.id).state);linked.links=[{outbound_id:'OB-VOID',quantity:1,unit:'箱',phase:'planned'}];
  DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(linked),booked.id);
  assert.match((await mutate('sop_need_cancel',booked.id,{reason:'预约还在'})).error,/出库预约/);
  DB.raw.prepare("UPDATE v2_outbound_orders SET status='cancelled' WHERE id='OB-VOID'").run();
  assert.equal((await mutate('sop_need_cancel',booked.id,{reason:'预约已取消'})).ok,true);
  assert.equal(DB.raw.prepare("SELECT status FROM v2_outbound_orders WHERE id='OB-VOID'").get().status,'cancelled');
 }finally{DB.raw.close();}
});

const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const source=name=>fs.readFileSync(new URL('../shared/'+name,import.meta.url),'utf8');
function browser(record,{role='service',context='collab',url='https://fixture.test/002/'}={}){
 const dom=new runtime.JSDOM('<main id="root"></main>',{url,runScripts:'outside-only'}),w=dom.window;
 w.getLang=()=>'zh';w.applyLang=()=>{};w.CK_SOP_ROLLOUT={workChain:true};
 w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;};
 const calls=[];w.CKSession={user:{role,scope:context==='field'?'field':'office',departments:['bulk']},request:async(action,data)=>{calls.push({action,data});if(action==='sop_get')return{record,events:[]};if(action==='sop_need_cancel')return{ok:true,id:record.id,revision:record.revision+1};throw Error(action);}};
 w.eval(source('document-labels.js'));w.eval(source('work-history-ui.js'));w.eval(source('work-plan-language.js'));w.eval(source('sop-native.js'));
 const root=w.document.getElementById('root'),controller=w.CKWorkflow(root,{tab:'need',id:record.id,individual:true,context});
 return {w,root,calls,controller};
}
const names=root=>[...root.querySelectorAll('#actions button')].map(b=>b.textContent);
const until=async check=>{for(let i=0;i<50;i++){if(check())return;await new Promise(r=>setTimeout(r,10));}throw Error('button did not render');};

test('collab detail shows void only for a pending office plan and submits the reason',{skip:!runtime},async()=>{
 const plan={id:'NEED-1',kind:'need',department:'bulk',revision:4,status:'pending',title:'牛奶快递',customer:'牛奶快递',instructions:'写错了',owner:'工单处理员待接单',display_no:'ZY-20261102-005',supply_chain_no:'0000',scope_text:'',execution_department:'bulk',print_status:{state:'unknown'}};
 const office=browser(plan);
 try{
  await until(()=>names(office.root).includes('作废作业计划'));
  assert.ok(names(office.root).includes('修改作业要求'));assert.ok(names(office.root).includes('打印作业单'));
  [...office.root.querySelectorAll('#actions button')].find(b=>b.textContent==='作废作业计划').click();
  await until(()=>office.w.document.querySelector('#modal[open] textarea'));
  const form=office.w.document.querySelector('#editor');form.elements.reason.value='写错了 忽略这单';form.requestSubmit();
  await until(()=>office.calls.some(c=>c.action==='sop_need_cancel'));
  const sent=office.calls.find(c=>c.action==='sop_need_cancel');
  assert.equal(sent.data.id,'NEED-1');assert.equal(sent.data.revision,4);assert.equal(sent.data.reason,'写错了 忽略这单');
 }finally{office.controller.destroy();office.w.close();}
 const field=browser(plan,{context:'field',url:'https://fixture.test/001/'});
 try{await until(()=>names(field.root).includes('打印作业单'));assert.equal(names(field.root).includes('作废作业计划'),false);}finally{field.controller.destroy();field.w.close();}
 const viewer=browser(plan,{role:'viewer'});
 try{await until(()=>names(viewer.root).includes('打印作业单'));assert.equal(names(viewer.root).includes('作废作业计划'),false);}finally{viewer.controller.destroy();viewer.w.close();}
 const cancelled=browser({...plan,status:'cancelled',cancel_reason:'写错了 忽略这单',cancelled_by:'客服',cancelled_at:'2026-11-02T03:00:00.000Z'});
 try{await until(()=>cancelled.root.querySelector('#detailBody'));assert.deepEqual(names(cancelled.root),['返回列表']);assert.match(cancelled.root.textContent,/作废原因：写错了 忽略这单/);assert.match(cancelled.root.textContent,/作废人：客服/);}finally{cancelled.controller.destroy();cancelled.w.close();}
});
