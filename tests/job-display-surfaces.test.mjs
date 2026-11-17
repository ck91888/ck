import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null,opts={skip:!runtime};
const id='JOB-11111111-2222-4333-8444-555555555555',otherId='JOB-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const source=name=>fs.readFileSync(new URL('../shared/'+name,import.meta.url),'utf8');
const until=async check=>{for(let i=0;i<100;i++){if(check())return;await new Promise(r=>setTimeout(r,5));}throw Error('DOM state did not become ready');};
function fixture(language='zh'){
 const dom=new runtime.JSDOM('<main id="root"></main>',{url:'https://fixture.test/001/',runScripts:'outside-only'}),w=dom.window;
 w.getLang=()=>language;w.applyLang=()=>{};w.CKAttendance={at:value=>value};w.goPage=()=>{};w.eval(source('document-labels.js'));
 w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};w.HTMLDialogElement.prototype.close=function(){this.open=false;};
 return {w,dom,root:w.document.getElementById('root')};
}
test('external field cards and headers show canonical dispatcher and business code while actions retain internal IDs',opts,async()=>{
 const {w,root}=fixture(),calls=[],changed=[];
 const job={id,job_type:'bulk_op',business_no:'EXT-20261008-004',display_no:id,related_doc_type:'work_order',related_doc_id:'EXT-20261008-004',status:'working',created_at:'2026-10-07T16:00:00Z',dispatcher_name:'实际派审员 <甲>',customer:'Fixture customer'};
 const detail={can_manage_dispatch:true,job,dispatch:{revision:7,state:JSON.stringify({owner:'旧派审员',lead_id:'EMP-1',workers:[{id:'EMP-1',name:'操作员'}]})},workers:[{worker_id:'EMP-1',worker_name:'操作员',left_at:''}],results:[]};
 w.CKSession={request:async(action,data)=>{calls.push({action,data});if(action==='sop_dispatch_list')return{items:[{...job,task_kind:'dispatch',workers:[{id:'EMP-1',name:'操作员'}]}]};if(action==='v2_ops_job_detail')return detail;if(action==='sop_native_pause')return{};throw Error('Unexpected request '+action);}};
 w.eval(source('native-lifecycle-ui.js'));w.eval(source('field-work.js'));const controller=w.CKFieldWork(root,'',{external:true,onChange:t=>changed.push(t)});
 try{
  await until(()=>root.querySelector('[data-task]'));const card=root.querySelector('[data-task]');assert.equal(card.dataset.task,id);assert.match(card.textContent,/EXT-20261008-004/);assert.match(card.textContent,/派审员.*实际派审员 <甲>/);assert.ok(!card.textContent.includes(id));assert.equal(card.querySelector('甲'),null);
  card.click();await until(()=>root.querySelector('.ck-work-dispatcher'));assert.match(root.textContent,/EXT-20261008-004/);assert.match(root.querySelector('.ck-work-dispatcher').textContent,/实际派审员 <甲>/);assert.ok(!root.textContent.includes(id));
  const current=changed.at(-1);assert.equal(current.id,id);assert.equal(current.job_type,'bulk_op');assert.equal(current.business_no,job.business_no);assert.equal(current.dispatcher_name,job.dispatcher_name);
  [...root.querySelectorAll('button')].find(b=>b.textContent==='暂停整个任务 / 전체 작업 중지').click();await until(()=>w.document.querySelector('dialog form'));const pause=w.document.querySelector('dialog form');pause.elements.reason.value='检查设备';pause.requestSubmit();await until(()=>calls.some(c=>c.action==='sop_native_pause'));
  const mutation=calls.find(c=>c.action==='sop_native_pause');assert.equal(mutation.data.job_id,id);assert.equal(mutation.data.revision,7);assert.equal(mutation.data.reason,'检查设备');assert.equal(mutation.data.workers,undefined);
  job.dispatcher_name='';await controller.open(id,detail);assert.match(root.querySelector('.ck-work-dispatcher').textContent,/未记录/);assert.ok(!root.querySelector('.ck-work-dispatcher').textContent.includes('旧派审员'));
 }finally{controller.destroy();w.close();}
});
test('work-plan field number remains its plan number and does not mistake an operator for dispatcher',opts,async()=>{
 const {w,root}=fixture('ko'),task={id,kind:'task',job_type:'bulk_op',status:'completed',display_no:id,work_plan_no:'ZY-20261008-003',worker_name:'작업자',created_by:'작성자',workers:[],lead_id:''};
 w.CKSession={request:async()=>({need:{id:'NEED-fixture',display_no:'ZY-20261008-003',title:'작업 계획',source_type:'inventory'},task,source:null,segments:[]})};w.CKResultSummary=()=>'';w.CKResultPhotos=()=>'';
 w.eval(source('native-lifecycle-ui.js'));w.eval(source('field-work.js'));const controller=w.CKFieldWork(root,id);
 try{await until(()=>root.querySelector('.ck-work-dispatcher'));assert.match(root.querySelector('.ck-work-number').textContent,/ZY-20261008-003/);assert.match(root.querySelector('.ck-work-dispatcher').textContent,/미기록/);assert.ok(!root.querySelector('.ck-work-dispatcher').textContent.includes('작업자'));assert.ok(!root.textContent.includes(id));}finally{controller.destroy();w.close();}
});
test('job audit handles unknown events and nested references without leaking internal IDs or changing source events',opts,()=>{
 const {w,root}=fixture();w.eval(source('work-history-ui.js'));
 const record={id,kind:'task',job_type:'bulk_op',business_no:'ZY-20261008-003',dispatcher_name:'实际派审员'};
 const events=[{action:'sop_unknown_new_action',actor_name:'审核人员',created_at:'2026-10-08T01:00:00Z',before_json:JSON.stringify({job_id:id,job_type:'bulk_op',status:'working',result:{description:'接续 '+otherId},dispatcher_name:'先前派审员'}),after_json:JSON.stringify({job_id:otherId,job_type:'bulk_op',status:'completed',result:{description:'已核实 '+otherId+'，外部单 JOB-20261008-009'},title:'本次 '+id,dispatcher_name:'实际派审员',leave_reason:'borrow_return'})}];
 const snapshot=JSON.stringify(events);w.CKWorkHistory(root,events,record);
 assert.ok(!root.textContent.includes(id));assert.ok(!root.textContent.includes(otherId));assert.ok(!root.textContent.includes('sop_unknown_new_action'));assert.ok(!root.textContent.includes('bulk_op'));assert.match(root.textContent,/JOB-20261008-009/);assert.match(root.textContent,/派审员/);assert.match(root.textContent,/实际派审员/);assert.equal(JSON.stringify(events),snapshot);w.close();
});
test('native task list and detail keep IDs in API calls and redact legacy nested audit JSON',opts,async()=>{
 const {w,root}=fixture(),calls=[],task={id,kind:'task',department:'bulk',job_type:'bulk_op',title:'核对 '+id,business_no:'ZY-20261008-005',display_no:id,status:'completed',owner:'真正派审员',workers:[],estimated_minutes:10,round:1,revision:3};
 w.CKSession={user:{role:'viewer'},request:async(action,data)=>{calls.push({action,data});if(action==='sop_list')return{items:[task],more:false};if(action==='sop_get')return{record:task,events:[{actor_name:'审核',action:'legacy',before_json:JSON.stringify({nested:{job_id:id,note:otherId}}),after_json:JSON.stringify({business_no:'JOB-20261008-009'})}]};throw Error(action);}};
 w.eval(source('sop-native.js'));const controller=w.CKWorkflow(root,{tab:'task'});
 try{await until(()=>root.querySelector('#content article'));assert.match(root.textContent,/ZY-20261008-005/);assert.match(root.textContent,/派审员.*真正派审员/);assert.ok(!root.textContent.includes(id));[...root.querySelectorAll('button')].find(b=>b.textContent==='打开 / 열기').click();await until(()=>root.querySelector('#detailBody'));assert.equal(calls.find(c=>c.action==='sop_get').data.id,id);assert.ok(!root.textContent.includes(id));assert.ok(!root.textContent.includes(otherId));assert.match(root.textContent,/JOB-20261008-009/);}finally{controller.destroy();w.close();}
});
test('native dashboard and CSV present job labels and real dispatchers with non-job owners preserved',opts,async()=>{
 const {w,root}=fixture(),blobs=[],task={id,kind:'task',department:'bulk',job_type:'bulk_op',title:'核对 '+id,work_plan_no:'ZY-20261008-005',status:'paused',owner:'真正派审员'},need={id:'NEED-f',kind:'need',department:'bulk',title:'补货',display_no:'ZY-20261008-006',status:'pending',owner:'计划负责人'};
 w.CKSession={user:{role:'viewer'},request:async()=>({date:'2026-10-08',scope:'Fixture',active_people:1,working:1,completed:0,person_hours:1,outputs:{'bulk / bulk_op / 箱':5},data_quality:[{...task,reason:'核实 '+otherId}],rankings:[{department:'bulk',job_type:'bulk_op',metric:'完成量',unit:'箱',quantity:5,worker_name:'操作员',tasks:[id]}],reported_outputs:[],roster:[{worker_name:'操作员',status:'作业中',current_job_id:id,business_no:'EXT-20261008-008',current_task:id,dispatcher_name:'当前派审员'}],alerts:[task,need]})};
 w.Blob=class{constructor(parts){this.text=parts.join('');blobs.push(this);}};w.URL.createObjectURL=()=> 'blob:fixture';w.URL.revokeObjectURL=()=>{};w.HTMLAnchorElement.prototype.click=function(){};
 w.eval(source('sop-native.js'));const controller=w.CKWorkflow(root,{tab:'dashboard'});
 try{await until(()=>root.querySelector('#livePeople'));assert.ok(!root.textContent.includes(id));assert.ok(!root.textContent.includes(otherId));assert.ok(!root.textContent.includes('bulk_op'));assert.match(root.querySelector('#livePeople').textContent,/EXT-20261008-008.*派审员.*当前派审员/);assert.match(root.querySelector('#alerts').textContent,/真正派审员/);assert.match(root.querySelector('#alerts').textContent,/计划负责人/);
  [...root.querySelectorAll('button')].find(b=>b.textContent==='导出今日待办日报 CSV').click();await until(()=>blobs.length);assert.ok(!blobs[0].text.includes(id));assert.match(blobs[0].text,/ZY-20261008-005/);assert.match(blobs[0].text,/真正派审员/);assert.match(blobs[0].text,/计划负责人/);assert.match(blobs[0].text,/派审员/);
 }finally{controller.destroy();w.close();}
});

test('audit owner changes preserve historical dispatchers rather than relabeling them with today’s dispatcher',opts,()=>{
 const {w,root}=fixture();w.eval(source('work-history-ui.js'));w.CKWorkHistory(root,[{action:'sop_task_people',before_json:JSON.stringify({owner:'原派审员'}),after_json:JSON.stringify({owner:'新派审员'})}],{id,kind:'task',business_no:'ZY-20261008-003',title:'调整人员',dispatcher_name:'当前派审员'});
 const change=[...root.querySelectorAll('.ck-audit-change')].find(x=>x.querySelector('strong').textContent==='派审员');assert.match(change.textContent,/原派审员/);assert.match(change.textContent,/新派审员/);assert.ok(!change.textContent.includes('当前派审员'));w.close();
});
