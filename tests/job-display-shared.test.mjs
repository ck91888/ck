import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=name=>fs.readFileSync(new URL('../shared/'+name+'.js',import.meta.url),'utf8');
const labels=source('document-labels');
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const opts={skip:!runtime};
const id='JOB-01234567-89ab-4cde-8123-456789abcdef',other='JOB-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
function context(lang='zh'){const w={getLang:()=>lang};w.window=w;vm.runInNewContext(labels,w);return w;}
test('shared labels preserve real searchable references and never use internal IDs as labels',()=>{
 const {CKDocumentLabels:l}=context();
 for(const ref of ['WMS-01234567-89ab-4cde-8123-456789abcdef','WMS-WORK-738','JOB-20261008-001','TASK-123','ZY-20261008-003','RW-20261008-009','PICK-20261008-008'])assert.equal(l.jobLabel({id,business_no:ref}),ref);
 assert.equal(l.jobLabel({business_no:id+'、IB-73、OB-99'}),'IB-73、OB-99');
 assert.equal(l.jobLabel({id,business_no:'WMS-01',job_label:'Custom title',display_no:'RW-01'}),'WMS-01');
 assert.equal(l.jobLabel({id,business_no:'',job_label:'盘点 · 大货 · 2026-10-08 08:00 KST',display_no:'RW-20261008-010'}),'盘点 · 大货 · 2026-10-08 08:00 KST');
 for(const raw of [id,'SOPJOB-01234567-89ab-4cde-8123-456789abcdef','JOB-LD-01234567-89ab-4cde-8123-456789abcdef','JOB-mggr0123-abcdefgh']){
  const result=l.jobLabel({id:raw,no:raw,display_no:raw,jobType:'inventory',department:'bulk',start:'2026-10-07T23:00:00Z'});assert.equal(result,'盘点 · 大货 · 2026-10-08 08:00 KST');assert.ok(!result.includes(raw));
 }
 assert.equal(l.jobLabel({jobId:id,jobNo:id,job_type:'bulk_op',job_department:'bulk',job_started_at:'2026-10-08T00:00:00Z'}),'大货操作 · 大货 · 2026-10-08 09:00 KST');
 assert.equal(l.jobLabel({business_no:'RW-20261008-007',display_business_no:'',has_business_reference:false,job_label:'盘点 · 大货 · 2026-10-08 08:00 KST'}),'盘点 · 大货 · 2026-10-08 08:00 KST');
 assert.equal(l.jobLabel({no:'EXT-004'}),'EXT-004');assert.equal(l.jobLabel({jobNo:'EXT-005'}),'EXT-005');
 assert.equal(l.jobType('unknown_machine_type'),'作业');assert.equal(l.jobLabel({id,job_type:'unknown_machine_type',start:'broken date'}),'作业');
});
test('dispatcher is actual saved assignment ownership, never creator, crew, session or unrelated owner',()=>{
 const w=context(),l=w.CKDocumentLabels;w.CKSession={user:{name:'当前登录人员'}};
 assert.equal(l.jobDispatcher({dispatcher_name:'真实派审员',worker_name:'操作员',owner:'其他所属人',created_by:'创建人',lead:{name:'主操作员'}}),'真实派审员');
 for(const x of [{owner:'客户所属'},{kind:'need',owner:'作业计划填写人'},{worker_name:'操作员',created_by:'创建人',lead_id:'EMP-1'},{kind:'task',owner:'旧值',dispatcher_name:''},{kind:'courier',owner:'店铺归属'}])assert.equal(l.jobDispatcher(x),'未记录 / 미기록');
 assert.equal(l.jobDispatcher({kind:'task',owner:'保存的任务派审员'}),'保存的任务派审员');
 assert.equal(l.jobDispatcher({job:{dispatcher_name:'真实派审员'},dispatch:{state:JSON.stringify({owner:'旧值'})}}),'真实派审员');
 assert.match(l.jobSummary({no:'EXT-5',dispatcher_name:'真实派审员'}),/EXT-5 · 派审员：真实派审员/);
});
test('reason codes and type fallbacks are localized while natural notes remain intact',()=>{
 const zh=context().CKDocumentLabels,ko=context('ko').CKDocumentLabels;
 assert.equal(zh.jobLeaveReason('job_completed'),'作业已完成');assert.equal(ko.jobLeaveReason('dispatcher_finish'),'배정·검수 담당자가 작업 종료');assert.equal(zh.jobLeaveReason('dispatcher_change'),'派审员调整人员');
 assert.equal(ko.jobType({jobType:'inventory'}),'재고 조사');assert.equal(ko.jobLabel({job_type:'inventory',department:'bulk',start:'2026-10-07T23:00:00Z'}),'재고 조사 · 대량 · 2026-10-08 08:00 KST');
 for(const reason of ['mystery_code','attendance_stale:checkout','borrow:CB-0001','auto_closed_scan_verify_after_48h'])assert.ok(!zh.jobLeaveReason(reason).includes(reason));
 for(const note of ['检查设备后交接','장비 점검','Lunch break','lunch'])assert.equal(zh.jobLeaveReason(note),note);
 for(const [job_type,job_title,expected] of [['inventory','盘点','재고 조사 · 대량 · 2026-10-08 08:00 KST'],['pack_direct','代发打包 / 직배송 포장','출고대행 포장 · 대량 · 2026-10-08 08:00 KST']])assert.equal(ko.jobLabel({id,business_no:'RW-20261008-001',display_business_no:'',has_business_reference:false,job_type,job_title,job_label:'服务器中文标题',job_department:'bulk',job_started_at:'2026-10-07T23:00:00Z'}),expected);
 assert.equal(ko.jobLabel({display_business_no:'',has_business_reference:false,job_type:'inventory',job_title:'동쪽 선반 확인',job_department:'bulk'}),'재고 조사 · 동쪽 선반 확인 · 대량');
 assert.equal(zh.jobLeaveReason('pause:检查设备'),'作业已暂停 · 检查设备');
});
test('audit maps only the matching job ID; another job never inherits the current business identity',()=>{
 const l=context().CKDocumentLabels,bare='11111111-2222-4333-8444-555555555555',input=JSON.stringify({job_id:id,other_job_id:other,worker_id:bare,external:'JOB-20261008-001'}),snapshot=input;
 const out=l.jobHistoryText(input,{id,business_no:'EXT-123'});assert.equal(out.split('EXT-123').length-1,1);assert.match(out,/关联作业（详情未记录）/);assert.ok(out.includes(bare));assert.match(out,/JOB-20261008-001/);assert.ok(!out.includes(other));assert.equal(input,snapshot);
});
test('status and native task cards escape human labels and dispatchers while keeping click identity',opts,async()=>{
 const dom=new runtime.JSDOM('<main></main>',{runScripts:'outside-only'}),w=dom.window;w.eval(labels);w.eval(source('attendance-ui'));w.eval(source('sop-dispatch-ui'));
 const job={id,business_no:'EXT-<img src=x onerror=alert(1)>',dispatcher_name:'Kim <svg onload=alert(1)>',workers:[{id:'EMP-01',name:'实际操作员'}],job_type:'bulk_op'};
 const root=w.document.querySelector('main');root.innerHTML=w.CKAttendance.statusMarkup({status:'working',currentJobs:[job]});assert.equal(root.querySelector('img,svg'),null);assert.match(root.textContent,/EXT-<img/);assert.match(root.textContent,/派审员：Kim <svg/);assert.ok(!root.textContent.includes(id));
 root.innerHTML=w.CKAttendance.statusMarkup({status:'working',currentJobs:[{id,has_business_reference:false,display_business_no:'',business_no:'RW-20261008-008',job_type:'inventory',job_title:'盘点',job_department:'bulk',department:'bulk',job_started_at:'2026-10-07T23:00:00Z',dispatcher_name:''}]});assert.equal((root.textContent.match(/盘点/g)||[]).length,1);assert.equal((root.textContent.match(/大货/g)||[]).length,1);
 let opened;w.CKOpenNativeJob=async x=>{opened=x;};const button=w.CKNativeTaskButton(job);root.append(button);assert.equal(button.querySelector('img,svg'),null);assert.match(button.textContent,/派审员.*Kim <svg/);assert.match(button.textContent,/作业人员.*实际操作员/);button.click();await new Promise(r=>setTimeout(r,0));assert.equal(opened.id,id);assert.equal(button.disabled,false);w.close();
});
test('work plan print retains its public number and intentionally blank future dispatcher',()=>{
 const w=context();let printed='';w.qrcode=Object.assign(()=>({addData(){},make(){},createSvgTag(){return '<svg></svg>';}}),{stringToBytesFuncs:{'UTF-8':()=>{}}});w.open=()=>({document:{write:value=>{printed=value;},close(){}},focus(){},print(){}});w.CKNeedPrint({id:'NEED-1',display_no:'ZY-20261008-001',source_type:'inventory',title:'计划',owner:'计划填写人',dispatcher_name:'不可猜测'});
 assert.match(printed,/ZY-20261008-001/);assert.match(printed,/派审员：________/);assert.doesNotMatch(printed,/计划填写人|不可猜测/);assert.equal(w.CKDocumentLabels.number({display_no:'RW-20261008-001'}),'RW-20261008-001');
});
test('crew return history presents enriched source and destination jobs with separate real dispatchers',opts,async()=>{
 const dom=new runtime.JSDOM('<section id="page-home"></section>',{url:'https://fixture.test/001/',runScripts:'outside-only'}),w=dom.window;
 w.eval(labels);w.CKSession={user:{id:'EMP-OP'},request:async()=>({items:[{worker_name:'借调操作员',status:'borrowed',source_job_id:id,source_business_no:'PICK-0731',source_dispatcher_name:'原派审员',source_job_type:'pick_direct',destination_job_id:other,destination_business_no:'OB-0911',destination_dispatcher_name:'装货派审员',destination_job_type:'load_outbound'}]})};w.api=async()=>({});w.showPage=()=>{};w.sessionStorage.setItem('ck_crew_jobs:EMP-OP',JSON.stringify([other]));w.eval(source('crew-borrow-ui'));w.CKInstallCrewBorrowUI();await new Promise(r=>setTimeout(r,0));const panel=w.document.querySelector('.ck-crew-returns');assert.match(panel.textContent,/PICK-0731.*派审员：原派审员.*OB-0911.*派审员：装货派审员/);assert.ok(!panel.textContent.includes(id));assert.ok(!panel.textContent.includes(other));w.close();
});
