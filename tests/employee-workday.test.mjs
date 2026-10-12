import {pickStartFixture} from './pick-workload-fixture.mjs';
// Synthetic employees, real Worker handlers, and a controlled SQLite :memory: clock.
import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {handleAttendance} from '../worker-v2/attendance.js';
import {database} from './d1-adapter.mjs';

const NativeDate=Date,day='2026-10-12',origin='https://employee-workday.fixture.invalid';
const instant=(time,date=day)=>new NativeDate(`${date}T${time.length===5?time+':00':time}+09:00`).toISOString();
const zeroOwn={participated:0,completed:0,minutes:0,conflictMinutes:0};
const zeroReviewed={total:0,operationCount:0,passed:0,returned:0};
const subset=(actual,expected)=>{for(const [key,value] of Object.entries(expected))assert.deepEqual(actual?.[key],value,key);};

async function fixture(t){
  let clock=NativeDate.parse(instant('08:00'));
  globalThis.Date=class extends NativeDate{constructor(...args){super(...(args.length?args:[clock]));}static now(){return clock;}};
  const DB=database();t.after(()=>{DB.raw.close();globalThis.Date=NativeDate;});
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:'Synthetic-Workday-Only-Password'};
  const cookies={office:'',field:'',kiosk:''},contexts={};
  const at=(time,date=day)=>{clock=NativeDate.parse(instant(time,date));};
  const call=async(scope,action,data={})=>{
    const path=scope==='field'?'/001/api':scope==='kiosk'?'/attendance/api':'/api';
    const response=await worker.fetch(new Request(origin+path,{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:cookies[scope],'X-CK-Operation-Context':contexts[scope]||''},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
    if(action==='sop_login'&&response.ok)cookies[scope]=response.headers.get('set-cookie')?.split(';')[0]||'';
    const result=await response.json();if(result.user?.operation_context)contexts[scope]=result.user.operation_context.id;
    return result;
  };
  const ok=async(scope,action,data={})=>{const result=await call(scope,action,data);assert.equal(result.ok,true,action+': '+JSON.stringify(result));return result;};
  const officeLogin=async()=>{await ok('office','sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await ok('office','sop_identity');await ok('office','sop_operator_confirm',{name:'合成办公室标签'});};
  await officeLogin();await ok('office','sop_attendance_config');
  const employee=async(code,{name='合成同名职员',checkin=true}={})=>{
    const person=(await ok('office','sop_attendance_employee_register',{name,employeeNo:'SYNTHETIC-'+code,department:'direct_ship'})).person;
    const record=checkin?(await ok('office','sop_attendance_checkin',{badge:person.badgeId})).record:null;return {...person,record};
  };
  const login=person=>ok('field','sop_login',{badge:person.badgeId});
  const start=async(actor,people,{scope='field',codes=['SYNTHETIC-PICK-'+crypto.randomUUID()],request=crypto.randomUUID()}={})=>{
    if(scope==='field')await login(actor);
    const body={payload:{action:'v2_pick_job_start_by_docs',...pickStartFixture('v2_pick_job_start_by_docs'),pick_doc_nos:codes,client_req_id:request},workers:people.map(p=>({id:p.badgeId,name:p.name})),lead_id:people[0].badgeId,estimated_minutes:30};
    return {result:await ok(scope,'sop_native_start',body),body};
  };
  const report=(date=day)=>ok('office','sop_attendance_summary',{date,scope:'employee'});
  const item=async(person,date=day)=>(await report(date)).items.find(x=>x.record.personId===person.id);
  const detail=(person,date=day)=>ok('office','sop_attendance_employee_work_detail',{date,person_id:person.id});
  const rows=table=>DB.raw.prepare('SELECT * FROM "'+table.replaceAll('"','""')+'" ORDER BY rowid').all();
  const snapshot=()=>Object.fromEntries(DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(x=>[x.name,rows(x.name)]));
  const seedJob=({id='SYNTHETIC-JOB-'+crypto.randomUUID(),actor,created=instant('09:00'),status='working',kind='dispatch',action='sop_native_start',department='direct_ship',jobType='pick_direct',state={},withEvent=true,createdBy='UNVERIFIED-CREATOR',jobCreated=created}={})=>{
    const after={owner_id:actor?.id||actor||'',owner:actor?.name||'合成派发标签',status:'working',workers:[],lead_id:'',job_type:jobType,labor_department:department,created_at:created,...state};
    DB.raw.prepare('INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?)').run(id,jobType,department,status,createdBy,jobCreated,instant('10:00'));
    if(withEvent)DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run('SYNTHETIC-CREATE-'+id,id,0,action,actor?.id||actor||'',actor?.name||'合成派发标签','{}',JSON.stringify(after),'{}',created);
    DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(id,kind,1,department,JSON.stringify(after),created);return id;
  };
  const event=(job,action,actor,{at:time=instant('10:00'),after={},before,request=crypto.randomUUID(),result={}}={})=>{
    const row=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(job),old=JSON.parse(row.state),next={...old,...after};
    DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run(request,job,row.revision,action,actor?.id||actor||'',actor?.name||'合成操作者标签',JSON.stringify(before||old),JSON.stringify(next),JSON.stringify(result),time);
    DB.raw.prepare('UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?').run(JSON.stringify(next),time,job);return request;
  };
  const segment=(job,person,{start='09:10',end='09:40',date=day,minutes=999,id='SYNTHETIC-SEGMENT-'+crypto.randomUUID()}={})=>{
    DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(id,job,person.badgeId,person.name,instant(start,date),end?instant(end,date):'',minutes);return id;
  };
  return {DB,env,at,call,ok,employee,login,officeLogin,start,report,item,detail,rows,snapshot,seedJob,event,segment};
}

test('verified field actor and employee worker stay separate even with identical names and two picking documents',async t=>{
  const s=await fixture(t),a=await s.employee('DISPATCHER'),b=await s.employee('WORKER'),c=await s.employee('SAME-NAME');
  s.at('09:10');const {result,body}=await s.start(a,[b],{codes:['SYNTHETIC-PICK-A','SYNTHETIC-PICK-B']});await s.ok('field','sop_native_start',body);s.at('10:00');
  const events=s.rows('sop_events').filter(x=>x.action==='sop_native_start');assert.equal(events.length,1);assert.equal(events[0].actor_id,a.id);
  assert.equal(s.rows('v2_ops_jobs')[0].created_by,b.badgeId);assert.equal(s.rows('v2_ops_job_workers')[0].worker_id,b.badgeId);
  const report=await s.report(),find=p=>report.items.find(x=>x.record.personId===p.id).workday;
  subset(find(a).dispatched,{total:1,inProgress:1,completed:0,carryover:0});subset(find(a).own,zeroOwn);
  subset(find(b).dispatched,{total:0});subset(find(b).own,{participated:1,completed:0,minutes:50,conflictMinutes:0});
  subset(find(c).dispatched,{total:0});subset(find(c).own,zeroOwn);subset(find(a).reviewed,zeroReviewed);
  assert.equal(find(a).documents.total,0,'linked picking sheets are not business documents personally created by the dispatcher');assert.equal(find(a).coverage.identity,'exact');assert.equal(find(a).coverage.officeActorsUnmapped,true);
  const detail=await s.detail(a);assert.deepEqual(detail.workday,find(a));assert.equal(detail.sections.dispatched.length,1);assert.equal(detail.sections.own.length,0);assert.ok(JSON.stringify(detail.sections.dispatched).includes(result.job_id));
});

test('a shared office account with an employee name never receives that employee identity',async t=>{
  const s=await fixture(t),a=await s.employee('LABEL'),b=await s.employee('OFFICE-WORKER');
  await s.ok('office','sop_operator_confirm',{name:a.name});s.at('09:10');await s.start(null,[b],{scope:'office'});s.at('10:00');
  const event=s.rows('sop_events').find(x=>x.action==='sop_native_start');assert.equal(event.actor_id,'ck-office-admin');assert.equal(event.actor_name,a.name);
  subset((await s.item(a)).workday.dispatched,{total:0});subset((await s.item(a)).workday.own,zeroOwn);
  const own=(await s.item(b)).workday;subset(own.coverage,{identity:'exact',officeActorsUnmapped:true});assert.equal(own.own.participated,1);
});

test('first creation events count unique jobs by current job status and exclude ownership, adoption, resume and need mirrors',async t=>{
  const s=await fixture(t),a=await s.employee('STATUS'),other=await s.employee('OTHER-ACTOR');s.at('12:00');
  for(const [status,action,kind] of [['working','sop_native_start','dispatch'],['completed','sop_task_dispatch','task'],['cancelled','sop_task_create','task'],['paused','sop_native_start','dispatch'],['mystery','sop_task_create','task']])s.seedJob({actor:a,status,action,kind});
  const resumed=s.seedJob({actor:a});s.event(resumed,'sop_native_start',a);s.event(resumed,'sop_native_resume',a);s.event(resumed,'sop_native_people',a);
  s.seedJob({actor:a,action:'sop_native_adopt'});s.seedJob({actor:a,withEvent:false,createdBy:a.id});s.seedJob({actor:other,state:{owner_id:a.id}});
  const need='SYNTHETIC-MIRROR-NEED';s.DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run('SYNTHETIC-NEED-DISPATCH',need,0,'sop_task_dispatch',a.id,a.name,'{}','{}','{}',instant('09:00'));s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk','{}',instant('09:00'));
  subset((await s.item(a)).workday.dispatched,{total:6,inProgress:3,completed:1,cancelled:1,other:1,carryover:0});
  const detail=await s.detail(a);assert.equal(detail.sections.dispatched.length,6);assert.equal(detail.sections.carryover.length,0);
});

test('KST creation boundaries use event time, with prior ongoing jobs listed only as carryover',async t=>{
  const s=await fixture(t),a=await s.employee('DAY-EDGE');s.at('23:59:59');await s.officeLogin();
  s.seedJob({actor:a,created:instant('00:00'),jobCreated:instant('23:59:59','2026-10-11')});
  s.seedJob({actor:a,created:instant('23:59:59'),status:'completed'});
  s.seedJob({actor:a,created:instant('00:00','2026-10-13')});
  s.seedJob({actor:a,created:instant('23:59:59','2026-10-11'),status:'working',jobCreated:instant('09:00')});
  s.seedJob({actor:a,created:instant('10:00','2026-10-11'),status:'paused'});
  s.seedJob({actor:a,created:instant('10:00','2026-10-11'),status:'completed'});
  subset((await s.item(a)).workday.dispatched,{total:2,inProgress:1,completed:1,carryover:2});
  const detail=await s.detail(a);assert.equal(detail.sections.dispatched.length,2);assert.equal(detail.sections.carryover.length,2);
});

test('own jobs require real intersecting segments, deduplicate repeated participation and clip attendance rather than raw stored minutes',async t=>{
  const s=await fixture(t),a=await s.employee('PARTICIPANT'),other=await s.employee('OWNER');s.at('12:00');
  const j=s.seedJob({actor:other,status:'completed'});s.segment(j,a,{start:'09:00',end:'10:00'});s.segment(j,a,{start:'10:30',end:'11:00'});
  const working=s.seedJob({actor:other});s.segment(working,a,{start:'11:00',end:'12:00'});
  s.seedJob({actor:a,status:'completed',state:{workers:[{id:a.badgeId,name:a.name}]}});
  const out=s.seedJob({actor:other,status:'completed'});s.segment(out,a,{start:'06:00',end:'07:00'});
  s.DB.raw.prepare('UPDATE ck_attendance_days SET signed_in=?,signed_out=? WHERE id=?').run(instant('09:30'),instant('11:30'),a.record.id);
  const before=s.snapshot(),r=await s.item(a);subset(r.workday.own,{participated:2,completed:1,minutes:90,conflictMinutes:0});assert.deepEqual(s.snapshot(),before);
  const detail=await s.detail(a);assert.equal(detail.sections.own.length,2);assert.ok(JSON.stringify(detail.sections.own).includes(j));assert.ok(JSON.stringify(detail.sections.own).includes(working));
});

test('overlapping departments expose conflict minutes without inventing billable labor',async t=>{
  const s=await fixture(t),a=await s.employee('CONFLICT');s.at('10:00');
  s.segment(s.seedJob({actor:a,department:'bulk',status:'completed'}),a,{start:'09:00',end:'10:00'});
  s.segment(s.seedJob({actor:a,department:'direct_ship',status:'completed'}),a,{start:'09:30',end:'10:00'});
  subset((await s.item(a)).workday.own,{participated:2,completed:2,minutes:30,conflictMinutes:30});
});

test('review occurrences retain returns and re-reviews, exclude mirrored needs, names and native finalization results',async t=>{
  const s=await fixture(t),a=await s.employee('REVIEWER'),other=await s.employee('REVIEW-NAME');s.at('12:00');
  const job=s.seedJob({actor:other,kind:'task',action:'sop_task_create',status:'completed'}),need='SYNTHETIC-REVIEW-NEED';
  s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk','{}',instant('08:00'));
  for(const [action,decision,round] of [['sop_task_review','return',1],['sop_task_review','pass',2],['sop_task_complete_review','pass',3]]){
    const request=crypto.randomUUID(),after={review:{decision,by:a.name,at:instant('10:00'),round}};
    s.event(job,action,a,{request,after});s.event(need,action,a,{request:request+'-need',after});
  }
  s.event(job,'sop_task_review','ck-office-admin',{after:{review:{decision:'pass',by:a.name,at:instant('10:00'),round:4}}});
  s.event(job,'sop_task_review',other,{after:{review:{decision:'pass',by:a.name,at:instant('10:00'),round:5}}});
  s.event(job,'sop_task_finish',a,{after:{review:{decision:'pass',by:a.name,at:instant('10:00')}}});
  s.DB.raw.prepare('INSERT INTO v2_ops_job_results(id,job_id,created_by,created_at,result_json) VALUES(?,?,?,?,?)').run('RES-FINAL-'+job,job,a.id,instant('10:00'),JSON.stringify({finalized_by:a.id}));
  subset((await s.item(a)).workday.reviewed,{total:1,operationCount:3,passed:2,returned:1});
});

test('review dates come from the audit event in KST, not a current review snapshot or job completion date',async t=>{
  const s=await fixture(t),a=await s.employee('REVIEW-DAY');s.at('23:59:59');await s.officeLogin();
  const job=s.seedJob({actor:a,kind:'task',action:'sop_task_create',status:'completed'});
  for(const [at,decision] of [[instant('23:59:59','2026-10-11'),'pass'],[instant('00:00'),'return'],[instant('23:59:59'),'pass'],[instant('00:00','2026-10-13'),'pass']])s.event(job,'sop_task_review',a,{at,after:{review:{decision,at:instant('10:00','2026-10-01'),by:a.name}}});
  s.DB.raw.prepare('UPDATE v2_ops_jobs SET finished_at=? WHERE id=?').run(instant('12:00','2026-10-13'),job);
  subset((await s.item(a)).workday.reviewed,{total:1,operationCount:2,passed:1,returned:1});
});

test('real multi-group reviews count one task and each batch operation once while mirrored events and final wrap-up add nothing',async t=>{
  const s=await fixture(t),a=await s.employee('CARGO-REVIEW');s.env.SOP_WORK_CHAIN_ENABLED='true';await s.login(a);s.at('09:10');
  const need='SYNTHETIC-CARGO-NEED',groups=['SYNTHETIC-GROUP-A','SYNTHETIC-GROUP-B'].map(id=>({id,name:id,mapping_version:1,ack_version:1,box_count:2,documents:[],public_documents:[]}));
  const job=s.seedJob({actor:a,kind:'task',action:'sop_task_dispatch',department:'bulk',jobType:'bulk_op',state:{need_id:need,workers:[{id:a.badgeId,name:a.name}],lead_id:a.badgeId}});
  s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk',JSON.stringify({status:'assigned',customer:'合成分组客户',planned_quantity:4,planned_unit:'箱',task_id:job,cargo_groups:{version:1,groups,public_documents:[]}}),instant('09:00'));
  s.segment(job,a,{start:'09:10',end:'',minutes:0});
  const cargo=(action,data={})=>s.ok('field',action,{id:need,task_id:job,revision:s.DB.raw.prepare('SELECT revision FROM sop_records WHERE id=?').get(need).revision,...data});
  for(const group of groups)await cargo('sop_cargo_group_complete',{group_id:group.id,quantity:2,unit:'箱'});
  s.at('09:30');await cargo('sop_cargo_group_review',{group_ids:groups.map(g=>g.id),decision:'return',reason:'合成两组同次退回'});
  for(const group of groups)await cargo('sop_cargo_group_complete',{group_id:group.id,quantity:2,unit:'箱'});
  s.at('09:50');const request=crypto.randomUUID(),revision=s.DB.raw.prepare('SELECT revision FROM sop_records WHERE id=?').get(need).revision,body={id:need,task_id:job,revision,client_req_id:request,group_ids:groups.map(g=>g.id),decision:'pass',reason:'合成两组同次通过'};
  await s.ok('field','sop_cargo_group_review',body);await s.ok('field','sop_cargo_group_review',body);
  s.at('10:00');await cargo('sop_cargo_job_finish',{counts:{total_operated_box_count:4},reason:'合成已审核分组收尾'});
  assert.equal(s.rows('sop_events').filter(x=>x.action==='sop_cargo_group_review').length,4,'each successful action has a need event and one task mirror');
  subset((await s.item(a)).workday.reviewed,{total:1,operationCount:2,passed:1,returned:1});
});

test('created documents require exact actor evidence and real records, deduplicate mirrors, and retain separate box and pallet quantities',async t=>{
  const s=await fixture(t),a=await s.employee('DOCUMENTS'),other=await s.employee('DOC-SAME-NAME');s.at('12:00');
  const created=instant('09:00'),need='SYNTHETIC-CREATED-NEED',inbound='SYNTHETIC-CREATED-INBOUND',feedback='SYNTHETIC-CREATED-FEEDBACK';
  const outs=['SYNTHETIC-LINKED-CREATED-OB','SYNTHETIC-BATCH-CREATED-OB','SYNTHETIC-RESERVED-OB','SYNTHETIC-RELATED-ONLY-OB'];
  for(const [i,id] of outs.entries())s.DB.raw.prepare('INSERT INTO v2_outbound_orders(id,display_no,status,customer,planned_box_count,planned_pallet_count,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)').run(id,id,'issued','合成创建单据客户',8,2,a.name,i===1||i===2?instant('10:00'):created);
  s.DB.raw.prepare('INSERT INTO v2_inbound_plans(id,display_no,status,customer,created_by,created_at) VALUES(?,?,?,?,?,?)').run(inbound,'SYNTHETIC-INBOUND-NUMBER','pending','合成入库客户',a.name,created);
  s.DB.raw.prepare('INSERT INTO v2_field_feedbacks(id,feedback_type,status,title,submitted_by,created_at) VALUES(?,?,?,?,?,?)').run(feedback,'unplanned_unload','field_working','合成计划外卸货',a.name,created);
  const needState={status:'pending',customer:'合成创建需求客户',planned_quantity:4,planned_unit:'托',links:[{outbound_id:outs[0],quantity:4,unit:'托'}]};
  s.DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run('SYNTHETIC-NEED-CREATION',need,0,'sop_need_create',a.id,a.name,'{}',JSON.stringify(needState),'{}',created);
  s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk',JSON.stringify(needState),created);
  s.event(need,'v2_outbound_order_batch_create',a,{result:{outbounds:[{id:outs[1]},{id:outs[0]},{id:'SYNTHETIC-MISSING-OB'}]}});
  s.event(need,'sop_cargo_reservation_create',a,{after:{links:[...needState.links,{outbound_id:outs[2],quantity:2,unit:'托'}]}});
  s.event(need,'sop_need_ack',a,{after:{links:[...needState.links,{outbound_id:outs[3]}]}});
  const feedbackJob=s.seedJob({actor:a,state:{source_type:'field_feedback',source_id:feedback}});s.DB.raw.prepare("UPDATE v2_field_feedbacks SET related_doc_type='ops_job',related_doc_id=? WHERE id=?").run(feedbackJob,feedback);
  s.seedJob({actor:a,state:{source_type:'field_feedback',source_id:'SYNTHETIC-MISSING-FEEDBACK'}});
  const audit=(doc_type,doc_id,actor,kind='create',time=created)=>s.DB.raw.prepare('INSERT INTO ck_plan_operation_audit VALUES(?,?,?,?,?,?,?)').run('SYNTHETIC-AUDIT-'+crypto.randomUUID(),doc_type,doc_id,kind,actor,a.name,time);
  audit('inbound',inbound,a.id);audit('inbound',inbound,a.id);audit('outbound',outs[0],a.id);audit('outbound',outs[3],a.id,'update');audit('outbound',outs[3],'ck-office-admin');audit('outbound',outs[3],other.id);audit('inbound','SYNTHETIC-MISSING-INBOUND',a.id);
  await s.report();await s.detail(a);const before=s.snapshot(),r=await s.item(a),detail=await s.detail(a);
  assert.equal(r.workday.documents.total,6);assert.equal(detail.sections.documents.length,6);assert.equal((await s.item(other)).workday.documents.total,1);
  assert.deepEqual(new Set(detail.sections.documents.map(d=>d.type+':'+d.id)),new Set(['need:'+need,'inbound:'+inbound,'feedback:'+feedback,...outs.slice(0,3).map(id=>'outbound:'+id)]));
  for(const doc of detail.sections.documents)assert.equal(doc.reference,'current');
  const quantities=detail.sections.documents.find(d=>d.id===outs[0]).quantities;assert.deepEqual(quantities.map(q=>[q.quantity,q.unit]),[[8,'箱'],[2,'托']]);
  assert.deepEqual(s.snapshot(),before);
});

test('linking old outbound orders and unrelated feedback does not claim their creation',async t=>{
  const s=await fixture(t),a=await s.employee('OLD-LINK');s.at('10:00');
  const need='SYNTHETIC-OLD-LINK-NEED',outbound='SYNTHETIC-EXISTING-OUTBOUND',feedback='SYNTHETIC-OLD-FEEDBACK';
  s.DB.raw.prepare('INSERT INTO v2_outbound_orders(id,status,created_at) VALUES(?,?,?)').run(outbound,'issued',instant('08:00'));
  const state={status:'pending',created_at:instant('09:00'),links:[{outbound_id:outbound}]};
  s.DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run('SYNTHETIC-OLD-LINK-EVENT',need,0,'sop_need_create',a.id,a.name,'{}',JSON.stringify(state),'{}',instant('09:00'));
  s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk',JSON.stringify(state),instant('09:00'));
  s.DB.raw.prepare('INSERT INTO v2_field_feedbacks(id,feedback_type,status,related_doc_id,created_at) VALUES(?,?,?,?,?)').run(feedback,'unplanned_unload','open','SYNTHETIC-UNRELATED-JOB',instant('08:00'));
  s.seedJob({actor:a,state:{source_type:'field_feedback',source_id:feedback}});
  const detail=await s.detail(a);assert.equal(detail.workday.documents.total,1);assert.deepEqual(detail.sections.documents.map(d=>d.id),[need]);
});

test('real unload result lines keep carton and pallet quantities as whole-task output for every participating employee',async t=>{
  const s=await fixture(t),a=await s.employee('RESULT-A'),b=await s.employee('RESULT-B');await s.login(a);s.at('09:10');
  const started=await s.ok('field','sop_native_start',{payload:{action:'v2_unplanned_unload_start',client_req_id:crypto.randomUUID(),biz_class:'bulk',customer:'合成卸货成果客户'},workers:[a,b].map(p=>({id:p.badgeId,name:p.name})),lead_id:a.badgeId,estimated_minutes:30,labor_department:'bulk'});
  const lines=[{unit_type:'carton',actual_qty:12},{unit_type:'pallet',actual_qty:2}];s.at('10:00');
  await s.ok('field','v2_unplanned_unload_finish',{job_id:started.job_id,worker_id:a.badgeId,result_lines:lines,complete_job:true});
  await s.report();const before=s.snapshot();
  for(const p of [a,b]){
    const detail=await s.detail(p),job=detail.sections.own.find(j=>j.job_id===started.job_id);subset(detail.workday.own,{participated:1,completed:1,minutes:50,conflictMinutes:0});
    assert.equal(job.results.length,1);assert.equal(job.results[0].scope,'whole_task');assert.deepEqual(job.results[0].result_lines,lines);
    assert.deepEqual(job.results[0].quantities.map(q=>[q.quantity,q.unit]),[[12,'箱'],[2,'托']]);
  }
  assert.deepEqual(s.snapshot(),before);assert.equal((await s.item(a)).workday.documents.total,1);assert.equal((await s.item(b)).workday.documents.total,0);
});

test('creation proof uses the saved document and link time when the audit event is written later, without a time tolerance for older orders',async t=>{
  const s=await fixture(t),a=await s.employee('CREATION-TIMES');s.at('12:00');
  const need='SYNTHETIC-TIMED-NEED',created=instant('09:00:00.000'),reserved=instant('10:00:00.000');
  const orders=[['SYNTHETIC-TIMED-NEW',created],['SYNTHETIC-TIMED-OLD',instant('08:59:59.999')],['SYNTHETIC-RESERVED-NEW',reserved],['SYNTHETIC-RESERVED-OLD',instant('09:59:59.999')]];
  for(const [id,time] of orders)s.DB.raw.prepare('INSERT INTO v2_outbound_orders(id,status,created_at) VALUES(?,?,?)').run(id,'issued',time);
  const state={status:'pending',created_at:created,links:orders.slice(0,2).map(([outbound_id])=>({outbound_id,at:created}))};
  s.DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run('SYNTHETIC-TIMED-CREATE',need,0,'sop_need_create',a.id,a.name,'{}',JSON.stringify(state),'{}',instant('09:00:00.123'));
  s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk',JSON.stringify(state),created);
  s.event(need,'sop_cargo_reservation_create',a,{at:instant('10:00:00.456'),after:{links:[...state.links,...orders.slice(2).map(([outbound_id])=>({outbound_id,at:reserved}))]}});
  const detail=await s.detail(a);
  assert.equal(detail.workday.documents.total,3);
  assert.deepEqual(new Set(detail.sections.documents.map(d=>d.id)),new Set([need,orders[0][0],orders[2][0]]));
  assert.equal(detail.sections.documents.find(d=>d.id===orders[0][0]).created_at,instant('09:00:00.123'));
  assert.equal(detail.sections.documents.find(d=>d.id===orders[2][0]).created_at,instant('10:00:00.456'));
});

test('explicit zero result lines remain zero and bulk JSON and process outputs retain their own units and labels',async t=>{
  const s=await fixture(t),a=await s.employee('RESULT-SHAPES');s.at('10:00');
  const zero=s.seedJob({actor:a,status:'completed'}),bulk=s.seedJob({actor:a,status:'completed',department:'bulk',jobType:'bulk_op'});
  const result=(id,job,box,pallet,lines,data)=>s.DB.raw.prepare('INSERT INTO v2_ops_job_results(id,job_id,created_at,box_count,pallet_count,result_lines_json,result_json) VALUES(?,?,?,?,?,?,?)').run(id,job,instant('09:40'),box,pallet,JSON.stringify(lines),JSON.stringify(data));
  result('SYNTHETIC-ZERO-RESULT',zero,99,99,[{unit_type:'carton',actual_qty:0},{unit_type:'pallet',actual_qty:2}],{});
  result('SYNTHETIC-BULK-RESULT',bulk,0,0,[],{packed_box_count:7,total_operated_box_count:12,label_count:90,repaired_box_count:3,reboxed_count:2,pallet_count:4,group_outputs:[],process_results:[{quantity:5,unit:'袋'}]});
  const detail=await s.detail(a),facts=new Map(detail.sections.dispatched.map(j=>[j.job_id,j.results[0]]));
  assert.deepEqual(facts.get(zero).quantities.map(q=>[q.quantity,q.unit]),[[0,'箱'],[2,'托']]);
  assert.deepEqual(facts.get(bulk).quantities.map(q=>[q.quantity,q.unit,q.label]),[[4,'托','整任务实收/完成'],[7,'箱','整任务打包'],[12,'箱','整任务操作'],[90,'张','整任务贴标'],[3,'箱','整任务修箱'],[2,'箱','整任务换箱'],[5,'袋','整任务分组成果']]);
  for(const fact of facts.values())assert.equal(fact.scope,'whole_task');
});

test('job detail derives customers from the actual need, direct plan and every linked load or unload plan',async t=>{
  const s=await fixture(t),a=await s.employee('CUSTOMERS');s.at('10:00');const time=instant('09:00');
  const inboundIds=['SYNTHETIC-CUSTOMER-INBOUND-DIRECT','SYNTHETIC-CUSTOMER-INBOUND-A','SYNTHETIC-CUSTOMER-INBOUND-B'];
  for(const [i,id] of inboundIds.entries())s.DB.raw.prepare('INSERT INTO v2_inbound_plans(id,display_no,status,customer,created_at,updated_at) VALUES(?,?,?,?,?,?)').run(id,id,'pending','合成入库客户'+i,time,time);
  const outboundIds=['SYNTHETIC-CUSTOMER-OUTBOUND-A','SYNTHETIC-CUSTOMER-OUTBOUND-B'];
  for(const [i,id] of outboundIds.entries())s.DB.raw.prepare('INSERT INTO v2_outbound_orders(id,display_no,status,customer,created_at,updated_at) VALUES(?,?,?,?,?,?)').run(id,id,'issued','合成出库客户'+i,time,time);
  const need='SYNTHETIC-CUSTOMER-NEED';s.DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run(need,'need',1,'bulk',JSON.stringify({status:'assigned',customer:'合成需求客户'}),time);
  const task=s.seedJob({actor:a,kind:'task',action:'sop_task_create',state:{need_id:need}}),direct=s.seedJob({actor:a,jobType:'inbound_direct'}),unload=s.seedJob({actor:a,jobType:'unload'}),load=s.seedJob({actor:a,jobType:'load_outbound'});
  s.DB.raw.prepare("UPDATE v2_ops_jobs SET related_doc_type='inbound_plan',related_doc_id=? WHERE id=?").run(inboundIds[0],direct);
  for(const [i,plan] of inboundIds.slice(1).entries())s.DB.raw.prepare('INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot) VALUES(?,?,?,?,?)').run(unload,plan,i,time,'[]');
  for(const [i,order] of outboundIds.entries())s.DB.raw.prepare('INSERT INTO ck_load_order_links(job_id,order_id,position,snapshot_json) VALUES(?,?,?,?)').run(load,order,i,JSON.stringify({order_version:time,revision_no:0,needs:[]}));
  const detail=await s.detail(a),byId=new Map(detail.sections.dispatched.map(j=>[j.job_id,j]));
  assert.deepEqual(byId.get(task).customers,['合成需求客户']);assert.deepEqual(byId.get(direct).customers,['合成入库客户0']);
  assert.deepEqual(new Set(byId.get(unload).customers),new Set(['合成入库客户1','合成入库客户2']));assert.deepEqual(new Set(byId.get(load).customers),new Set(['合成出库客户0','合成出库客户1']));
});

for(const missing of ['sop_events','ck_plan_operation_audit','sop_document_numbers','v2_ops_job_results'])test(`historical database without optional ${missing} still reports real personal work without creating replacement tables`,async t=>{
  const s=await fixture(t),a=await s.employee('OLD-SCHEMA');s.at('10:00');const job=s.seedJob({actor:a,status:'completed'});s.segment(job,a,{start:'09:00',end:'10:00',minutes:60});
  await s.report();await s.detail(a);s.DB.raw.exec('DROP TABLE '+missing);const before=s.snapshot();
  const r=await s.item(a),detail=await s.detail(a);subset(r.workday.own,{participated:1,completed:1,minutes:60,conflictMinutes:0});assert.equal(detail.sections.own.length,1);assert.equal(detail.sections.own[0].job_id,job);
  if(missing==='sop_events'){assert.equal(r.workday.dispatched.total,null);assert.equal(r.workday.reviewed.total,null);assert.equal(r.workday.coverage.dispatchAuditAvailable,false);}else assert.equal(r.workday.dispatched.total,1);
  if(missing==='v2_ops_job_results')assert.deepEqual(detail.sections.own[0].results,[]);
  assert.equal(s.DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(missing),undefined);assert.deepEqual(s.snapshot(),before);
});

test('summary and employee detail are read-only and share the same as-of metrics',async t=>{
  const s=await fixture(t),a=await s.employee('READ-ONLY');s.at('09:10');await s.start(a,[a]);s.at('10:00');
  await s.report();await s.detail(a);const before=s.snapshot(),r=await s.item(a),d=await s.detail(a);
  assert.equal(d.date,day);assert.equal(d.asOf,instant('10:00'));assert.equal(d.record.personId,a.id);assert.deepEqual(d.workday,r.workday);
  assert.deepEqual(Object.keys(d.sections).sort(),['carryover','dispatched','documents','own','reviewed']);assert.ok(Array.isArray(d.flags));assert.deepEqual(s.snapshot(),before);
});

for(const role of ['manager','dispatcher','reviewer','viewer'])test(`legacy ${role} and office ${role} can read employee detail`,async t=>{
  const s=await fixture(t),a=await s.employee('ROLE-'+role);s.at('10:00');
  for(const scope of [undefined,'office']){
    const result=await handleAttendance({action:'sop_attendance_employee_work_detail',date:day,person_id:a.id},{...s.env,SOP_ACCESS_CONTROL:scope?'true':'false',SOP_REQUEST_USER:{id:'SYNTHETIC-READER',name:'合成只读人员',role,...(scope?{scope}:{})}});
    assert.equal(result.ok,true);assert.equal(result.record.personId,a.id);
  }
});

for(const [role,scope] of [['dispatcher','field'],['manager','field'],['kiosk','kiosk'],['manager','kiosk'],['service','office']])test(`${scope} ${role} cannot obtain employee detail even through the handler`,async t=>{
  const s=await fixture(t),a=await s.employee('DENIED');s.at('10:00');const before=s.snapshot();
  await assert.rejects(()=>handleAttendance({action:'sop_attendance_employee_work_detail',date:day,person_id:a.id},{...s.env,SOP_REQUEST_USER:{id:a.id,badge:a.badgeId,name:a.name,role,scope}}),/权限|办公室|office|출근|권한/i);
  assert.deepEqual(s.snapshot(),before);
});

test('field API and unrecognized employee identity cannot enumerate work detail',async t=>{
  const s=await fixture(t),a=await s.employee('DETAIL-ACCESS');await s.login(a);
  assert.equal((await s.call('field','sop_attendance_employee_work_detail',{date:day,person_id:a.id})).ok,false);
  assert.equal((await s.call('office','sop_attendance_employee_work_detail',{date:day,person_id:'SYNTHETIC-MISSING-PERSON'})).ok,false);
  assert.equal((await s.call('office','sop_attendance_employee_work_detail',{date:day,person_id:a.badgeId})).ok,false,'employee badge is not a person ID');
});

test('employee summary uses a constant number of database reads as the employee roster grows',async t=>{
  const s=await fixture(t),a=await s.employee('QUERY-ONE');s.at('10:00');s.seedJob({actor:a});await s.report();
  let counts,queries,measuring=false;const prepare=s.DB.prepare.bind(s.DB),batch=s.DB.batch.bind(s.DB);
  s.DB.prepare=sql=>{if(measuring)counts.prepare++;const statement=prepare(sql);for(const method of ['first','all','run']){const original=statement[method];statement[method]=async(...args)=>{if(measuring){counts.execute++;queries.push({sql,args:[...statement.args]});}return original.apply(statement,args);};}return statement;};
  s.DB.batch=async statements=>{if(measuring){counts.execute+=statements.length;counts.batches++;for(const st of statements)queries.push({sql:st.sql,args:[...st.args]});}return batch(statements);};
  const measure=async()=>{counts={prepare:0,execute:0,batches:0};queries=[];measuring=true;try{const report=await s.report();return {...counts,employees:report.items.length,queries};}finally{measuring=false;}};
  const small=await measure();for(let i=0;i<49;i++){const p=await s.employee('QUERY-'+i);s.seedJob({actor:p});}
  await s.report();const large=await measure();assert.equal(small.employees,1);assert.equal(large.employees,50);
  subset(large,{prepare:small.prepare,execute:small.execute,batches:small.batches});
  const source=large.queries.find(q=>q.sql.includes('FROM sop_events e JOIN sop_records s'));assert.ok(source);
  const plan=s.DB.raw.prepare('EXPLAIN QUERY PLAN '+source.sql).all(...source.args);assert.ok(plan.some(row=>row.detail.includes('ck_employee_workday_actor_time')));
  t.diagnostic(JSON.stringify({employees:[1,50],prepare:[small.prepare,large.prepare],execute:[small.execute,large.execute],batches:[small.batches,large.batches],actorQueryPlan:plan.filter(row=>row.detail.includes('ck_employee_workday_actor_time')).map(row=>row.detail)}));
});
