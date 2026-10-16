// Display-only regression fixtures. All data lives in isolated SQLite; no external services.
import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import worker from '../worker-v2/index.js';
import {handleSop} from '../worker-v2/sop.js';
import {handleAttendance} from '../worker-v2/attendance.js';
import {jobNumbers,jobDisplayMetadata,businessReference} from '../worker-v2/document-numbers.js';
import {jobDefinitions} from '../shared/labor-department.js';
import {ensureLoadSchema} from '../worker-v2/outbound-load-trip.js';
import {ensureCourierBatches,courierBatchList,courierBatchDetail} from '../worker-v2/courier-batches.js';
import {ensureCrewBorrow,crewAvailability,crewStatus} from '../worker-v2/crew-borrow.js';
const stamp='2026-10-07T15:07:00.000Z',owner={id:'DISPATCH-FIXTURE',name:'派审员金',role:'manager',departments:['bulk','direct_ship','import']};
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_REQUEST_USER:{...owner}};
 await ensureLoadSchema(env);
 const api=async(action,data={})=>{const r=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...data})}),env);const out=await r.json();assert.equal(out.ok,true,action+': '+(out.error||out.message));return out;};
 const sop=async(action,data={})=>{const out=await handleSop({action,client_req_id:crypto.randomUUID(),...data},env);assert.equal(out.ok,true,action+': '+out.error);return out;};
 function job(id,type='pack_direct',fields={}){const data={id,job_type:type,biz_class:'direct_ship',flow_stage:'order_op',status:'working',created_by:'LEAD-FIXTURE',created_at:stamp,updated_at:stamp,active_worker_count:1,...fields},keys=Object.keys(data);DB.raw.prepare('INSERT INTO v2_ops_jobs('+keys.join(',')+') VALUES('+keys.map(()=>'?').join(',')+')').run(...keys.map(k=>data[k]));return id;}
 function record(id,kind='dispatch',data={},department='direct_ship'){const state={created_at:stamp,owner:owner.name,owner_id:owner.id,title:'',status:'working',lead_id:'LEAD-FIXTURE',workers:[{id:'LEAD-FIXTURE',name:'主操作员朴'}],...data};DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,1,?,?,?)').run(id,kind,department,JSON.stringify(state),stamp);return state;}
 function segment(jobId,workerId='LEAD-FIXTURE',start=stamp,end=''){const id='WS-'+crypto.randomUUID();DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,leave_reason) VALUES(?,?,?,?,?,?,?)').run(id,jobId,workerId,'主操作员朴',start,end,end?'job_completed':'');return id;}
 const snapshot=()=>Object.fromEntries(['v2_ops_jobs','sop_records','v2_ops_job_workers','sop_document_numbers'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 return {DB,env,api,sop,job,record,segment,snapshot};
}

test('linked fields preserve external business codes, leading zeros and meaningful JOB prefixes without leaking immutable IDs',async t=>{
 const f=await fixture(t),id='JOB-018d3fd1-2ab3-4e3b-8d55-b6fead37ef92';
 for(const [key,number] of [['plain','0000123'],['job-prefix','JOB-20261008-001'],['task-prefix','TASK-123'],['external-uuid','WMS-018d3fd1-2ab3-4e3b-8d55-b6fead37ef92']]){
  f.job(key,'bulk_op',{related_doc_type:'work_order',related_doc_id:number});const row=(await jobNumbers(f.env,[{id:key}]))[0];
  assert.equal(row.business_no,number);assert.equal(row.display_business_no,number);assert.equal(row.has_business_reference,true);assert.ok(row.job_label.startsWith(number+' · '));
 }
 f.job(id,'pack_direct',{display_no:id});f.record(id);const before=f.snapshot(),row=(await jobNumbers(f.env,[{id}]))[0];
 assert.equal(row.id,id);assert.equal(row.display_business_no,'');assert.equal(row.has_business_reference,false);assert.match(row.business_no,/^RW-/);assert.equal(row.task_no,row.business_no);
 assert.match(row.job_label,/代发打包.*代发.*2026-10-08 00:07 KST/);assert.doesNotMatch(row.job_label,/JOB-|RW-/);assert.deepEqual(f.snapshot(),before);
 assert.equal(businessReference('SOPJOB-018d3fd1-2ab3-4e3b-8d55-b6fead37ef92'),'');assert.equal(businessReference('JOB-mg99abcd-ab12cd34'),'');
 assert.equal(jobDisplayMetadata({id:'JOB-fixture',business_no:'JOB-fixture',job_type:'pack_direct'}).display_business_no,'');
});

test('every known job type has a meaningful department/start fallback; missing dispatchers never borrow creator, lead or current identity',async t=>{
 const f=await fixture(t);
 for(const type of Object.keys(jobDefinitions)){f.job('J-'+type,type,{biz_class:'bulk'});}
 const rows=await jobNumbers(f.env,Object.keys(jobDefinitions).map(type=>({id:'J-'+type,owner:'客服计划人',created_by:'主操作员朴',dispatcher_name:'当前办公室李'})));
 for(const row of rows){assert.ok(row.job_label);assert.match(row.job_label,/2026-10-08 00:07 KST/);assert.equal(row.dispatcher_name,'');assert.equal(row.dispatcher_id,'');assert.equal(row.has_business_reference,false);}
 f.job('legacy','inventory');f.record('legacy','need',{owner:'客服计划人',owner_id:'SERVICE'});
 const legacy=(await jobNumbers(f.env,[{id:'legacy'}]))[0];assert.equal(legacy.dispatcher_name,'');assert.equal(legacy.dispatcher_id,'');
 f.job('assigned','inventory');f.record('assigned','dispatch',{labor_department:'import'});
 f.env.SOP_REQUEST_USER={id:'OFFICE',name:'当前办公室李',role:'manager'};
 const row=(await jobNumbers(f.env,[{id:'assigned',created_by:'主操作员朴',owner:'客服计划人'}]))[0];assert.equal(row.dispatcher_name,'派审员金');assert.equal(row.dispatcher_id,'DISPATCH-FIXTURE');assert.equal(row.job_department,'import');assert.match(row.job_label,/进口/);
 assert.equal(jobDisplayMetadata({job_type:'unknown_machine_code'}).job_title,'作业 / 작업');
});

test('same-truck unload and load labels include every linked public order in stable position order and are searchable',async t=>{
 const f=await fixture(t);
 f.DB.raw.exec("INSERT INTO v2_inbound_plans(id,display_no,status,updated_at) VALUES('IB-one','RU-20261008-001','pending','v1'),('IB-two','RU-20261008-002','pending','v1'); INSERT INTO v2_outbound_orders(id,display_no,status,updated_at) VALUES('OB-one','CHU-FIRST','issued','v1'),('OB-two','CHU-SECOND','issued','v1')");
 f.job('UNLOAD','unload',{related_doc_type:'inbound_plan',related_doc_id:'IB-one'});f.record('UNLOAD','dispatch',{},'bulk');
 f.DB.raw.exec("INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot) VALUES('UNLOAD','IB-two',2,'v1','[]'),('UNLOAD','IB-one',1,'v1','[]')");
 f.job('LOAD','load_outbound',{related_doc_type:'outbound_order',related_doc_id:'OB-one'});f.record('LOAD','dispatch',{},'bulk');
 const snapshot=JSON.stringify({order_version:'v1',revision_no:0,needs:[]});
 for(const [id,position] of [['OB-two',2],['OB-one',1]])f.DB.raw.prepare('INSERT INTO ck_load_order_links(job_id,order_id,position,snapshot_json) VALUES(?,?,?,?)').run('LOAD',id,position,snapshot);
 const rows=await jobNumbers(f.env,[{id:'UNLOAD'},{id:'LOAD'}]);assert.equal(rows[0].display_business_no,'RU-20261008-001、RU-20261008-002');assert.equal(rows[0].inbound_plan_no,rows[0].display_business_no);assert.equal(rows[1].display_business_no,'CHU-FIRST、CHU-SECOND');assert.equal(rows[1].outbound_plan_no,rows[1].display_business_no);
 for(const [code,id] of [['RU-20261008-002','UNLOAD'],['CHU-SECOND','LOAD']]){assert.deepEqual((await f.api('v2_dashboard_order_list',{doc_no:code})).items.map(x=>x.id),[id]);assert.equal((await f.api('v2_dashboard_order_export',{doc_no:code})).rows[0].job_id,id);}
});

test('external inbound, pick documents/trip and genuine courier batch numbers come from their own sources',async t=>{
 const f=await fixture(t);await ensureCourierBatches(f.env);
 f.DB.raw.exec("INSERT INTO v2_inbound_plans(id,display_no,status,external_inbound_no) VALUES('IB','RU-20261008-009','pending','0000077')");
 f.job('IN','inbound_direct',{related_doc_type:'inbound_plan',related_doc_id:'IB',inbound_external_no:'0000077'});
 f.job('PICK','pick_direct',{display_no:'PK-20261008-001'});f.DB.raw.exec("INSERT INTO v2_ops_job_pick_docs(id,job_id,pick_doc_no,created_at) VALUES('PD1','PICK','0000001','1'),('PD2','PICK','0000002','1')");
 f.DB.raw.prepare('INSERT INTO ck_courier_batches VALUES(?,?,?,?,?)').run('COURIER','2026-10-08',1,'2026-10-08-01',stamp);f.job('COURIER','courier_receiving',{related_doc_type:'courier_batch',related_doc_id:'COURIER',display_no:'2026-10-08-01'});f.record('COURIER');
 const [inbound,pick,courier]=await jobNumbers(f.env,[{id:'IN'},{id:'PICK'},{id:'COURIER'}]);
 assert.equal(inbound.display_business_no,'0000077');assert.equal(inbound.inbound_plan_no,'RU-20261008-009');assert.equal(pick.display_business_no,'0000001、0000002');assert.equal(pick.trip_no,'PK-20261008-001');assert.equal(courier.display_business_no,'2026-10-08-01');assert.equal(courier.job_type,'courier_receiving');assert.match(courier.job_label,/快递收货/);
 assert.equal((await courierBatchList(f.env)).items[0].dispatcher_name,owner.name);
 assert.equal((await courierBatchDetail(f.env,'COURIER')).dispatcher_name,owner.name);
 assert.equal((await f.api('v2_dashboard_order_list',{doc_no:'0000002'})).items[0].id,'PICK');
});

test('SOP task list, detail, scan resolver, dispatch and dashboard share ZY label and authenticated dispatcher while preserving RW and scan IDs',async t=>{
 const f=await fixture(t),need=await f.sop('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'000-STOCK',customer:'虚拟客户',title:'两托贴标',instructions:'按单贴标',planned_quantity:2,planned_unit:'托'});
 const task=await f.sop('sop_task_create',{department:'bulk',need_id:need.id,title:'两托贴标',job_type:'bulk_op',workers:[{id:'W1',name:'主操作员朴'}],lead_id:'W1',estimated_minutes:5});
 await f.sop('sop_task_start',{id:task.id,revision:task.revision});
 const before=f.snapshot(),record=(await f.sop('sop_get',{id:task.id})).record;assert.match(record.display_no,/^RW-/);assert.match(record.display_business_no,/^ZY-/);assert.equal(record.dispatcher_name,owner.name);assert.equal(record.id,task.id);
 const list=(await f.sop('sop_list',{kind:'task'})).items[0],field=(await f.sop('sop_field_resolve',{code:task.id})).task,dispatch=(await f.sop('sop_dispatch_list')).items[0];
 const dashboard=await f.sop('sop_dashboard');assert.equal(dashboard.roster[0].dispatcher_name,owner.name);assert.equal(dashboard.live[0].job_label,record.job_label);
 for(const row of [list,field,dispatch]){assert.equal(row.job_label,record.job_label);assert.equal(row.dispatcher_name,owner.name);assert.equal(row.id,task.id);}
 assert.deepEqual(f.snapshot(),before);assert.equal((await f.sop('sop_get',{id:record.display_no})).record.id,task.id);
});

test('attendance current jobs and historical segments retain IDs/times and expose real job labels instead of JOB aliases',async t=>{
 const f=await fixture(t),call=(suffix,data={})=>handleAttendance({action:'sop_attendance_'+suffix,client_req_id:crypto.randomUUID(),...data},f.env);
 const checkin=(await call('checkin',{name:'虚拟日当',agency:'가온'})).record,start=new Date(Date.parse(checkin.inAt)+1).toISOString(),id='JOB-'+crypto.randomUUID();
 f.job(id,'inventory',{created_at:start});f.record(id,'dispatch',{labor_department:'import'},'import');f.segment(id,checkin.badgeId,start);
 const before=f.snapshot(),summary=await call('summary'),row=summary.items[0];
 for(const item of [row.currentJobs[0],row.segments[0]]){assert.equal(item.dispatcher_name,owner.name);assert.equal(item.job_department,'import');assert.match(item.job_label,/盘点.*进口/);assert.doesNotMatch(item.no||item.jobNo,/JOB-|RW-/);}
 assert.equal(row.currentJobs[0].id,id);assert.equal(row.segments[0].jobId,id);assert.equal(row.segments[0].start,start);assert.deepEqual(f.snapshot(),before);
 f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=?,leave_reason='job_completed' WHERE job_id=?").run(new Date(Date.parse(start)+60000).toISOString(),id);
 const history=(await call('summary')).items[0];assert.equal(history.currentJobs.length,0);assert.equal(history.segments[0].reason,'job_completed');assert.equal(history.segments[0].job_label,row.segments[0].job_label);
});

test('worker details, lists, live boards, exports and workhour reports preserve canonical fields and machine IDs',async t=>{
 const f=await fixture(t),id='JOB-'+crypto.randomUUID(),now=new Date().toISOString();f.job(id,'pack_direct',{created_at:now,updated_at:now});f.record(id);f.segment(id,'LEAD-FIXTURE',now);
 const detail=(await f.api('v2_ops_job_detail',{job_id:id})).job;
 const rows=[(await f.api('v2_order_ops_job_list')).items[0],(await f.api('v2_dashboard_order_detail',{job_id:id})).job,(await f.api('v2_dashboard_order_list')).items[0],(await f.api('v2_dashboard_live_docs')).docs[0],(await f.api('v2_ops_realtime_board')).active_workers[0],(await f.api('v2_dashboard_workhour_summary')).segments[0]];
 for(const row of rows){assert.equal(row.id||row.job_id,id);assert.equal(row.job_label,detail.job_label);assert.equal(row.dispatcher_name,owner.name);assert.equal(row.has_business_reference,false);}
 const output=(await f.api('v2_dashboard_order_export')).rows[0];assert.equal(output.job_id,id);assert.equal(output.派审员,owner.name);assert.equal(output.作业,detail.job_label);assert.equal(output.单号,detail.job_label);assert.doesNotMatch(output.单号,/JOB-|RW-/);
});

test('one metadata query covers repeated job references without mutating rows or changing disabled-rollout responses',async t=>{
 const f=await fixture(t);for(let i=0;i<20;i++){f.job('J-'+i);f.record('J-'+i);}
 const input=Array.from({length:1000},(_,i)=>({id:'SEG-'+i,job_id:'J-'+i%20,worker_id:'W'+i})),copy=structuredClone(input),prepare=f.DB.prepare.bind(f.DB);let reads=0;
 f.DB.prepare=sql=>{const stmt=prepare(sql),all=stmt.all.bind(stmt);stmt.all=async()=>{reads++;return all();};return stmt;};
 const rows=await jobNumbers(f.env,input,'job_id');assert.equal(reads,1);assert.equal(rows.length,1000);assert.deepEqual(input,copy);for(let i=0;i<rows.length;i++){assert.equal(rows[i].id,input[i].id);assert.equal(rows[i].job_id,input[i].job_id);assert.equal(rows[i].dispatcher_name,owner.name);}
 const disabled={SOP_UPGRADE_ENABLED:'false',DB:{prepare(){throw Error('Must not read');}}};assert.equal(await jobNumbers(disabled,input,'job_id'),input);
});

test('crew availability and reconciliation display metadata describes the actual source and destination without substituting worker IDs',async t=>{
 const f=await fixture(t);await handleAttendance({action:'sop_attendance_config'},f.env);await ensureCrewBorrow(f.env);const now=new Date().toISOString();f.job('SOURCE','pack_direct',{created_at:now});f.record('SOURCE');const seg=f.segment('SOURCE','LEAD-FIXTURE',now);f.job('DEST','unload',{created_at:now});f.record('DEST','dispatch',{},'bulk');
 const available=await crewAvailability(f.env,{job_id:'DEST',worker_id:'LEAD-FIXTURE'});assert.equal(available.can_borrow,true);assert.equal(available.source.job_id,'SOURCE');assert.equal(available.source.dispatcher_name,owner.name);assert.match(available.source.job_label,/代发打包/);
 f.DB.raw.prepare('INSERT INTO ck_crew_borrows(id,request_id,actor_id,actor_name,destination_job_id,worker_id,worker_name,source_job_id,source_segment_id,source_kind,source_revision,source_owner_id,source_day,status,borrowed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run('LOAN','REQ',owner.id,owner.name,'DEST','LEAD-FIXTURE','主操作员朴','SOURCE',seg,'dispatch',1,owner.id,new Date(Date.now()+9*3600000).toISOString().slice(0,10),'borrowed',now);
 const row=(await crewStatus(f.env,{job_id:'DEST'})).items[0];assert.equal(row.source_job_id,'SOURCE');assert.equal(row.destination_job_id,'DEST');assert.equal(row.source_dispatcher_name,owner.name);assert.match(row.source_job_label,/代发打包/);assert.match(row.destination_job_label,/卸货/);assert.equal(row.source_has_business_reference,false);
});
