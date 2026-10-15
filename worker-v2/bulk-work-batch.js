import {retireCompletedWorkClocks} from './completed-work-clocks.js';
import {ensureSchema} from './schema-ready.js';
import {ensureAttendance,attendanceEnabled} from './attendance.js';
import {ensureDispatchStartGuard} from './atomic-native-start.js';
import {canonicalDailyBadge,sameWorkerBadge} from '../shared/badge-identity.js';
import {workerBadgeMatch} from './daily-badge-identity.js';
import {assignCargoProcess,processNames,taskGroups} from './cargo-processes.js';
import {recordNumbers,resolveRecordId} from './document-numbers.js';
import {documentCodeError} from '../shared/document-code.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),rows=async(e,s,...a)=>(await q(e,s,...a).all()).results;
const text=v=>String(v??'').trim(),terminal=d=>['completed','cancelled'].includes(d.status);
const read=async(e,id)=>{if(!id)return null;const r=await q(e,'SELECT * FROM sop_records WHERE id=?',id).first();return r?{...r,data:JSON.parse(r.state)}:null;};
const pub=r=>({id:r.id,kind:r.kind,revision:r.revision,department:r.department,...r.data});
export const bulkBatchEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_UPGRADE_ENABLED==='true'&&e.SOP_WORK_CHAIN_ENABLED==='true'&&attendanceEnabled(e);
export async function ensureBulkBatch(e){
 await ensureSchema(e.DB,'bulk-work-batch-v1',()=>e.DB.batch([
  q(e,'CREATE TABLE IF NOT EXISTS ck_bulk_work_batch_items(job_id TEXT PRIMARY KEY,batch_id TEXT NOT NULL,position INTEGER NOT NULL,source_type TEXT NOT NULL,source_id TEXT NOT NULL)'),
  q(e,'CREATE INDEX IF NOT EXISTS ck_bulk_work_batch_order ON ck_bulk_work_batch_items(batch_id,position)'),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_bulk_batch_one_clock BEFORE INSERT ON v2_ops_job_workers WHEN NEW.left_at='' AND EXISTS(SELECT 1 FROM ck_bulk_work_batch_items WHERE job_id=NEW.job_id) BEGIN
   SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_bulk_work_batch_items i JOIN sop_records b ON b.id=i.batch_id WHERE i.job_id=NEW.job_id AND json_extract(b.state,'$.status')='working' AND json_extract(b.state,'$.active_job_id')=NEW.job_id AND ${workerBadgeMatch('NEW.worker_id',"json_extract(b.state,'$.worker.id')")}) OR EXISTS(SELECT 1 FROM ck_bulk_work_batch_items i JOIN v2_ops_job_workers w ON w.job_id=i.job_id WHERE i.batch_id=(SELECT batch_id FROM ck_bulk_work_batch_items WHERE job_id=NEW.job_id) AND w.left_at='') THEN RAISE(ABORT,'bulk_batch_one_person_one_clock') END; END`)
  ,q(e,`CREATE TRIGGER IF NOT EXISTS ck_bulk_batch_job_state BEFORE INSERT ON sop_events WHEN json_extract(NEW.result_json,'$._bulk_expected_job_status') IS NOT NULL BEGIN
   SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=NEW.record_id AND j.status=json_extract(NEW.result_json,'$._bulk_expected_job_status')) THEN RAISE(ABORT,'bulk_batch_job_changed') END;
   SELECT CASE WHEN NEW.action IN ('sop_bulk_batch_switch','sop_bulk_batch_resume') AND json_extract(NEW.after_json,'$.status')='working' AND (EXISTS(SELECT 1 FROM sop_records n WHERE n.id=json_extract(NEW.after_json,'$.need_id') AND json_extract(n.state,'$.status') IN ('closed','cancelled')) OR EXISTS(SELECT 1 FROM v2_ops_jobs j LEFT JOIN v2_outbound_orders o ON o.id=j.linked_outbound_order_id WHERE j.id=NEW.record_id AND COALESCE(j.linked_outbound_order_id,'')<>'' AND (o.id IS NULL OR o.status IN ('cancelled','completed','shipped','loaded','loading') OR EXISTS(SELECT 1 FROM sop_records n WHERE n.kind='need' AND json_extract(n.state,'$.source_id')=o.id)))) THEN RAISE(ABORT,'bulk_batch_source_changed') END;
  END`)
 ]));
 await ensureDispatchStartGuard(e);
}
function event(e,row,data,u,request,action,t,result={}){return [q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,row.id,row.revision,action,u.id,u.name,row.state||'{}',JSON.stringify(data),JSON.stringify({...result,...(row.job_status?{_bulk_expected_job_status:row.job_status}:{})}),t),row.revision?q(e,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(data),t,row.id):q(e,'INSERT INTO sop_records VALUES(?,?,?,?,?,?)',row.id,row.kind,1,'bulk',JSON.stringify(data),t)];}
const close=(e,id,t,why)=>q(e,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason=? WHERE job_id=? AND left_at=''",t,t,why,id);
async function permit(e,u,owner='',admission=false){
 if(!bulkBatchEnabled(e)||!['manager','dispatcher'].includes(u?.role))throw Error('仅大货派审员可办理一人多单');
 if(u.scope==='field'&&admission){
  if(u.role!=='dispatcher'||u.daily_dispatch&&!u.can_dispatch||!(u.daily_dispatch?u.dispatch_departments||[]:u.departments||[]).includes('bulk'))throw Error('无大货派工权限');
  if(!await q(e,"SELECT 1 FROM ck_attendance_days d WHERE d.id=? AND d.day=date('now','+9 hours') AND d.signed_out='' AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at='')",u.attendance_id||'').first())throw Error('派审员须已签到且不在休息中');
 }
 if(owner&&owner!==u.id&&!(u.role==='manager'&&u.scope==='office'))throw Error('请由本批派审员操作；原负责人离岗时请先办理接班');
}
async function person(e,workers,lead){
 if(!Array.isArray(workers)||workers.length!==1||!workers[0]?.id||!sameWorkerBadge(workers[0].id,lead))throw Error('一人多单必须且只能选择1名实际操作员');
 const id=canonicalDailyBadge(workers[0].id),r=await q(e,`SELECT d.* FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id LEFT JOIN ck_attendance_management m ON m.attendance_id=d.id WHERE ${workerBadgeMatch('d.worker_id','?')} AND d.day=date('now','+9 hours') AND d.signed_out='' AND p.enabled=1 AND COALESCE(m.voided_at,'')='' AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at='')`,id).first();
 if(!r)throw Error('操作员须已签到、未签退且不在休息中');await retireCompletedWorkClocks(e,[{id}]);return {id:canonicalDailyBadge(r.worker_id),name:r.name};
}
function readyProcesses(d){
 if(!d.cargo_groups)return [{}];const choices=new Map();
 for(const g of d.cargo_groups.groups){const ps=g.processes||processNames(g.process_names).map(name=>({name,status:'pending',task_id:''}));for(let index=0;index<ps.length;index++){const p=ps[index];if(p.status!=='pending'||p.task_id||ps.slice(0,index).some(x=>x.status!=='approved'))continue;const key=index+'|'+p.name;if(!choices.has(key))choices.set(key,{cargo_process_index:index,cargo_group_ids:[],name:p.name});choices.get(key).cargo_group_ids.push(g.id);}}
 return [...choices.values()];
}
async function needCandidate(e,r,activeSet=null){
 if(!r||r.kind!=='need'||r.department!=='bulk'||['closed','cancelled','waiting_customer','linked','completed'].includes(r.data.status)||r.data.needs_clarification||r.data.operation_kind==='direct_forward')return null;
 if(!r.data.cargo_groups&&r.data.status!=='pending')return null;
 const processes=readyProcesses(r.data);if(!processes.length)return null;
 const active=activeSet?activeSet.has(r.id):await q(e,"SELECT 1 FROM sop_records WHERE kind='task' AND json_extract(state,'$.need_id')=? AND json_extract(state,'$.status') NOT IN ('completed','cancelled') LIMIT 1",r.id).first();if(active)return null;
 return {source_type:'need',id:r.id,revision:r.revision,customer:r.data.customer,title:r.data.title,scope_text:r.data.scope_text||'',processes};
}
async function candidate(e,item,helpers){
 if(item.source_type==='need'){const r=await read(e,item.id),c=await needCandidate(e,r);if(!c||Number(item.revision)!==r.revision)throw Error('作业单已派工、状态或版本已变化，请刷新后重选');return {...c,row:r,key:'need:'+r.id};}
 const no=text(item.work_order_no);if(!no||documentCodeError(no)||/^(CKWORK\||NEED-|SOPJOB-|(?:ZY|RW)-\d{8}-\d+)/i.test(no))throw Error('请使用外部作业单号，作业计划请从需求列表选择');
 const order=await helpers.findOutbound(no);if(order){if(order.biz_class!=='bulk'||['completed','cancelled','shipped','loaded','loading'].includes(order.status)||(await helpers.linkedNeeds(order.id)).length)throw Error('此单不属于可派发的大货外部作业，请使用原关联作业');if(item.token&&item.token!==JSON.stringify([order.id,order.status,order.updated_at,order.revision_no||0]))throw Error('订单已更新，请重新选单');}
 const old=await q(e,"SELECT id,status FROM v2_ops_jobs WHERE job_type='bulk_op' AND (related_doc_id=? OR (?!='' AND linked_outbound_order_id=?)) ORDER BY created_at DESC LIMIT 1",no,order?.id||'',order?.id||'').first();
 if(old&&(['pending','working','awaiting_close','paused'].includes(old.status)||old.status==='completed'&&order?.status!=='reopen_pending'))throw Error('该单已有任务或已完成，请继续原任务');
 const customer=text(order?.customer||item.customer);return {source_type:'external',work_order_no:no,order,customer,title:no,token:order?JSON.stringify([order.id,order.status,order.updated_at,order.revision_no||0]):'',key:order?'outbound:'+order.id:'external:'+no};
}
async function batchItems(e,id){return (await rows(e,'SELECT s.*,i.position,i.source_type AS batch_source_type,i.source_id AS batch_source_id,j.status AS job_status,j.active_worker_count FROM ck_bulk_work_batch_items i JOIN sop_records s ON s.id=i.job_id JOIN v2_ops_jobs j ON j.id=s.id WHERE i.batch_id=? ORDER BY i.position',id)).map(r=>({...r,data:{...JSON.parse(r.state),...(r.kind==='dispatch'?{status:r.job_status}:{})}}));}
async function commit(e,b,u,row,data,sql,result){
 const t=new Date().toISOString(),signature=JSON.stringify(b),answer={ok:true,id:row.id,revision:row.revision+1,...result,_bulk_signature:signature};
 try{await e.DB.batch([...event(e,row,data,u,b.client_req_id,b.action,t,answer),...sql]);}catch(error){const prior=await replay(e,b,u);if(prior)return prior;throw error;}return answer;
}
async function replay(e,b,u){if(!b.client_req_id)return null;const r=await q(e,'SELECT * FROM sop_events WHERE request_id=?',b.client_req_id).first();if(!r)return null;const a=JSON.parse(r.result_json);if(r.actor_id!==u.id||r.action!==b.action||a._bulk_signature!==JSON.stringify(b))throw Error('请求编号已使用，内容已变化');return a;}
export async function handleBulkBatch(b,e,helpers){
 const u=e.SOP_REQUEST_USER;await permit(e,u,'',['sop_bulk_batch_candidates','sop_bulk_batch_create','sop_bulk_batch_people'].includes(b.action));await ensureAttendance(e);await ensureBulkBatch(e);
 if(b.action==='sop_bulk_batch_candidates'){
  if(b.external&&b.code){const c=await candidate(e,{source_type:'external',work_order_no:b.code,customer:b.customer},helpers);delete c.order;return {ok:true,items:[c]};}
  let records;if(b.code){const id=await resolveRecordId(e,text(b.code));records=[await read(e,id)].filter(Boolean);}else records=(await rows(e,"SELECT * FROM sop_records WHERE kind='need' AND department='bulk' AND json_extract(state,'$.status') IN ('pending','assigned','working') ORDER BY updated_at DESC LIMIT 500")).map(r=>({...r,data:JSON.parse(r.state)}));
  const activeSet=new Set((await rows(e,"SELECT json_extract(state,'$.need_id') AS need_id FROM sop_records WHERE kind='task' AND department='bulk' AND json_extract(state,'$.status') NOT IN ('completed','cancelled')")).map(r=>r.need_id));const list=[];for(const r of records){const c=await needCandidate(e,r,activeSet);if(c)list.push({...pub(r),...c});}return {ok:true,items:await recordNumbers(e,list)};
 }
 if(b.action==='sop_bulk_batch_create'){
  if(!text(b.client_req_id))throw Error('缺少请求编号');const old=await replay(e,b,u);if(old)return old;
  const worker=await person(e,b.workers,b.lead_id);if(!Array.isArray(b.items)||!b.items.length||b.items.length>20)throw Error('请选1至20张大货作业单');
  if(await q(e,`SELECT 1 FROM v2_ops_job_workers WHERE ${workerBadgeMatch('worker_id','?')} AND left_at=''`,worker.id).first())throw Error('该操作员仍在其他任务中，请先办理交接');
  const selected=[],keys=new Set();for(const item of b.items){const c=await candidate(e,item,helpers);if(keys.has(c.key))throw Error('同一作业单不能重复选择');keys.add(c.key);if(!c.customer)throw Error('请填写每张外部作业单的客户');selected.push(c);}
  const id='BB-'+crypto.randomUUID(),t=new Date().toISOString(),state={title:'大货一人多单',owner_id:u.id,owner:u.name,worker,status:'working',active_job_id:'',created_at:t},sql=[],jobs=[];
  for(const c of selected){
   if(c.source_type==='need'){
    const nd=structuredClone(c.row.data);nd.bulk_batch_id=id;nd.status='assigned';
    for(const process of c.processes){const job='SOPJOB-'+crypto.randomUUID(),first=!jobs.length,data={title:c.title+(process.name?' · '+process.name:''),job_type:'bulk_op',need_id:c.id,owner_id:u.id,owner:u.name,workers:[worker],lead_id:worker.id,status:first?'working':'assigned',estimated_minutes:30,location:nd.location||'',created_at:t,started_at:first?t:'',customer:c.customer,result:null,review:null,round:1,delegates:[],bulk_batch_id:id};if(nd.cargo_groups)assignCargoProcess(nd,{id:job,data},process,u,t);else nd.task_id=job;
     sql.push(q(e,"INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count,customer) VALUES(?,'order_op','bulk','bulk_op',?,?,?,?,?,?,?,?)",job,nd.source_type==='inbound'?'inbound_plan':'sop_need',nd.source_id||c.id,first?'working':'pending',u.id,t,t,0,c.customer),...event(e,{id:job,kind:'task',revision:0},data,u,b.client_req_id+'-'+job,'sop_task_dispatch',t),q(e,'INSERT INTO ck_bulk_work_batch_items VALUES(?,?,?,?,?)',job,id,jobs.length,'need',c.id));jobs.push(job);
    }
    sql.push(...event(e,c.row,nd,u,b.client_req_id+'-'+c.id,b.action,t));
   }else{
    const job='JOB-'+crypto.randomUUID(),first=!jobs.length,data={title:c.work_order_no,customer:c.customer,job_type:'bulk_op',source_type:'work_order',source_id:c.order?.id||c.work_order_no,owner_id:u.id,owner:u.name,workers:[worker],lead_id:worker.id,status:first?'working':'pending',estimated_minutes:30,labor_department:'bulk',created_at:t,bulk_batch_id:id};
    const tail=c.order?' FROM v2_outbound_orders WHERE id=? AND status=? AND updated_at IS ? AND COALESCE(revision_no,0)=?':'';
    sql.push(q(e,"INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count,customer,linked_outbound_order_id,shared_result_json) SELECT ?,'order_op','bulk','bulk_op','work_order',?,?,?,?,?,0,?,?,?"+tail,job,c.work_order_no,first?'working':'pending',worker.id,t,t,c.customer,c.order?.id||'',JSON.stringify(c.order?.status==='reopen_pending'?{started_from_reopen_pending:true}:{}),...(c.order?[c.order.id,c.order.status,c.order.updated_at,c.order.revision_no||0]:[])),...event(e,{id:job,kind:'dispatch',revision:0},data,u,b.client_req_id+'-'+job,'sop_native_start',t),q(e,'INSERT INTO ck_bulk_work_batch_items VALUES(?,?,?,?,?)',job,id,jobs.length,'external',c.order?.id||c.work_order_no));jobs.push(job);
   }
  }
  state.active_job_id=jobs[0];sql.push(q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)','WS-'+crypto.randomUUID(),jobs[0],worker.id,worker.name,t),q(e,'UPDATE v2_ops_jobs SET active_worker_count=1 WHERE id=?',jobs[0]));
  return commit(e,b,u,{id,kind:'bulk_batch',revision:0},state,sql,{batch_id:id,job_id:jobs[0],jobs});
 }
 const row=await read(e,b.id);if(!row||row.kind!=='bulk_batch')throw Error('多单派工批次不存在');await permit(e,u,row.data.owner_id);const items=await batchItems(e,row.id);
 if(b.action==='sop_bulk_batch_get')return {ok:true,batch:pub(row),items:await recordNumbers(e,items.map(r=>({...pub(r),job_status:r.job_status,active_worker_count:r.active_worker_count}))),completed:items.every(r=>terminal(r.data))};
 if(!text(b.client_req_id))throw Error('缺少请求编号');const old=await replay(e,b,u);if(old)return old;if(Number(b.revision)!==row.revision)throw Error('多单派工已更新，请刷新后重试');
 const data=structuredClone(row.data),t=new Date().toISOString(),sql=[];
 if(b.action==='sop_bulk_batch_switch'||b.action==='sop_bulk_batch_resume'){
  if(data.status==='paused'&&b.action!=='sop_bulk_batch_resume')throw Error('本批作业已暂停，请先恢复');
  const target=items.find(r=>r.id===b.job_id);if(!target||terminal(target.data)||target.data.status==='awaiting_review')throw Error('请选择尚未完成且可操作的单据');const worker=await person(e,[data.worker],data.worker.id);
  if(target.kind==='dispatch'){const doc=await q(e,'SELECT linked_outbound_order_id FROM v2_ops_jobs WHERE id=?',target.id).first();if(doc?.linked_outbound_order_id){const order=await q(e,'SELECT status FROM v2_outbound_orders WHERE id=?',doc.linked_outbound_order_id).first();if(!order||['cancelled','completed','shipped','loaded','loading'].includes(order.status))throw Error('关联订单已取消或结束，请核对原单');if((await helpers.linkedNeeds(doc.linked_outbound_order_id)).length)throw Error('此单已有作业需求，请核对原关联作业');}}
  if(target.data.need_id){const need=await read(e,target.data.need_id);if(!need||['closed','cancelled'].includes(need.data.status))throw Error('作业要求已关闭');if(need.data.cargo_groups&&taskGroups(need.data.cargo_groups,{id:target.id,data:target.data}).some(g=>g.ack_version!==(g.mapping_version||1)))throw Error('请先确认此单最新资料版本');}
  const busy=await q(e,`SELECT job_id FROM v2_ops_job_workers WHERE ${workerBadgeMatch('worker_id','?')} AND left_at='' AND job_id NOT IN (SELECT job_id FROM ck_bulk_work_batch_items WHERE batch_id=?)`,worker.id,row.id).first();if(busy)throw Error('操作员正在其他任务中，请先交接');
  for(const r of items){if(r.id!==target.id&&!terminal(r.data)&&r.job_status==='working'){const d={...r.data,status:'paused',pause_reason:'切换至同批另一单'};sql.push(close(e,r.id,t,'bulk_batch_switch'),...event(e,r,d,u,b.client_req_id+'-'+r.id,b.action,t),q(e,"UPDATE v2_ops_jobs SET status='paused',active_worker_count=0,updated_at=? WHERE id=?",t,r.id));}}
  data.status='working';data.active_job_id=target.id;const d={...target.data,status:'working',started_at:target.data.started_at||t,pause_reason:'',workers:[worker],lead_id:worker.id};
  sql.push(close(e,target.id,t,'bulk_batch_switch'),...event(e,target,d,u,b.client_req_id+'-'+target.id,b.action,t),q(e,"UPDATE v2_ops_jobs SET status='working',active_worker_count=1,updated_at=? WHERE id=?",t,target.id),q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)','WS-'+crypto.randomUUID(),target.id,worker.id,worker.name,t));
 }else if(b.action==='sop_bulk_batch_pause'){
  if(!text(b.reason))throw Error('请填写暂停原因');data.status='paused';for(const r of items)if(!terminal(r.data)&&r.job_status==='working')sql.push(close(e,r.id,t,'bulk_batch_pause'),...event(e,r,{...r.data,status:'paused',pause_reason:text(b.reason)},u,b.client_req_id+'-'+r.id,b.action,t),q(e,"UPDATE v2_ops_jobs SET status='paused',active_worker_count=0,updated_at=? WHERE id=?",t,r.id));
 }else if(b.action==='sop_bulk_batch_people'){
  const worker=await person(e,b.workers,b.lead_id);if(!text(b.reason))throw Error('请填写换人原因');if(await q(e,`SELECT 1 FROM v2_ops_job_workers WHERE ${workerBadgeMatch('worker_id','?')} AND left_at='' AND job_id NOT IN (SELECT job_id FROM ck_bulk_work_batch_items WHERE batch_id=?)`,worker.id,row.id).first())throw Error('操作员仍在其他任务中');data.worker=worker;
  for(const r of items)if(!terminal(r.data)){sql.push(close(e,r.id,t,'bulk_batch_people'),...event(e,r,{...r.data,workers:[worker],lead_id:worker.id,last_lead:worker},u,b.client_req_id+'-'+r.id,b.action,t));if(data.status==='working'&&r.id===data.active_job_id&&r.job_status==='working')sql.push(q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)','WS-'+crypto.randomUUID(),r.id,worker.id,worker.name,t),q(e,'UPDATE v2_ops_jobs SET active_worker_count=1 WHERE id=?',r.id));}
 }else if(b.action==='sop_bulk_batch_cancel'){
  if(!text(b.reason))throw Error('请填写取消本单派工原因');const r=items.find(r=>r.id===b.job_id);if(!r||terminal(r.data))throw Error('此单已结束，请刷新');if(r.data.result||await q(e,'SELECT 1 FROM v2_ops_job_results WHERE job_id=? LIMIT 1',r.id).first())throw Error('已提交产出请先按原流程审核，不可取消');
  if(r.data.need_id){const need=await read(e,r.data.need_id),nd=structuredClone(need.data);if(nd.cargo_groups){for(const g of taskGroups(nd.cargo_groups,{id:r.id,data:r.data})){const p=g.processes?.find(x=>x.task_id===r.id);if(p?.result||p?.status==='approved'||p?.status==='awaiting_review')throw Error('本单已有成果，请按原流程完成审核');if(p){p.task_id='';p.status='pending';}}}else{nd.task_id='';nd.status='pending';}sql.push(...event(e,need,nd,u,b.client_req_id+'-need',b.action,t));}
  sql.push(close(e,r.id,t,'bulk_batch_cancel'),...event(e,r,{...r.data,status:'cancelled',cancel_reason:text(b.reason)},u,b.client_req_id+'-'+r.id,b.action,t),q(e,"UPDATE v2_ops_jobs SET status='cancelled',active_worker_count=0,updated_at=? WHERE id=?",t,r.id));if(data.active_job_id===r.id)data.active_job_id='';
 }else throw Error('未知多单派工操作');
 return commit(e,b,u,row,data,sql,{batch_id:row.id,job_id:data.active_job_id});
}

export async function guardBulkBatchMutation(b,e){
 if(!bulkBatchEnabled(e))return;if(b.action==='sop_task_dispatch'&&b.need_id){const n=await read(e,b.need_id);if(n?.data.bulk_batch_id&&await q(e,"SELECT 1 FROM sop_records WHERE kind='task' AND json_extract(state,'$.need_id')=? AND json_extract(state,'$.bulk_batch_id')=? AND json_extract(state,'$.status') NOT IN ('completed','cancelled')",n.id,n.data.bulk_batch_id).first())throw Error('此单仍在一人多单批次中，请从原批次继续');}
 const guarded=['sop_task_start','sop_task_people','sop_task_delegate','sop_native_people','sop_native_resume','v2_ops_job_resume','sop_task_cancel','sop_task_finish','sop_task_complete_review','sop_cargo_group_complete','sop_cargo_job_finish','v2_bulk_op_job_finish'];if(!guarded.includes(b.action))return;
 const id=b.job_id||b.task_id||b.id;if(!id)return;let r=await read(e,id);if(r?.kind==='need'&&/^sop_cargo_/.test(b.action))r=await read(e,b.task_id||r.data.task_id);const batch=r?.data.bulk_batch_id;if(!batch||!['task','dispatch'].includes(r.kind))return;
 if(['sop_task_start','sop_task_people','sop_task_delegate','sop_native_people','sop_native_resume','v2_ops_job_resume','sop_task_cancel'].includes(b.action))throw Error('一人多单请从批次列表切单、换人或取消，实际操作员必须为1人');
 const work=['sop_task_finish','sop_task_complete_review','sop_cargo_group_complete','sop_cargo_job_finish','v2_bulk_op_job_finish'];if(work.includes(b.action)&&!b.leave_only){const parent=await read(e,batch);if(parent?.data.status!=='working'||parent.data.active_job_id!==r.id||r.data.workers?.length!==1)throw Error('请先从一人多单列表切换到此单再登记产出');}
}
export async function bulkOwnerTransferStatements(e,row,next,u,b,t){
 if(!row.data.bulk_batch_id)return [];const batch=await read(e,row.data.bulk_batch_id);if(!batch)return [];const sql=[];
 for(const r of await batchItems(e,batch.id))if(r.id!==row.id&&!terminal(r.data)){if(r.data.owner_id!==row.data.owner_id)throw Error('同批派审归属已变化，请刷新核对');const d={...r.data,owner_id:next.owner_id,owner:next.owner,dispatcher_name:next.dispatcher_name||next.owner,delegates:[],original_owner_id:r.data.original_owner_id||r.data.owner_id,original_owner:r.data.original_owner||r.data.owner,last_owner_transfer:next.last_owner_transfer,owner_transfers:[...(r.data.owner_transfers||[]),next.owner_transfers.at(-1)]};sql.push(...event(e,r,d,u,b.client_req_id+'-'+r.id,'sop_owner_transfer',t));}
 sql.push(...event(e,batch,{...batch.data,owner_id:next.owner_id,owner:next.owner},u,b.client_req_id+'-batch','sop_bulk_batch_transfer',t));return sql;
}
