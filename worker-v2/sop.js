import {issueAccounting} from './issue-accounting.js';
import {cargoReservations} from './cargo-reservations.js';
import {cancelCargoDraft} from './cargo-create.js';
import {assignCargoProcess,taskGroups} from './cargo-processes.js';
import {cargoSplit} from './cargo-split.js';
import {cargoReferencesFile} from './cargo-allocation.js';
import {cargoShipping} from './cargo-shipping.js';
import {cargoExecution} from './cargo-execution.js';
import {cargoGroups} from './cargo-groups.js';
import {restingDispatchAccess} from './dispatch-access.js';
import {ensureDocumentNumbers,recordNumbers,resolveRecordId,jobNumbers,jobDisplayMetadata} from './document-numbers.js';
import {loadTripEnabled} from './outbound-load-trip.js';
import {readBatchMaterials,changeBatchMaterial} from './batch-work-materials.js';
import {completionDate} from './completion-date.js';
import { workChainEnabled, chainOutboundStatements, chainAllocation, guardWorkChain, workMaterialRead, workMaterials, notifyMaterialChange, assertShippingResult, shippingBasisStatements } from './work-chain.js';
import { workPlanStatements } from './sop-planning.js';
import { dispatchAccess } from './dispatch-access.js';
import {sourceActiveWorkers} from './crew-borrow.js';
import {effectiveResults} from './effective-results.js';
/* SOP pilot: explicit opt-in, revision checked atomic mutations, immutable history.
 * No production migration or legacy job conversion on request paths.
 */
const kinds = ['need','task','check','issue'];
const roles = ['manager','dispatcher','reviewer','service','viewer'];
const text = (v, max=4000) => String(v ?? '').trim().slice(0,max);
const now = () => new Date().toISOString();
const uid = p => p + '-' + crypto.randomUUID();
const fail = m => { throw new Error(m); };
const positive = v => { const n=Number(v); if(!Number.isSafeInteger(n)||n<1) fail('数量必须为正整数'); return n; };
const date = v => { const s=text(v,10); if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||new Date(s).toISOString().slice(0,10)!==s) fail('日期无效'); return s; };
const required = (v, label) => text(v) || fail(label+'不能为空');
const stmt = (env, sql, ...args) => env.DB.prepare(sql).bind(...args);
const all = async (env, sql,...args) => (await stmt(env,sql,...args).all()).results||[];
function authorization(body,env) {
 const configError = detail => ({ok:false,unauthorized:true,code:'AUTH_CONFIG',error:env.SOP_ENVIRONMENT==='staging'
  ? detail+'。请在测试 Worker 的设置 → 变量和机密中检查 SOP_USERS_JSON（不是构建变量），保存并部署。'
  : '授权配置未就绪，请联系管理员'});
 if(!env.SOP_USERS_JSON) return configError('后台未配置个人授权');
 let users; try {users=JSON.parse(env.SOP_USERS_JSON);}catch{return configError('后台个人授权配置不是有效 JSON');}
 if(!Array.isArray(users)||!users.length) return configError('后台个人授权配置必须是非空人员数组');
 const valid=u=>u&&typeof u.key==='string'&&u.key.length>0&&typeof u.id==='string'&&u.id&&typeof u.name==='string'&&u.name&&roles.includes(u.role);
 if(!users.every(valid)) return configError('后台人员配置缺少有效的 key、id、name 或 role');
 if(typeof body.sop_key!=='string'||!body.sop_key) return {ok:false,unauthorized:true,code:'AUTH_REQUIRED',error:'请先填写个人授权码，再点击进入'};
 const user=users.find(u=>u.key===body.sop_key);
 if(!user) return {ok:false,unauthorized:true,code:'AUTH_INVALID',error:'个人授权码不匹配。请输入后台人员配置中 key 对应的值，注意大小写及前后空格；不是整段 JSON 或 Cloudflare API 令牌。'};
 return {ok:true,user};
}
export function principal(body,env) { return authorization(body,env).user||null; }
function permit(u, department, allowed) {
 if(!u || !allowed.includes(u.role)) fail('无此操作权限');
 if(u.role!=='manager' && !(u.departments||[]).includes(department)) fail('无此部门权限');
}
async function read(env,id) {
 id=await resolveRecordId(env,id);
 const row=await stmt(env,'SELECT * FROM sop_records WHERE id=?',id).first();
 return row ? {...row,data:JSON.parse(row.state)} : null;
}
function checkSummary(data) {
 const items=data.items||[];
 const active=items.filter(x=>!x.removed);
 return {original:data.original_total||0,added:data.added_total||0,removed:data.removed_total||0,adjusted:data.adjusted_total||0,
  planned:active.reduce((n,x)=>n+x.qty,0),scanned:active.reduce((n,x)=>n+x.scanned,0),
  pending:active.filter(x=>x.recheck || x.scanned!==x.qty).length,
  unresolved:(data.exceptions||[]).filter(x=>!x.resolved).length};
}
function publicState(row) {
 return {id:row.id,kind:row.kind,revision:row.revision,department:row.department,updated_at:row.updated_at,...row.data,
 ...(row.kind==='check'?{summary:checkSummary(row.data)}:{})};
}
async function save(env,b,u,row,data,extra=[]) {
 const request=required(b.client_req_id,'请求编号');
 const t=now(), revision=(row?.revision||0)+1;
 const id=row.id, kind=row.kind, department=row.department;
 const result={ok:true,id,revision};
 if(row.kind==='check')syncCheck(env,extra,row,data,u,t,b);
 const before=JSON.stringify(row.data||{}),after=JSON.stringify(data);
 const statements=[
  stmt(env,`INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)`,request,id,revision-1,b.action,u.id,u.name,before,after,JSON.stringify(result),t),
  stmt(env,`INSERT INTO sop_records VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,state=excluded.state,updated_at=excluded.updated_at`,id,kind,revision,department,after,t),...extra
 ];
 await env.DB.batch(statements);
 return result;
}
function syncCheck(env,extra,row,data,u,t,b){
 const id=data.legacy_id||row.id;data.legacy_id=id;
 const total=checkSummary(data).planned;
 extra.push(stmt(env,`INSERT INTO v2_verify_batches(id,batch_no,planned_qty,status,remark,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET planned_qty=excluded.planned_qty,status=excluded.status,updated_at=excluded.updated_at`,id,'出库-'+data.ship_date,total,data.status==='closed'?'completed':'pending','按出库日期分轮核对',u.name,data.created_at,t));
 extra.push(stmt(env,'DELETE FROM v2_verify_batch_items WHERE batch_id=?',id));
 for(const item of data.items.filter(x=>!x.removed))extra.push(stmt(env,'INSERT INTO v2_verify_batch_items(id,batch_id,barcode,planned_qty,planned_box_count,customer_name,created_at) VALUES(?,?,?,?,?,?,?)',item.id,id,item.barcode,item.qty,item.qty,item.customer,item.added_at||t));
 if(b.action==='sop_check_scan'){
  const ex=data.exceptions.find(x=>x.at===t&&x.barcode===b.barcode);
  extra.push(stmt(env,'INSERT INTO v2_verify_scan_logs(id,batch_id,worker_id,worker_name,pallet_no,barcode,scan_result,message,scanned_at) VALUES(?,?,?,?,?,?,?,?,?)',uid('SCAN'),id,u.id,u.name,b.pallet,b.barcode,ex?ex.type:'ok','按日期核对；当前有效数量以版本清单为准',t));
 }
}
function closeSegments(env,id,t,reason) {
 return [stmt(env,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)),leave_reason=? WHERE job_id=? AND left_at=''`,t,t,reason,id),
 stmt(env,"UPDATE v2_ops_jobs SET active_worker_count=0,updated_at=? WHERE id=?",t,id)];
}
async function verifySource(env, type, id, customer) {
 if(!id) return null;
 const table={inbound:'v2_inbound_plans',outbound:'v2_outbound_orders'}[type];
 if(!table) fail('关联类型无效');
 const doc=await stmt(env,`SELECT * FROM ${table} WHERE id=?`,id).first();
 if(!doc || doc.is_deleted || doc.status==='cancelled') fail('关联单据不存在或已取消');
 if(customer && doc.customer && doc.customer!==customer) fail('关联单据客户不一致');
 return doc;
}
export async function handleSop(b,env) {
 if(env.SOP_UPGRADE_ENABLED!=='true') return {ok:false,error:'新版尚未启用，原系统继续使用',disabled:true};
 const auth=env.SOP_REQUEST_USER ? {ok:true,user:env.SOP_REQUEST_USER} : authorization(b,env); if(!auth.ok) return auth;
 const u=auth.user;
 try {
  await ensureDocumentNumbers(env);
  const cancelledDraft=await cancelCargoDraft(b,env,u);if(cancelledDraft)return cancelledDraft;
  const split=await cargoSplit(b,env,u);if(split)return split;
  const reservation=await cargoReservations(b,env,u);if(reservation)return reservation;
  const shipping=await cargoShipping(b,env,u);if(shipping)return shipping;
  const execution=await cargoExecution(b,env,u);if(execution)return execution;
  const cargo=await cargoGroups(b,env,u);if(cargo)return cargo;
  const batchMaterials=await readBatchMaterials(b,env,u);if(batchMaterials)return batchMaterials;
  const batchChange=await changeBatchMaterial(b,env,u);if(batchChange)return batchChange;
  const materialRead=await workMaterialRead(b,env,u);if(materialRead)return materialRead;
  if(env.SOP_ACCEPT_NEW==='false' && (/_create$|_adopt$|_from_outbound$/.test(b.action)||b.action==='sop_task_dispatch'))fail('新版已暂停接收新任务，现有任务仍可收尾');
  if(b.action==='sop_session') return {ok:true,user:{id:u.id,name:u.name,role:u.role,departments:u.departments||[]},mode:env.SOP_ENVIRONMENT||'pilot'};
  if(b.action==='sop_field_resolve') {
   const code=required(b.code,'作业单号').split('|'),id=code[0]==='CKWORK'?code[1]:code[0];
   let row=await read(env,id);if(!row||!['need','task'].includes(row.kind))fail('未找到作业需求，请扫描订单处理组打印的作业单');
   permit(u,row.department,['manager','dispatcher','reviewer']);
   let task=row.kind==='task'?row:row.data.task_id?await read(env,row.data.task_id):null;
   const need=row.kind==='need'?row:row.data.need_id?await read(env,row.data.need_id):null;
   if(need?.data.needs_clarification)fail('要求未补充完整，请联系订单处理组');
   const taskIds=need?[...new Set([...(need.data.task_ids||[]),need.data.task_id].filter(Boolean))]:[];const relatedTasks=[];for(const id of taskIds){const item=await read(env,id);if(item)relatedTasks.push(item);}if(row.kind==='need'&&relatedTasks.length)task=relatedTasks.find(x=>!['completed','cancelled'].includes(x.data.status)&&x.data.owner_id===u.id)||relatedTasks.find(x=>!['completed','cancelled'].includes(x.data.status))||relatedTasks.at(-1);
   const segments=task?await all(env,'SELECT worker_id,worker_name,joined_at,left_at,leave_reason FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at',task.id):[];
   let source=null;if(need?.data.source_id&&['inbound','outbound'].includes(need.data.source_type))source=await verifySource(env,need.data.source_type,need.data.source_id);
   const shipping=need&&workChainEnabled(env)?await chainAllocation(env,need.data):null;
   const numbered=await recordNumbers(env,[need,...(relatedTasks.length?relatedTasks:[task])].filter(Boolean).map(publicState));
   return {ok:true,tasks:relatedTasks.map(t=>numbered.find(x=>x.id===t.id)),need:need?numbered.find(x=>x.id===need.id):null,task:task?numbered.find(x=>x.id===task.id):null,shipping,source:source?{id:source.id,number:source.display_no||'来源单号待补充',customer:source.customer}:null,segments,changed:code[0]==='CKWORK'&&Number(code[2])!==(need?.data.requirement_version||1)};
  }
  if(b.action==='sop_need_groups')return await needGroups(env,u,b);
  if(b.action==='sop_list') {
   if(!kinds.includes(b.kind)) fail('无效类别');
   const offset=Math.max(0,Number(b.offset)||0),limit=50;
   const dep=u.role==='manager'?'':` AND department IN (${(u.departments||[]).map(()=>'?').join(',')||"''"})`;
   const args=u.role==='manager'?[]:u.departments||[];
   const rows=await all(env,`SELECT * FROM sop_records WHERE kind=?${dep} ORDER BY updated_at DESC LIMIT ? OFFSET ?`,b.kind,...args,limit+1,offset);
   return {ok:true,items:await recordNumbers(env,rows.slice(0,limit).map(r=>publicState({...r,data:JSON.parse(r.state)}))),more:rows.length>limit,offset};
  }
  if(b.action==='sop_source_detail') {
   permit(u,b.department||'bulk',['manager','dispatcher','service','reviewer']);
   const doc=await verifySource(env,b.type,required(b.source_id,'单据编号'));
   if(!doc)fail('单据不存在');
   return {ok:true,source:{id:doc.id,title:doc.display_no||'来源单号待补充',customer:doc.customer,instructions:doc.instruction||doc.remark||'',department:doc.biz_class==='direct_ship'?'direct_ship':'bulk'}};
  }
  if(b.action==='sop_linked') {
   permit(u,b.department||'bulk',roles);
   const rows=await all(env,"SELECT * FROM sop_records WHERE kind='need' AND (json_extract(state,'$.source_id')=? OR EXISTS(SELECT 1 FROM json_each(json_extract(state,'$.links')) l WHERE json_extract(l.value,'$.outbound_id')=?))",text(b.source_id),text(b.source_id));
   return {ok:true,items:await recordNumbers(env,rows.filter(r=>u.role==='manager'||(u.departments||[]).includes(r.department)).map(r=>publicState({...r,data:JSON.parse(r.state)})))};
  }
  if(b.action==='sop_sources') {
   const mapping={inbound:'v2_inbound_plans',outbound:'v2_outbound_orders',issue:'v2_issue_tickets',check:'v2_verify_batches'};
   const table=mapping[b.type];if(!table)fail('无效来源');
   permit(u,b.department||'bulk',['manager','service','dispatcher','reviewer']);
   const rows=await all(env,`SELECT * FROM ${table} ORDER BY created_at DESC LIMIT 200`);
   return {ok:true,items:rows.map(x=>({id:x.id,number:x.display_no||x.batch_no||'',label:(x.display_no||x.batch_no||'单号待补充')+' / '+(x.customer||x.customer_name||'')+' / '+(x.status||''),status:x.status})),limited:true};
  }
  if(b.action==='sop_get') {
   const row=await read(env,b.id); if(!row) fail('记录不存在'); permit(u,row.department,roles);
   if(row.kind==='issue'&&!b.version_only)row.data.accounting=await issueAccounting(env,row.data.legacy_id);
   const events=b.version_only?[]:await all(env,'SELECT action,actor_name,before_json,after_json,created_at FROM sop_events WHERE record_id=? ORDER BY created_at DESC LIMIT 100',row.id);
   return {ok:true,record:b.version_only?publicState(row):(await recordNumbers(env,[publicState(row)]))[0],events};
  }
  if(b.action==='sop_dispatch_list'){
   if(!['manager','dispatcher'].includes(u.role))fail('仅派审员可查看现场派工');
   const access=dispatchAccess(u),rest=restingDispatchAccess(u),externalCode=text(b.external_code);
    const rows=await all(env,`SELECT COALESCE(s.kind,'legacy') AS task_kind,COALESCE(s.state,'{}') AS state,j.*,wn.display_no AS work_plan_no,n.state AS need_state,
    ib.display_no AS inbound_no,ib.customer AS inbound_customer,
    ob.display_no AS outbound_no,ob.wms_work_order_no AS outbound_external_no,ob.customer AS outbound_customer,
    fb.display_no AS feedback_no,
    (SELECT json_group_array(json_object('id',w.worker_id,'name',w.worker_name)) FROM v2_ops_job_workers w WHERE w.job_id=j.id AND w.left_at='') AS active_crew,
    (SELECT json_group_array(pd.pick_doc_no) FROM v2_ops_job_pick_docs pd WHERE pd.job_id=j.id) AS pick_numbers,
    (SELECT json_group_array(json_object('number',p.display_no,'customer',p.customer)) FROM ck_unload_plan_links l JOIN v2_inbound_plans p ON p.id=l.plan_id WHERE l.job_id=j.id ORDER BY l.position) AS unload_plans,
    ${loadTripEnabled(env)?"(SELECT json_group_array(json_object('number',o.display_no,'customer',o.customer)) FROM ck_load_order_links l JOIN v2_outbound_orders o ON o.id=l.order_id WHERE l.job_id=j.id ORDER BY l.position)":"'[]'"} AS load_orders
     FROM v2_ops_jobs j LEFT JOIN sop_records s ON j.id=s.id
    LEFT JOIN sop_records n ON n.id=json_extract(s.state,'$.need_id') AND n.kind='need'
    LEFT JOIN sop_document_numbers wn ON wn.record_id=n.id
    LEFT JOIN v2_inbound_plans ib ON j.related_doc_type='inbound_plan' AND ib.id=j.related_doc_id
    LEFT JOIN v2_outbound_orders ob ON ob.id=CASE WHEN j.linked_outbound_order_id!='' THEN j.linked_outbound_order_id WHEN j.related_doc_type='outbound_order' THEN j.related_doc_id END
    LEFT JOIN v2_field_feedbacks fb ON j.related_doc_type='field_feedback' AND fb.id=j.related_doc_id
     WHERE j.status NOT IN ('completed','cancelled')
     AND ((s.kind='dispatch' AND (${access.sql} OR ${rest.sql})) OR (s.kind='task' AND s.department='bulk' AND (?='manager' OR s.department IN (SELECT value FROM json_each(?)))) OR (s.id IS NULL AND j.job_type='load_outbound' AND j.related_doc_type='outbound_order' AND (?='manager' OR j.biz_class IN (SELECT value FROM json_each(?)))))
    AND (?='' OR (s.kind='dispatch' AND j.job_type='bulk_op' AND (j.id=? OR j.related_doc_id=? OR j.display_no=? OR ob.display_no=? OR ob.wms_work_order_no=?)))
     ORDER BY j.updated_at DESC LIMIT 200`,...access.args,...rest.args,u.role,JSON.stringify(u.departments||[]),u.role,JSON.stringify(u.departments||[]),...Array(6).fill(externalCode));
   return {ok:true,items:(await jobNumbers(env,rows)).map(r=>{
    const d=JSON.parse(r.state),plans=JSON.parse(r.unload_plans||'[]'),loads=JSON.parse(r.load_orders||'[]'),picks=JSON.parse(r.pick_numbers||'[]');
    const customer=[...new Set(loads.length?loads.map(o=>o.customer).filter(Boolean):plans.length?plans.map(p=>p.customer).filter(Boolean):[r.customer||r.inbound_customer||r.outbound_customer].filter(Boolean))].join('、');
    return {id:r.id,...jobDisplayMetadata(r),task_no:r.task_no||'',task_kind:r.task_kind,job_type:r.job_type,title:r.task_kind==='task'?JSON.parse(r.need_state||'{}').title||d.title:loads.length?'本车装货 · '+loads.length+' 单':'',status:r.task_kind==='task'?d.status:r.status,source_type:r.related_doc_type,source_id:r.related_doc_id,lead_id:d.lead_id,last_lead:d.last_lead,display_no:r.display_no,business_no:r.business_no,customer,workers:JSON.parse(r.active_crew||'[]'),owner:d.owner,estimated_minutes:d.estimated_minutes};
   })};
  }
  if(b.action==='sop_dashboard') return await dashboard(env,u,b);
  if(b.action==='sop_updates'){
   const rows=await all(env,"SELECT * FROM sop_records WHERE kind='issue' AND json_extract(state,'$.requirement_version')>json_extract(state,'$.ack_version') ORDER BY updated_at DESC LIMIT 100");
   return {ok:true,items:rows.filter(r=>u.role==='manager'||(u.departments||[]).includes(r.department)).map(r=>({id:r.id,legacy_id:JSON.parse(r.state).legacy_id,title:JSON.parse(r.state).title,updated_at:r.updated_at}))};
  }
  if(b.action==='sop_check_for_batch'){
   permit(u,'bulk',roles);const rows=await all(env,"SELECT * FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?",text(b.legacy_id));
   return {ok:true,record:rows[0]?publicState({...rows[0],data:JSON.parse(rows[0].state)}):null};
  }
  const key=required(b.client_req_id,'请求编号');
  const cached=await stmt(env,'SELECT actor_id,action,result_json FROM sop_events WHERE request_id=?',key).first();
  if(cached) {if(cached.actor_id!==u.id||cached.action!==b.action) fail('请求编号冲突'); return JSON.parse(cached.result_json);}
  let row=b.id?await read(env,b.id):null;
  if(b.id && !row && b.action!=='sop_check_create' && b.action!=='sop_issue_adopt') fail('记录不存在');
  if(row && Number(b.revision)!==row.revision) fail('记录已更新，请刷新后再操作');
  const t=now();
  if(b.action==='sop_need_create'||b.action==='sop_need_from_outbound') {
   const department=required(b.department,'部门');permit(u,department,['manager','service','dispatcher']);
   const source_id=text(b.source_id),source_type=b.action==='sop_need_from_outbound'?'outbound':text(b.source_type);
   if(workChainEnabled(env)&&(!['inbound','inventory'].includes(source_type)||(source_type==='inbound'&&!source_id)))fail('作业需求来源只能是入库计划或库内库存');
   const doc=source_type==='inventory'?null:await verifySource(env,source_type,source_id,b.customer);
   const customer=required(b.customer||doc?.customer,'客户');
   if(source_type==='outbound'&&doc){
    const busy=await stmt(env,"SELECT id FROM v2_ops_jobs WHERE (related_doc_id=? OR linked_outbound_order_id=?) AND job_type IN ('bulk_op','outbound_stock_op') LIMIT 1",source_id,source_id).first();
    if(busy || doc.stock_operation_status==='completed')fail('已有旧版操作记录，请从原流程完成；不得再建重复作业');
   }
   if(source_id) {
    const existing=await all(env,`SELECT id FROM sop_records WHERE kind='need' AND json_extract(state,'$.source_id')=? AND json_extract(state,'$.status') NOT IN ('closed','cancelled')`,source_id);
    if(existing.length && !text(b.reason)) fail('已有作业需求；请引用原需求，追加作业须填写原因');
   }
   row={id:source_type==='outbound'?'NEED-'+source_id:uid('NEED'),kind:'need',department,revision:0,data:{}};
   if(await read(env,row.id))fail('此出库计划已有关联作业，请引用原作业');
   const bundle=await workPlanStatements(env,[{...b,id:row.id,title:b.title||(doc?'关联作业 '+(doc.display_no||'来源单号待补充'):''),instructions:b.instructions||doc?.instruction}],{type:source_type,id:source_id,customer,supply_chain_no:b.supply_chain_no},u,t);
   const data={...bundle.needs[0],reason:text(b.reason)};delete data.id;
   // save owns the requirement's event and record; optional outbound statements precede them.
   return await save(env,b,u,row,data,bundle.statements.slice(0,-2));
  }
  if(b.action==='sop_task_create'||b.action==='sop_task_dispatch') {
   const department=required(b.department,'部门');permit(u,department,['manager','dispatcher']);
   const workers=validateWorkers(b.workers,b.lead_id);
   let need=null;
   if(b.need_id) {need=await read(env,b.need_id);if(!need||need.kind!=='need'||need.department!==department)fail('作业需求不存在或部门不符');
    if(workChainEnabled(env)&&need.data.operation_kind==='direct_forward')fail('直接转发不创建加工任务，请在作业需求中确认可发货');
    if(need.data.needs_clarification)fail('请先补充完整作业要求');
    if(need.data.cargo_groups){if(['closed','cancelled'].includes(need.data.status))fail('作业计划已关闭');}else if(need.data.status!=='pending') fail('需求已派工或已完成，不得重复建任务');}
   row={id:uid('SOPJOB'),kind:'task',department,revision:0,data:{}};
   const data={title:required(b.title,'任务名称'),job_type:required(b.job_type,'作业类型'),need_id:need?.id||'',
    owner_id:u.id,owner:u.name,lead_id:required(b.lead_id,'主操作员'),workers,status:'assigned',
    estimated_minutes:positive(b.estimated_minutes),deadline:text(b.deadline),location:text(b.location),created_at:t,
    result:null,review:null,round:1,delegates:[],waiting_reason:'',pause_reason:''};
   const flow={bulk_op:'order_op',pick_direct:'order_op',pack_direct:'internal',unload:'unload',load_outbound:'outbound',inbound_bulk:'inbound',inbound_direct:'inbound',inbound_return:'inbound',qc:'internal',scan_pallet:'import',load_import:'import',pickup_delivery_import:'import'}[data.job_type]||'internal';
   const extra=[stmt(env,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count) VALUES(?,?,?,?,?,?,'pending',?,?,?,0)`,row.id,flow,department,data.job_type,need?.data.source_type==='inbound'?'inbound_plan':need?.data.source_type==='outbound'?'outbound_order':'sop_need',need?.data.source_id||data.need_id,u.id,t,t)];
   data.customer=need?.data.customer||'';extra.push(stmt(env,'UPDATE v2_ops_jobs SET customer=? WHERE id=?',data.customer,row.id));
   if(b.action==='sop_task_dispatch'){if(department!=='bulk')fail('此作业属于其他部门，请从对应现场入口办理');if(!need)fail('请先扫描作业需求');data.status='working';data.started_at=t;for(const w of workers)extra.push(stmt(env,"INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)",uid('WS'),row.id,w.id,w.name,t));extra.push(stmt(env,"UPDATE v2_ops_jobs SET status='working',active_worker_count=? WHERE id=?",workers.length,row.id));}
   if(need){const nd=structuredClone({...need.data,status:'assigned'});if(nd.cargo_groups)assignCargoProcess(nd,{id:row.id,data},b,u,t);else nd.task_id=row.id;appendRelated(env,extra,key+'-need',u,need,nd,b.action,t);}
   return await save(env,b,u,row,data,extra);
  }
  if(b.action==='sop_check_create'||b.action==='sop_check_adopt') {
   const department='bulk';permit(u,department,['manager','service','reviewer','dispatcher']);
   const ship_date=date(b.ship_date),id='CHECK-'+ship_date;
   if(await read(env,id))fail('此出库日期已有总清单，请打开后追加');
   row={id,kind:'check',department,revision:0,data:{}};
   const data={title:ship_date+' 出库核对',ship_date,status:'open',items:[],rounds:[],exceptions:[],original_total:0,added_total:0,removed_total:0,adjusted_total:0,created_at:t};
   if(b.action==='sop_check_adopt') {
    const legacy=await stmt(env,'SELECT * FROM v2_verify_batches WHERE id=?',required(b.legacy_id,'旧批次编号')).first();if(!legacy)fail('旧批次不存在');
    if(legacy.status==='cancelled')fail('已取消批次不能接入');
    const active=await stmt(env,"SELECT id FROM v2_ops_jobs WHERE related_doc_id=? AND job_type='verify_scan' AND status IN ('pending','working','awaiting_close')",legacy.id).first();if(active)fail('旧批次尚有进行中任务，请先收尾后接入');
    const taken=await stmt(env,"SELECT id FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?",legacy.id).first();if(taken)fail('此批次已经接入，不可重复');
    const items=await all(env,'SELECT * FROM v2_verify_batch_items WHERE batch_id=?',legacy.id);
    const scans=await all(env,"SELECT barcode,COUNT(*) AS qty FROM v2_verify_scan_logs WHERE batch_id=? AND scan_result='ok' GROUP BY barcode",legacy.id);
    const counts=Object.fromEntries(scans.map(x=>[x.barcode,x.qty]));
    data.items=items.map(x=>({id:uid('ITEM'),barcode:x.barcode,customer:x.customer_name,qty:x.planned_box_count||x.planned_qty,scanned:counts[x.barcode]||0,recheck:false,removed:false,pallets:[],added_at:t}));
    data.original_total=data.items.reduce((n,x)=>n+x.qty,0);data.legacy_id=legacy.id;
    const issues=await all(env,"SELECT * FROM v2_verify_scan_logs WHERE batch_id=? AND scan_result!='ok'",legacy.id);
    data.exceptions=issues.map(x=>({id:uid('EX'),barcode:x.barcode,pallet:x.pallet_no,type:x.scan_result,at:x.scanned_at,by:x.worker_name,resolved:false}));
   }
   return await save(env,b,u,row,data);
  }
  if(b.action==='sop_issue_adopt') {
   const issue=await stmt(env,'SELECT * FROM v2_issue_tickets WHERE id=?',required(b.legacy_id,'问题编号')).first();
   if(!issue)fail('问题不存在');
   const running=await stmt(env,"SELECT id FROM v2_issue_handle_runs WHERE issue_id=? AND run_status!='completed'",issue.id).first();if(running&&!b.native)fail('旧版问题仍有未完成处理轮次，请先从原入口收尾');
   const department=issue.biz_class==='direct_ship'||issue.biz_class==='return'?'direct_ship':issue.biz_class;
   permit(u,department,['manager','service','dispatcher']);
   row={id:'ISSUE-'+issue.id,kind:'issue',department,revision:0,data:{}};
   const existing=await read(env,row.id);if(existing&&b.native)return {ok:true,id:existing.id,revision:existing.revision};if(existing)fail('已接入新版，请打开现有记录');
   return await save(env,b,u,row,{title:issue.issue_description,initial_requirement:issue.issue_description,requirement_text:issue.issue_description,legacy_id:issue.id,native:!!b.native,status:['completed','closed'].includes(issue.status)?'closed':issue.status==='cancelled'?'cancelled':issue.status==='responded'?'responded':'open',messages:[],changes:[],requirement_version:0,ack_version:0,created_at:t});
  }
  if(!row)fail('记录不存在');
  const d=structuredClone(row.data),extra=[];
  permit(u,row.department,roles);
  if(b.action.startsWith('sop_task_')) {
   if(row.kind!=='task')fail('记录类型错误');
   permit(u,row.department,['manager','dispatcher','reviewer']);
   const groupedNeed=d.need_id?await read(env,d.need_id):null;
   if(groupedNeed?.data.cargo_groups&&['sop_task_finish','sop_task_complete_review','sop_task_review','sop_task_cancel'].includes(b.action))fail('此计划按资料组完成和审核，请使用分组操作；不得整单重复登记');
   const allowedOwner=u.role==='manager'||d.owner_id===u.id||(d.delegates||[]).includes(u.id);
   if(b.action!=='sop_task_review' && !allowedOwner)fail('仅本任务负责人或获授权人员可操作');
   if(b.action==='sop_task_start') {
    if(groupedNeed?.data.cargo_groups&&taskGroups(groupedNeed.data.cargo_groups,{id:row.id,data:d}).some(g=>g.ack_version!==(g.mapping_version||1)))fail('请先在资料分组中确认最新版本，再恢复作业');
    if(!['assigned','paused','rework'].includes(d.status))fail('当前状态不能开始');
    const present=await sourceActiveWorkers(env,row.id,d.workers);
    for(const w of present)extra.push(stmt(env,"INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)",uid('WS'),row.id,w.id,w.name,t));
    d.status='working';d.started_at=d.started_at||t;d.pause_reason='';
    extra.push(stmt(env,"UPDATE v2_ops_jobs SET status='working',active_worker_count=?,updated_at=? WHERE id=?",present.length,t,row.id));
   } else if(b.action==='sop_task_people') {
    if(!['assigned','working','paused','rework'].includes(d.status))fail('当前状态不能调整人员');
    const workers=validateWorkers(b.workers,b.lead_id);required(b.reason,'调整原因');
    if(d.status==='working') {
     const live=await all(env,"SELECT worker_id AS id FROM v2_ops_job_workers WHERE job_id=? AND left_at=''",row.id);
     const removed=live.filter(w=>!workers.some(n=>n.id===w.id));
     for(const w of removed)extra.push(stmt(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)),leave_reason=? WHERE job_id=? AND worker_id=? AND left_at=''",t,t,text(b.reason),row.id,w.id));
     const present=await sourceActiveWorkers(env,row.id,workers);
     for(const w of present.filter(w=>!live.some(o=>o.id===w.id)))extra.push(stmt(env,"INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)",uid('WS'),row.id,w.id,w.name,t));
     extra.push(stmt(env,'UPDATE v2_ops_jobs SET active_worker_count=?,updated_at=? WHERE id=?',present.length,t,row.id));
    }
    d.workers=workers;d.lead_id=text(b.lead_id);d.last_adjustment=text(b.reason);
   } else if(b.action==='sop_task_delegate') {
    if(u.role!=='manager'&&u.id!==d.owner_id)fail('只有负责人可授权');
    const users=JSON.parse(env.SOP_USERS_JSON||'[]');const delegate=users.find(x=>x.id===b.delegate_id&&x.role==='dispatcher'&&(x.departments||[]).includes(row.department));
    if(!delegate)fail('只能授权已配置的本部门派工人员');
    d.delegates=Array.from(new Set([...(d.delegates||[]),delegate.id]));
   } else if(b.action==='sop_task_pause') {
    if(d.status!=='working')fail('只有作业中可以暂停');d.status='paused';d.pause_reason=required(b.reason,'暂停原因');
    extra.push(...closeSegments(env,row.id,t,'pause:'+d.pause_reason),stmt(env,"UPDATE v2_ops_jobs SET status='paused' WHERE id=?",row.id));
   } else if(b.action==='sop_task_finish'||b.action==='sop_task_complete_review') {
    if(d.status!=='working')fail('只有作业中可以完成');
    const result=b.result||{}; const quantity=positive(result.quantity);const unit=required(result.unit,'成果单位');
    const counts={};for(const field of ['label_count','packed_count','operated_box_count','pallet_count','used_carton_large_count','used_carton_small_count','packed_sku_count','repaired_box_count','reboxed_count','forklift_location_count']){const v=Number(result[field]||0);if(!Number.isSafeInteger(v)||v<0)fail('工耗数量必须为非负整数');counts[field]=v;}
    if(d.need_id){const need=await read(env,d.need_id);assertShippingResult({quantity,unit},workChainEnabled(env)?(await chainAllocation(env,need.data)).links:(need?.data.links||[]));}
    const photoIds=result.location_photos||[];if(!Array.isArray(photoIds)||photoIds.length>8||new Set(photoIds).size!==photoIds.length)fail('货位照片无效');
    const photos=[];for(const id of photoIds){const photo=await stmt(env,"SELECT id,file_key,file_name,content_type FROM v2_attachments WHERE id=? AND related_doc_id=? AND related_doc_type='sop_task' AND attachment_category='location_photo'",text(id),row.id).first();if(!photo||!['image/jpeg','image/png','image/webp'].includes(photo.content_type))fail('货位照片不存在或不属于本任务');photos.push(photo);}
    d.result={quantity,unit,...counts,packed_box_count:counts.packed_count,total_operated_box_count:counts.operated_box_count,customer:d.customer||'',used_forklift:!!result.used_forklift,description:text(result.description),location:text(result.location),location_photos:photos,finished_at:t,by:u.name};
    d.status='awaiting_review';extra.push(...closeSegments(env,row.id,t,'operation_finished'),stmt(env,"UPDATE v2_ops_jobs SET status='awaiting_close',shared_result_json=?,updated_at=? WHERE id=?",JSON.stringify(d.result),t,row.id));
   }
   if(b.action==='sop_task_review'||b.action==='sop_task_complete_review') {
    permit(u,row.department,['manager','reviewer','dispatcher']);if(u.role==='dispatcher'&&!allowedOwner)fail('只能审核本人派发或获授权的任务');if(d.status!=='awaiting_review')fail('仅待审核任务可审核');
    if(!['pass','return'].includes(b.decision))fail('审核结论无效');
    if(b.action==='sop_task_review'&&b.decision==='pass'&&d.need_id){const need=await read(env,d.need_id);assertShippingResult(d.result,workChainEnabled(env)?(await chainAllocation(env,need.data)).links:(need?.data.links||[]));}
    const reason=required(b.reason,'审核说明');d.review={decision:b.decision,reason,by:u.name,at:t,round:d.round};
    d.status=b.decision==='pass'?'completed':'rework';if(b.decision==='return')d.round++;
    extra.push(stmt(env,"UPDATE v2_ops_jobs SET status=?,updated_at=?,finished_at=?,result_summary=? WHERE id=?",b.decision==='pass'?'completed':'pending',t,b.decision==='pass'?t:'',JSON.stringify(d.result),row.id));
    if(b.decision==='pass')extra.push(stmt(env,"INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",uid('RES'),row.id,d.result.unit==='箱'?d.result.quantity:d.result.operated_box_count||0,d.result.unit==='托'?d.result.quantity:d.result.pallet_count||0,d.result.description,JSON.stringify({...d.result,job_type:d.job_type}),u.name,t));
    if(d.need_id){const need=await read(env,d.need_id);if(need){const nd={...need.data,status:b.decision==='pass'?'waiting_customer':'assigned',result:b.decision==='pass'?d.result:null};
      if(b.decision==='pass'&&need.data.source_type==='outbound'){
       nd.links=[{outbound_id:need.data.source_id,quantity:d.result.quantity,unit:d.result.unit,by:u.name,at:t}];nd.status='linked';
       extra.push(stmt(env,"UPDATE v2_outbound_orders SET stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_completed_by=?,stock_operation_result_json=?,status=CASE WHEN uses_stock_operation=1 THEN 'pending_outbound_update' ELSE status END,updated_at=? WHERE id=?",t,u.name,JSON.stringify(d.result),t,need.data.source_id));
      }
      if(b.decision==='pass'&&nd.links?.some(l=>l.phase==='planned')){
       const activeLinks=workChainEnabled(env)?(await chainAllocation(env,nd)).links:nd.links;
       nd.links=nd.links.map(l=>({...l,phase:'confirmed'}));nd.status=activeLinks.reduce((n,l)=>n+l.quantity,0)===d.result.quantity?'linked':'waiting_customer';
       for(const l of activeLinks)extra.push(stmt(env,"UPDATE v2_outbound_orders SET stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_result_json=?,updated_at=? WHERE id=?",t,JSON.stringify(d.result),t,l.outbound_id));
      }
      appendRelated(env,extra,key+'-need',u,need,nd,b.action,t);}}
   } else if(b.action==='sop_task_cancel') {
    if(!['assigned','paused','rework'].includes(d.status))fail('请先暂停；已审核任务不能作废');
    d.status='cancelled';d.cancel_reason=required(b.reason,'作废原因');extra.push(stmt(env,"UPDATE v2_ops_jobs SET status='cancelled',updated_at=? WHERE id=?",t,row.id));
    if(d.need_id){const need=await read(env,d.need_id);if(need)appendRelated(env,extra,key+'-need',u,need,{...need.data,status:'pending',task_id:''},b.action,t);}
   } else if(!['sop_task_start','sop_task_people','sop_task_delegate','sop_task_pause','sop_task_finish'].includes(b.action))fail('未知任务操作');
  } else if(b.action.startsWith('sop_need_')||b.action==='sop_work_material_remove') {
   if(d.cargo_groups&&['sop_need_forward_ready','sop_need_link','sop_need_plan_quantity'].includes(b.action))fail('分组计划请按已审核组安排出库，不能整单确认或占用');
   if(row.kind!=='need')fail('记录类型错误');permit(u,row.department,b.action==='sop_work_material_remove'?['manager','service','dispatcher','reviewer']:['manager','service','dispatcher']);
   if(b.action==='sop_work_material_remove') {
    if(!workChainEnabled(env))fail('功能未启用');if(['closed','cancelled'].includes(d.status))fail('作业已关闭');
    const f=(await workMaterials(env,[row])).find(f=>f.id===b.attachment_id&&f.related_doc_type==='sop_need'&&f.related_doc_id===row.id);
    if(!f)fail('资料不存在；历史来源资料保留只读');
    const feedback=f.material_kind==='work_material';
    if(feedback&&u.scope!=='field'&&!['manager','service'].includes(u.role))fail('仓库反馈请由现场或办公室经办人撤下');
    if(!feedback&&(u.scope==='field'||!['manager','service'].includes(u.role)))fail('客服资料请由办公室撤下');
    if(cargoReferencesFile(d,f.id))fail('资料仍被分组或已确认成果引用，请保留原版本');
    if(f.id===d.details?.attachment_id)fail('当前打托明细请通过上传新版替换');
    d.last_material_change={action:'remove',attachment_id:f.id,file_name:f.file_name,by:u.name,at:t};d.removed_material_ids=[...(d.removed_material_ids||[]),f.id];d.material_version=(d.material_version||0)+1;
    if(!feedback){d.requirement_version=(d.requirement_version||1)+1;extra.push(...notifyMaterialChange(env,row,u,t,'客服作业资料已撤下：'+f.file_name));}
   } else if(b.action==='sop_need_forward_ready') {
    if(!workChainEnabled(env)||d.operation_kind!=='direct_forward'||d.status!=='pending'||d.task_id)fail('仅待确认的直接转发需求可以确认');
    permit(u,row.department,['manager','dispatcher','reviewer']);
    if(d.source_type==='inbound') {const ib=await verifySource(env,'inbound',d.source_id,d.customer);if(!ib||ib.is_deleted||ib.status==='cancelled'||!(ib.unload_completed_at||['received','completed'].includes(ib.status)))fail('请先完成这批货的收货，再确认直接转发');}
    const a=await chainAllocation(env,d),quantity=positive(b.quantity),unit=required(d.planned_unit,'计划单位');
    if(quantity<a.used)fail('可发货数量少于已安排的出库数量，请先调整出库计划');
    d.result={quantity,unit,direct_forward:true,description:'直接转发，未进行加工操作',by:u.name,finished_at:t};d.status=a.used===quantity?'linked':'waiting_customer';
    d.links=(d.links||[]).map(l=>({...l,phase:'confirmed'}));
    for(const l of a.links)extra.push(stmt(env,"UPDATE v2_outbound_orders SET stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_result_json=?,updated_at=? WHERE id=?",t,JSON.stringify(d.result),t,l.outbound_id));
   } else if(b.action==='sop_need_update') {
    if(d.status!=='pending')fail('已派工需求须先暂停并撤回任务；完成后追加请新建关联需求');
    if(workChainEnabled(env)&&b.planned_quantity!==undefined&&text(b.planned_quantity)!==''){const quantity=positive(b.planned_quantity),unit=required(b.planned_unit,'计划单位'),a=await chainAllocation(env,d);if(!d.shipping_basis&&(a.used>quantity||a.links.some(l=>l.unit!==unit)))fail('计划数量和单位不能小于或改变已分配的出库数量');d.planned_quantity=quantity;d.planned_unit=unit;}
    d.requirement_version=(d.requirement_version||1)+1;d.instructions=required(b.instructions,'要求');d.needs_clarification=false;d.location=text(b.location);d.owner=required(b.owner,'责任人');d.deadline=env.SOP_ENVIRONMENT==='staging'?completionDate(b.deadline===undefined?d.deadline:b.deadline,d.deadline):text(b.deadline);
    for(const link of d.links||[])extra.push(stmt(env,'UPDATE v2_outbound_orders SET instruction=?,updated_at=? WHERE id=?',d.instructions,t,link.outbound_id));
    if(workChainEnabled(env))extra.push(...notifyMaterialChange(env,row,u,t,'作业要求已更新，请重新确认',{type:'requirement_update',diff:{instruction:{from:row.data.instructions,to:d.instructions}}}));
    if(d.source_type==='outbound')extra.push(stmt(env,'UPDATE v2_outbound_orders SET instruction=?,updated_at=? WHERE id=?',d.instructions,t,d.source_id));
   } else if(b.action==='sop_need_shipping_basis') {
    extra.push(...await shippingBasisStatements(env,row,d,b,u,t));
   } else if(b.action==='sop_need_plan_quantity') {
    const link=(d.links||[]).find(l=>l.outbound_id===b.outbound_id&&l.phase==='planned');if(!link||d.result)fail('只能调整尚未完成的预关联出库数量');
    const ob=await verifySource(env,'outbound',link.outbound_id,d.customer);if(['shipped','completed','cancelled'].includes(ob.status))fail('出库状态不能调整');
    const quantity=positive(b.quantity);required(b.reason,'调整原因');const allocation=workChainEnabled(env)?await chainAllocation(env,d):{links:d.links,quantity:d.planned_quantity};if(allocation.links.reduce((n,l)=>n+(l.outbound_id===link.outbound_id?quantity:l.quantity),0)>allocation.quantity)fail('超过预计可出库数量');if(b.unit&&b.unit!==link.unit)fail('更改出库单位请使用调整出库数量与单位');link.quantity=quantity;link.adjustment_reason=text(b.reason);
    extra.push(stmt(env,'UPDATE v2_outbound_orders SET planned_box_count=?,planned_pallet_count=?,updated_at=? WHERE id=?',link.unit==='箱'?quantity:0,link.unit==='托'?quantity:0,t,link.outbound_id));
    if(workChainEnabled(env))extra.push(...notifyMaterialChange(env,row,u,t,'出库数量已调整：'+text(b.reason),{type:'shipping_adjustment',diff:id=>id===b.outbound_id?{shipping_quantity:{from:row.data.links.find(l=>l.outbound_id===id).quantity+' '+link.unit,to:quantity+' '+link.unit}}:{}}));
   } else if(b.action==='sop_need_details') {
    if(!d.result)fail('审核通过后再上传作业明细');
    const rows=b.rows;if(!Array.isArray(rows)||!rows.length||rows.length>10000)fail('明细须为1至10000行');
    const parsed=rows.map((r,i)=>({pallet:required(r.pallet,'第'+(i+1)+'行托盘号'),mark:required(r.mark,'箱唛/条码'),quantity:r.quantity===''||r.quantity==null?1:positive(r.quantity),expiry:text(r.expiry),batch:text(r.batch),remark:text(r.remark)}));
    const attachment=await stmt(env,"SELECT id FROM v2_attachments WHERE id=? AND related_doc_id=? AND related_doc_type='sop_need'",text(b.attachment_id),row.id).first();if(!attachment)fail('请先上传原始明细文件');
    d.details={version:(d.details?.version||0)+1,rows:parsed,attachment_id:attachment.id,filename:required(b.filename,'文件名'),uploaded_by:u.name,uploaded_at:t,pallets:new Set(parsed.map(r=>r.pallet)).size};
    d.forwarded=null;
   } else if(b.action==='sop_need_forward') {
    if(!d.details)fail('请先上传作业明细');d.forwarded={by:u.name,at:t,version:d.details.version,note:text(b.note)};
   } else if(b.action==='sop_need_link') {
    if(!d.result || !['waiting_customer','linked'].includes(d.status))fail('需审核通过后关联出库');
    const ob=await verifySource(env,'outbound',required(b.outbound_id,'出库编号'),d.customer);
    if((ob.biz_class==='return'?'direct_ship':ob.biz_class)!==row.department)fail('出库业务分类与作业需求不一致');
    const others=await linkedNeeds(env,ob.id);if(others.some(x=>x.id!==row.id))fail('此出库计划已有其他作业需求，不得重复关联；请核实原要求');
    if(Number(ob.uses_stock_operation)===1||ob.stock_operation_status==='completed')fail('此单已有库内操作要求或记录，请核实后处理，不能重复关联');
    if(d.links.some(l=>l.outbound_id===ob.id))fail('此出库单已关联，请勿重复');
    const quantity=positive(b.quantity),used=workChainEnabled(env)?(await chainAllocation(env,d)).used:d.links.reduce((n,l)=>n+l.quantity,0);
    if(used+quantity>d.result.quantity)fail('关联数量超过本次操作成果；请核实数量和单位');
    d.links.push({outbound_id:ob.id,quantity,unit:d.result.unit,by:u.name,at:t});
    d.status=used+quantity===d.result.quantity?'linked':'waiting_customer';
    extra.push(stmt(env,"UPDATE v2_outbound_orders SET instruction=?,source_inbound_plan_id=?,stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_result_json=?,updated_at=? WHERE id=?",d.instructions,d.source_type==='inbound'?d.source_id:'',d.result.finished_at,JSON.stringify(d.result),t,ob.id));
   } else if(b.action==='sop_need_close') {
    if(!['waiting_customer','linked'].includes(d.status))fail('当前状态不可关闭');
    d.disposition=required(b.reason,'剩余货物去向/交接结果');d.status='closed';
   } else fail('未知需求操作');
  } else if(b.action.startsWith('sop_check_')) {
   if(row.kind!=='check')fail('记录类型错误');permit(u,row.department,['manager','reviewer','service','dispatcher']);
   if(b.action==='sop_check_reopen'){permit(u,row.department,['manager','reviewer']);if(d.status!=='closed')fail('清单未关闭');d.status='open';d.reopen_reason=required(b.reason,'重新开启原因');}
   else {
    if(d.status!=='open')fail('清单已关闭，请负责人重新开启');
    if(b.action==='sop_check_items') {
     permit(u,row.department,['manager','service','dispatcher']);
     if(!Array.isArray(b.items)||!b.items.length||b.items.length>500)fail('每次提交1至500行');
     const seen=new Set();for(const item of b.items){
      const barcode=required(item.barcode,'条码');if(seen.has(barcode))fail('本次上传含重复条码，请先合并');seen.add(barcode);
      const old=d.items.find(x=>x.barcode===barcode&&!x.removed);
      if(old){if(b.duplicate_mode==='skip')continue;if(b.duplicate_mode!=='replace')fail('条码已存在：'+barcode+'；请选择跳过或修改');required(b.reason,'修改原因');
       const qty=positive(item.qty);if(qty!==old.qty||item.customer!==old.customer){old.previous_scanned=old.scanned;old.scanned=0;old.recheck=true;d.adjusted_total=(d.adjusted_total||0)+qty-old.qty;old.qty=qty;old.customer=required(item.customer,'客户');}
      }else {const qty=positive(item.qty);d.items.push({id:uid('ITEM'),barcode,customer:required(item.customer,'客户'),qty,scanned:0,recheck:false,removed:false,pallets:[],added_at:t}); if(d.rounds.length)d.added_total+=qty;else d.original_total+=qty;}
     }
    } else if(b.action==='sop_check_remove') {
     permit(u,row.department,['manager','service','dispatcher']);
     const item=d.items.find(x=>x.id===b.item_id&&!x.removed);if(!item)fail('条目不存在');
     item.removed=true;item.removal_reason=required(b.reason,'撤销原因');if(item.scanned||item.previous_scanned)item.disposition=required(b.disposition,'已核对货物去向');d.removed_total+=item.qty;
    } else if(b.action==='sop_check_move') {
     permit(u,row.department,['manager','service','dispatcher']);const shipDate=date(b.ship_date),item=d.items.find(x=>x.id===b.item_id&&!x.removed);
     if(!item)fail('条目不存在');if(shipDate===d.ship_date)fail('目标日期相同');
     const target=await read(env,'CHECK-'+shipDate);if(!target||target.data.status!=='open')fail('请先建立并打开目标日期清单');
     if(target.data.items.some(x=>x.barcode===item.barcode&&!x.removed))fail('目标日期已有该条码，请核实避免重复');
     const reason=required(b.reason,'变更原因'),td=structuredClone(target.data);
     td.items.push({...item,id:uid('ITEM'),scanned:0,recheck:true,pallets:[],added_at:t,moved_from:row.id});td.added_total+=item.qty;
     item.removed=true;item.removal_reason='移至 '+shipDate+'：'+reason;item.disposition=required(b.disposition,'货物实际去向');d.removed_total+=item.qty;
    appendRelated(env,extra,key+'-target',u,target,td,b.action,t);
     syncCheck(env,extra,target,td,u,t,{action:'sop_check_move_target'});
    } else if(b.action==='sop_check_scan') {
     permit(u,row.department,['manager','reviewer','dispatcher']);const barcode=required(b.barcode,'条码'),pallet=required(b.pallet,'托盘/散箱位置');
     const item=d.items.find(x=>x.barcode===barcode&&!x.removed);
     if(!item||item.scanned>=item.qty)d.exceptions.push({id:uid('EX'),barcode,pallet,type:item?'overflow':'not_found',at:t,by:u.name,resolved:false});
     else {item.scanned++;item.pallets.push(pallet);item.recheck=item.scanned!==item.qty;}
    } else if(b.action==='sop_check_resolve') {
     permit(u,row.department,['manager','reviewer']);const ex=d.exceptions.find(x=>x.id===b.exception_id);if(!ex)fail('异常不存在');ex.resolved=true;ex.resolution=required(b.reason,'异常处理结果');ex.reviewed_by=u.name;ex.reviewed_at=t;
    } else if(b.action==='sop_check_round') {
     permit(u,row.department,['manager','reviewer']);if(!['11:00','13:00','16:00','追加'].includes(b.slot))fail('核对轮次无效');
     d.rounds.push({slot:b.slot,at:t,by:u.name,summary:checkSummary(d),note:required(b.reason,'本轮交接说明')});
    } else if(b.action==='sop_check_close') {
     permit(u,row.department,['manager','reviewer']);const summary=checkSummary(d);if(!summary.planned||summary.pending||summary.unresolved)fail('存在待核对或未复核异常，不能关闭');
     d.handover=required(b.reason,'最终交接人及位置');d.status='closed';d.closed_at=t;
    } else fail('未知核对操作');
   }
  } else if(b.action.startsWith('sop_issue_')) {
   if(row.kind!=='issue')fail('记录类型错误');
   if(d.status==='cancelled')fail('问题已作废');
   if(b.action==='sop_issue_append'||b.action==='sop_issue_change') {
    permit(u,row.department,['manager','service']);const message=required(b.message,'内容');
    d.requirement_version++;
    d.requirement_text=b.action==='sop_issue_change'?message:(d.requirement_text||d.title)+'\n追加：'+message;
    d.changes.push({version:d.requirement_version,text:message,mode:b.action==='sop_issue_change'?'修改':'追加',by:u.name,at:t});
    d.status='open';
   } else if(b.action==='sop_issue_ack') {
    permit(u,row.department,['manager','dispatcher','reviewer']);if(Number(b.requirement_version)!==d.requirement_version)fail('存在更新版本，请重新查看');d.ack_version=d.requirement_version;d.ack_by=u.name;d.ack_at=t;
   } else if(b.action==='sop_issue_feedback') {
    permit(u,row.department,['manager','dispatcher','reviewer']);if(d.status==='closed')fail('问题已关闭；新要求请由客服明确重新开启');if(d.ack_version!==d.requirement_version)fail('请先确认最新要求');
    d.messages.push({text:required(b.message,'反馈'),by:u.name,at:t,feedback:true});d.status='responded';
   } else if(b.action==='sop_issue_close') {
    permit(u,row.department,['manager','service']);if(d.status!=='responded'||d.ack_version!==d.requirement_version)fail('需仓库反馈并确认最新要求');
     if(d.native){const active=await stmt(env,"SELECT id FROM v2_issue_handle_runs WHERE issue_id=? AND run_status!='completed' LIMIT 1",d.legacy_id).first(),working=await stmt(env,"SELECT w.id FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE j.related_doc_type='issue' AND j.related_doc_id=? AND w.left_at='' LIMIT 1",d.legacy_id).first();if(active||working)fail('仓库仍在处理，请先完成最新处理轮次及交接');extra.push(stmt(env,"UPDATE sop_records SET revision=CASE WHEN NOT EXISTS(SELECT 1 FROM v2_issue_handle_runs WHERE issue_id=? AND run_status!='completed') AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE j.related_doc_type='issue' AND j.related_doc_id=? AND w.left_at='') THEN revision ELSE NULL END WHERE id=?",d.legacy_id,d.legacy_id,row.id));}
     d.status='closed';
   } else if(b.action==='sop_issue_request_accounting'||b.action==='sop_issue_confirm_accounted') {
    permit(u,row.department,b.action==='sop_issue_request_accounting'?['manager','service','dispatcher','reviewer']:['manager','service']);
    if(b.action==='sop_issue_confirm_accounted'&&u.scope==='field')fail('请由客服确认已记账');
    d.accounting=await issueAccounting(env,d.legacy_id);
    row.data.accounting=structuredClone(d.accounting);
    if(Number(d.accounting.accounted)===1)fail('已确认记账，不能重复提交或重置');
    if(b.action==='sop_issue_request_accounting'){
     const first=Number(d.accounting.accounting_required)!==1;
     d.accounting={...d.accounting,accounting_required:1,accounting_note:text(b.note),accounting_required_by:first?u.name:d.accounting.accounting_required_by,accounting_required_at:first?t:d.accounting.accounting_required_at};
     extra.push(stmt(env,'UPDATE v2_issue_tickets SET accounting_required=1,accounting_required_by=?,accounting_required_at=?,accounting_note=?,updated_at=? WHERE id=?',d.accounting.accounting_required_by,d.accounting.accounting_required_at,d.accounting.accounting_note,t,d.legacy_id));
    }else{
     if(Number(d.accounting.accounting_required)!==1)fail('尚未提示客服记账');
     if(!['responded','closed'].includes(d.status))fail('请先完成仓库反馈再确认记账');
     d.accounting={...d.accounting,accounted:1,accounted_by:u.name,accounted_at:t};
     extra.push(stmt(env,'UPDATE v2_issue_tickets SET accounted=1,accounted_by=?,accounted_at=?,updated_at=? WHERE id=?',u.name,t,t,d.legacy_id));
    }
   } else if(b.action==='sop_issue_cancel') {
    permit(u,row.department,['manager','service']);const active=await stmt(env,"SELECT id FROM v2_issue_handle_runs WHERE issue_id=? AND run_status!='completed' LIMIT 1",d.legacy_id).first(),working=await stmt(env,"SELECT w.id FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE j.related_doc_type='issue' AND j.related_doc_id=? AND w.left_at='' LIMIT 1",d.legacy_id).first();if(active||working)fail('仓库仍在处理，请先完成交接再作废');d.cancel_reason=required(b.reason,'作废原因');
    extra.push(stmt(env,"UPDATE sop_records SET revision=CASE WHEN NOT EXISTS(SELECT 1 FROM v2_issue_handle_runs WHERE issue_id=? AND run_status!='completed') AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE j.related_doc_type='issue' AND j.related_doc_id=? AND w.left_at='') THEN revision ELSE NULL END WHERE id=?",d.legacy_id,d.legacy_id,row.id));d.status='cancelled';
   } else fail('未知问题操作');
   if(d.native){
    if(b.action==='sop_issue_change'||b.action==='sop_issue_append'){
     extra.push(stmt(env,"UPDATE v2_issue_tickets SET issue_description=?,status=CASE WHEN status='processing' THEN status ELSE 'rework_required' END,rework_note=?,updated_at=? WHERE id=?",d.requirement_text,text(b.message),t,d.legacy_id));
     extra.push(stmt(env,"INSERT INTO v2_issue_rework_requests(id,issue_id,request_note,requested_by,status,created_at,related_run_id) VALUES(?,?,?,?, 'open',?,'')",uid('RWK'),d.legacy_id,text(b.message),u.name,t));
    }
    if(b.action==='sop_issue_feedback'){
     const runs=await all(env,"SELECT * FROM v2_issue_handle_runs WHERE issue_id=? AND run_status!='completed'",d.legacy_id);
     for(const run of runs){
      extra.push(...closeSegments(env,run.job_id,t,'issue_feedback'));
      extra.push(stmt(env,"UPDATE v2_ops_jobs SET status='completed',updated_at=? WHERE id=?",t,run.job_id));
      extra.push(stmt(env,"UPDATE v2_issue_handle_runs SET ended_at=?,feedback_text=?,run_status='completed',minutes_worked=(SELECT COALESCE(SUM(minutes_worked),0) FROM v2_ops_job_workers WHERE job_id=?) WHERE id=?",t,text(b.message),run.job_id,run.id));
     }
     extra.push(stmt(env,"UPDATE v2_issue_tickets SET status='responded',latest_feedback_text=?,updated_at=?,total_minutes_worked=(SELECT COALESCE(SUM(minutes_worked),0) FROM v2_issue_handle_runs WHERE issue_id=?) WHERE id=?",text(b.message),t,d.legacy_id,d.legacy_id));
     extra.push(stmt(env,"UPDATE v2_issue_rework_requests SET status='resolved',resolved_at=?,resolved_by=? WHERE issue_id=? AND status='open'",t,u.name,d.legacy_id));
    }
    if(b.action==='sop_issue_close')extra.push(stmt(env,"UPDATE v2_issue_tickets SET status='completed',updated_at=? WHERE id=?",t,d.legacy_id));
    if(b.action==='sop_issue_cancel')extra.push(stmt(env,"UPDATE v2_issue_tickets SET status='cancelled',updated_at=? WHERE id=?",t,d.legacy_id));
   }
  } else fail('未知操作');
  return await save(env,b,u,row,d,extra);
 }catch(e){
  // A lost response retried with the same request id never applies the mutation twice.
  if(b.client_req_id){const cached=await stmt(env,'SELECT actor_id,action,result_json FROM sop_events WHERE request_id=?',b.client_req_id).first();if(cached&&cached.actor_id===u.id&&cached.action===b.action)return JSON.parse(cached.result_json);}
  return {ok:false,error:/revision_conflict/.test(e.message)?'记录已更新，请刷新后再操作':e.message};
 }
}
function appendRelated(env,extra,key,u,row,data,action,t){
 extra.push(stmt(env,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',key,row.id,row.revision,action,u.id,u.name,JSON.stringify(row.data),JSON.stringify(data),'{}',t));
 extra.push(stmt(env,'UPDATE sop_records SET revision=revision+1,state=?,updated_at=? WHERE id=?',JSON.stringify(data),t,row.id));
}
function validateWorkers(workers,lead){
 if(!Array.isArray(workers)||!workers.length||workers.length>50)fail('请扫描1至50名操作人员');
 const ids=new Set();return workers.map(w=>{const id=required(w.id,'工号'),name=required(w.name,'姓名');if(ids.has(id))fail('重复工牌');ids.add(id);return{id,name};}).map((w,i,a)=>{if(!a.some(x=>x.id===lead))fail('主操作员必须在参与人员中');return w;});
}
async function needGroups(env,u,b){
 const departments=u.role==='manager'?[]:u.departments||[];
 const filter=u.role==='manager'?'':` AND department IN (${departments.map(()=>'?').join(',')||"''"})`;
 let scope='',scopeArgs=[];
 const groupExpr="json_array(COALESCE(NULLIF(json_extract(state,'$.source_type'),''),'standalone'),COALESCE(NULLIF(json_extract(state,'$.source_id'),''),NULLIF(json_extract(state,'$.supply_chain_no'),''),id),CASE WHEN json_extract(state,'$.source_type')='inbound' THEN '' ELSE json_extract(state,'$.customer') END)";
 if(b.group_key){scope=' AND '+groupExpr+'=?';scopeArgs=[b.group_key];}
 else if(b.source_id){scope=" AND json_extract(state,'$.source_id')=?";scopeArgs=[b.source_id];if(b.source_type){scope+=" AND json_extract(state,'$.source_type')=?";scopeArgs.push(b.source_type);}}
 else if(b.need_id){scope=' AND '+groupExpr+'=(SELECT '+groupExpr+" FROM sop_records WHERE id=? AND kind='need')";scopeArgs=[b.need_id];}
 const rows=await all(env,`SELECT * FROM sop_records WHERE kind='need'${filter}${scope} ORDER BY updated_at DESC,id`,...departments,...scopeArgs);
 const groups=new Map();
 const numbered=await recordNumbers(env,rows.map(row=>publicState({...row,data:JSON.parse(row.state)})));
 for(const x of numbered){
  const key=JSON.stringify([x.source_type||'standalone',x.source_id||x.supply_chain_no||x.id,x.source_type==='inbound'?'':x.customer]);
  if(!groups.has(key))groups.set(key,{key,source_type:x.source_type||'standalone',source_id:x.source_id||'',customer:x.customer,display_no:x.source_display_no||x.supply_chain_no||x.display_no||'未关联单据',updated_at:x.updated_at,items:[]});
  groups.get(key).items.push(x);
 }
 let items=[...groups.values()];
 if(b.need_id)items=items.filter(g=>g.items.some(x=>x.id===b.need_id));
 if(b.source_id)items=items.filter(g=>g.source_id===b.source_id&&(!b.source_type||g.source_type===b.source_type));
 if(b.group_key)items=items.filter(g=>g.key===b.group_key);
 const total=items.length,offset=Math.max(0,Math.floor(Number(b.offset)||0));items=items.slice(offset,offset+50);
 const ibIds=[...new Set(items.filter(g=>g.source_type==='inbound'&&g.source_id).map(g=>g.source_id))];
 const obIds=[...new Set(items.filter(g=>g.source_type==='outbound'&&g.source_id).map(g=>g.source_id))];
 const inClause=ids=>ids.map(()=>'?').join(',');
 const reads=await env.DB.batch([
  stmt(env,'SELECT id,display_no,customer,plan_date,expected_arrival,cargo_summary,purpose,remark,status,is_deleted FROM v2_inbound_plans WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(ibIds)),
  stmt(env,'SELECT plan_id,unit_type,planned_qty,remark FROM v2_inbound_plan_lines WHERE plan_id IN (SELECT value FROM json_each(?)) ORDER BY line_no',JSON.stringify(ibIds)),
  stmt(env,'SELECT id,display_no FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(obIds))]);
 const [plans,lines,outbounds]=reads.map(r=>r.results||[]);
 for(const g of items){
  if(g.source_type==='inbound'){const p=plans.find(p=>p.id===g.source_id);if(p){Object.assign(g,{plan:p,customer:p.customer||g.customer,display_no:p.display_no||'入库计划单号待补充',cargo_summary:p.cargo_summary,plan_date:p.plan_date,expected_arrival:p.expected_arrival});g.lines=lines.filter(l=>l.plan_id===g.source_id);}}
  if(g.source_type==='outbound')g.display_no=outbounds.find(p=>p.id===g.source_id)?.display_no||'出库计划单号待补充';
  g.items.sort((a,b)=>(a.created_at||'').localeCompare(b.created_at||'')||(a.instruction_order||0)-(b.instruction_order||0)||a.title.localeCompare(b.title,'zh',{numeric:true}));
  const active=g.items.filter(x=>x.status!=='cancelled'&&!x.source_cancelled);g.completed=active.filter(x=>!!x.result||x.status==='closed').length;g.count=active.length;
  g.status=!active.length?'cancelled':g.completed===active.length?'completed':active.some(x=>x.status!=='pending')?'working':'pending';
 }
 return {ok:true,items,total,offset,more:offset+50<total};
}
async function dashboard(env,u,b){
 const days=date(b.date||new Date(Date.now()+9*3600000).toISOString().slice(0,10));
 const start=new Date(days+'T00:00:00+09:00').toISOString(),end=new Date(Date.parse(start)+86400000).toISOString();
 const dep=u.role==='manager'?'':` AND department IN (${(u.departments||[]).map(()=>'?').join(',')||"''"})`;
 const args=u.role==='manager'?[]:u.departments||[];
 const rows=await all(env,`SELECT * FROM sop_records WHERE 1=1${dep}`,...args);
 const records=rows.map(r=>publicState({...r,data:JSON.parse(r.state)}));
 const tasks=records.filter(r=>r.kind==='task');
 const nativeJobs=await all(env,`SELECT j.* FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE s.kind='dispatch'${dep.replaceAll('department','s.department')}`,...args);
 const segments=await all(env,`SELECT w.* FROM v2_ops_job_workers w JOIN sop_records s ON w.job_id=s.id WHERE w.joined_at<? AND (w.left_at='' OR w.left_at>?)${dep.replaceAll('department','s.department')}`,end,start,...args);
 const minutes=segments.reduce((n,s)=>n+Math.max(0,(Math.min(Date.parse(s.left_at||now()),Date.parse(end))-Math.max(Date.parse(s.joined_at),Date.parse(start)))/60000),0);
 const alerts=records.filter(r=>(r.kind==='task'&&['awaiting_review','paused','rework'].includes(r.status))||(r.kind==='need'&&['pending','waiting_customer'].includes(r.status))||(r.kind==='issue'&&r.status!=='closed'));
 const completed=tasks.filter(r=>r.status==='completed'&&r.review?.at>=start&&r.review.at<end);
 const outputs={};for(const r of completed){const key=r.department+' / '+r.job_type+' / '+r.result.unit;outputs[key]=(outputs[key]||0)+r.result.quantity;}
 const allSegments=await all(env,`SELECT w.* FROM v2_ops_job_workers w JOIN sop_records s ON w.job_id=s.id WHERE 1=1${dep.replaceAll('department','s.department')}`,...args);
 const metrics=[['label_count','贴标','张'],['packed_count','打包','件'],['operated_box_count','操作箱数','箱'],['pallet_count','打托','托']];
 const ranking=new Map(),reported=new Map();
 function allocate(task,result,bucket,verification){
  const crew=[...new Map(allSegments.filter(w=>w.job_id===task.id).map(w=>[w.worker_id,{id:w.worker_id,name:w.worker_name}])).values()];
  if(!crew.length)return;
  const values=[['完成量',Number(result.quantity)||0,result.unit||'单'],...metrics.map(([k,label,unit])=>[label,Number(result[k])||0,unit])];
  for(const [metric,total,unit] of values){if(!total)continue;for(const w of crew){const key=[task.department,task.job_type,metric,unit,w.id,...(bucket===reported?[verification]:[])].join('|');const x=bucket.get(key)||{worker_id:w.id,worker_name:w.name,department:task.department,job_type:task.job_type,metric,unit,quantity:0,tasks:[],verification};x.quantity+=total/crew.length;x.tasks.push(task.id);bucket.set(key,x);}}
 }
 for(const r of tasks.filter(r=>r.result?.finished_at>=start&&r.result.finished_at<end&&['awaiting_review','completed'].includes(r.status)))allocate(r,r.result,reported,r.status==='completed'?'已审核':'待审核');
 for(const r of completed)allocate(r,r.result,ranking,'已审核');
 const nativeResults=await all(env,`SELECT r.* FROM v2_ops_job_results r JOIN sop_records s ON s.id=r.job_id WHERE s.kind='dispatch'${dep.replaceAll('department','s.department')}`,...args);
 for(const j of nativeJobs.filter(j=>j.status==='completed'&&j.finished_at>=start&&j.finished_at<end)){
  const r={quantity:0,unit:'箱',label_count:0,packed_count:0,operated_box_count:0,pallet_count:0};
    for(const v of effectiveResults(nativeResults.filter(r=>r.job_id===j.id))){let d={};try{d=JSON.parse(v.result_json||'{}');}catch{}r.quantity+=Number(v.box_count)||0;for(const [k] of metrics)r[k]+=Number(d[k]??(k==='packed_count'?d.packed_box_count:k==='pallet_count'?v.pallet_count:k==='operated_box_count'?(d.total_operated_box_count??v.box_count):0))||0;}
  const task={...j,department:records.find(r=>r.id===j.id)?.department||j.biz_class};allocate(task,r,reported,'原流程已完成');allocate(task,r,ranking,'原流程已完成');
 }
 const ranked=[...ranking.values()].sort((a,b)=>b.quantity-a.quantity);
 const roster=[...new Map(allSegments.map(s=>[s.worker_id,{worker_id:s.worker_id,worker_name:s.worker_name}])).values()].map(w=>{const active=allSegments.find(s=>s.worker_id===w.worker_id&&!s.left_at);const task=records.find(r=>r.id===active?.job_id);return {...w,current_job_id:active?.job_id||'',current_task:task?.title||'',status:active?'作业中':'无进行中记录（不等于空闲）'};});
 const numberedRoster=await jobNumbers(env,roster,'current_job_id');
 return {ok:true,date:days,scope:'整单完成后更新产量；多人按该任务实际参与人数均分（分配产量，不是个人扫描实测）。按业务、作业、指标及单位分别排名。已登记人员无任务不等于空闲；外部系统Excel尚未导入的成果不包含在内。',roster:numberedRoster,rankings:ranked,reported_outputs:[...reported.values()],
  working:tasks.filter(r=>r.status==='working').length+nativeJobs.filter(x=>x.status==='working').length,
  active_people:new Set(segments.filter(s=>!s.left_at).map(s=>s.worker_id)).size,
  live:await jobNumbers(env,segments.filter(s=>!s.left_at).map(s=>({worker_id:s.worker_id,worker_name:s.worker_name,job_id:s.job_id,joined_at:s.joined_at})),'job_id'),completed:completed.length,person_hours:Math.round(minutes/6)/10,outputs,
  alerts:await recordNumbers(env,alerts.map(r=>({id:r.id,kind:r.kind,title:r.title,status:r.status,department:r.department,owner:r.owner||'',updated_at:r.updated_at,revision:r.revision}))),
  data_quality:await jobNumbers(env,tasks.filter(r=>r.status==='awaiting_review'||(r.status==='working'&&Date.parse(now())-Date.parse(r.started_at)>12*3600000)).map(r=>({id:r.id,title:r.title,reason:r.status==='awaiting_review'?'待审核':'计时超过12小时，请核实'})))};
}
// Defense in depth: legacy clients may finish old tasks but cannot alter SOP-owned tasks.
export async function linkedCheck(env,id){
 if(env.SOP_UPGRADE_ENABLED!=='true')return null;
 const row=await stmt(env,"SELECT * FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?",id).first();return row?publicState({...row,data:JSON.parse(row.state)}):null;
}
export async function linkedNeeds(env,id,knownRows){
 if(env.SOP_UPGRADE_ENABLED!=='true')return [];
 const rows=knownRows||await all(env,"SELECT * FROM sop_records WHERE kind='need' AND (json_extract(state,'$.source_id')=? OR EXISTS(SELECT 1 FROM json_each(json_extract(state,'$.links')) l WHERE json_extract(l.value,'$.outbound_id')=?))",id,id);
 return recordNumbers(env,rows.map(r=>publicState({...r,data:JSON.parse(r.state)})));
}
export async function guardLegacy(b,env){
 if(env.SOP_UPGRADE_ENABLED!=='true')return null;
 if(loadTripEnabled(env)&&b.action==='v2_outbound_load_finish'&&b.job_id&&await stmt(env,"SELECT id FROM v2_ops_jobs WHERE id=? AND job_type='load_outbound' AND status='completed'",b.job_id).first())return null;
 if(loadTripEnabled(env)&&b.job_id&&await stmt(env,'SELECT job_id FROM ck_load_trips WHERE job_id=?',b.job_id).first()){
  if(b.action==='v2_outbound_load_finish')return null; // The truck handler checks every order, including retries after shipment.
  if(['v2_ops_job_finish','v2_ops_job_manual_finalize','v2_ops_job_result_update'].includes(b.action))return '请从本车装货任务逐单核对并结束';
 }
 const chainError=await guardWorkChain(b,env);if(chainError)return chainError;
 const task=b.job_id||b.active_job_id;
 if(task&&['v2_ops_job_finish','v2_ops_job_manual_finalize','v2_ops_job_result_update'].includes(b.action)&&await stmt(env,'SELECT job_id FROM ck_unload_trips WHERE job_id=?',task).first())return '请从整车卸货任务中逐单填写结果并完成 / 차량 하차 작업에서 완료하세요';
 const batch=b.batch_id||b.id;
 if(b.action?.startsWith('v2_verify_')&& !/list|detail/.test(b.action)){
  let id=batch;if(!id&&task){const j=await stmt(env,'SELECT related_doc_id FROM v2_ops_jobs WHERE id=?',task).first();id=j?.related_doc_id;}
  if(id&&await stmt(env,"SELECT id FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?",id).first())return '此批次已接入按日期核对，请使用新版';
 }
 const order=b.order_id||b.id||b.related_doc_id;
 if(order && /outbound_stock_op|bulk_op_job_start|outbound_order_update|outbound_load_start/.test(b.action||'')) {
  const needs=await all(env,"SELECT state FROM sop_records WHERE kind='need' AND (json_extract(state,'$.source_id')=? OR EXISTS(SELECT 1 FROM json_each(json_extract(state,'$.links')) l WHERE json_extract(l.value,'$.outbound_id')=?))",order,order);
  if(needs.length&&(workChainEnabled(env)?/outbound_load_start/.test(b.action):/outbound_load_start|outbound_order_update_status/.test(b.action)) && needs.some(n=>{const d=JSON.parse(n.state);return !(d.result&&(d.links||[]).some(l=>l.outbound_id===order))&&!['linked','closed'].includes(d.status);}))return '关联作业尚未审核完成，不得进入装货';
  if(needs.length&&/stock_op|bulk_op/.test(b.action))return '此出库单的作业要求已统一到关联作业，请在作业需求执行，不能重复操作';
  if(needs.length&&(Object.hasOwn(b,'instruction')||Object.hasOwn(b,'uses_stock_operation'))){
   const old=await stmt(env,'SELECT instruction,uses_stock_operation FROM v2_outbound_orders WHERE id=?',order).first();
   if(Object.hasOwn(b,'instruction')&&text(b.instruction)!==text(old.instruction))return '操作要求请从关联作业修改';
   if(Object.hasOwn(b,'uses_stock_operation')&&Number(b.uses_stock_operation)!==Number(old.uses_stock_operation))return '此单已关联统一作业，不能修改旧版操作开关';
  }
 }
  if(task && /start|join|finish|leave|resume|finalize|correct|force|result_update/.test(b.action||'')){
  const r=await read(env,task);if(r?.kind==='task')return '此任务由负责人派工，请在现场执行的派工与审核中处理';
 }
 if(b.action?.startsWith('v2_issue_')&&!/detail|list/.test(b.action)){
  let id=b.id||b.issue_id;if(!id&&b.run_id){const run=await stmt(env,'SELECT issue_id FROM v2_issue_handle_runs WHERE id=?',b.run_id).first();id=run?.issue_id;}
  if(id){const row=await read(env,'ISSUE-'+id);if(row){
   if(row.data.native&&['v2_issue_handle_start','v2_issue_handle_resume'].includes(b.action))return null;
   return '请使用本问题详情中的追加、修改、确认和反馈按钮，确保按最新要求处理';
  }}
 }
 return null;
}
// Called inside the outbound creation transaction. The outgoing plan references this
// canonical requirement from birth; there is no second operation to execute or count.
export async function outboundNeedStatements(env,body,id,display,t){
 if(workChainEnabled(env))return chainOutboundStatements(env,body,id,t);
 if(env.SOP_UPGRADE_ENABLED==='true'&&body.sop_existing_need_id){
  const need=await read(env,text(body.sop_existing_need_id));
  if(!need||need.kind!=='need'||!need.data.result||!['waiting_customer','linked'].includes(need.data.status))fail('请选择已审核通过的作业需求');
  if(need.data.customer!==text(body.customer))fail('出库客户与作业需求不一致');
  if(need.department!==(body.biz_class==='return'?'direct_ship':body.biz_class))fail('出库业务分类与作业需求不一致');
  if(Number(body.uses_stock_operation)===1)fail('已完成的作业不能再次要求库内操作；追加操作请另建需求');
  const quantity=positive(body.sop_link_quantity),d=structuredClone(need.data);
  const used=d.links.reduce((n,x)=>n+x.quantity,0);if(used+quantity>d.result.quantity)fail('出库关联数量超过剩余操作成果');
  d.links.push({outbound_id:id,quantity,unit:d.result.unit,by:text(body.created_by),at:t});d.status=used+quantity===d.result.quantity?'linked':'waiting_customer';
  const extra=[];appendRelated(env,extra,'LINK-'+id,{id:env.SOP_REQUEST_USER?.id||'system:outbound',name:text(body.created_by)},need,d,'sop_need_link',t);
  extra.push(stmt(env,"UPDATE v2_outbound_orders SET instruction=?,source_inbound_plan_id=?,stock_operation_status='completed',stock_operation_completed_at=?,stock_operation_result_json=? WHERE id=?",d.instructions,d.source_type==='inbound'?d.source_id:'',d.result.finished_at,JSON.stringify(d.result),id));
  return extra;
 }
 if(env.SOP_UPGRADE_ENABLED!=='true'||env.SOP_AUTO_OUTBOUND!=='true'||env.SOP_ACCEPT_NEW==='false'||Number(body.uses_stock_operation)!==1)return [];
 const department=body.biz_class==='return'?'direct_ship':body.biz_class;
 const enabled=String(env.SOP_ROLLOUT_DEPARTMENTS||'').split(',').map(x=>x.trim());
 if(!enabled.includes(department))return [];
 const needId='NEED-'+id,data={title:'出库关联作业 '+display,customer:text(body.customer),source_type:'outbound',source_id:id,
  instructions:text(body.instruction),owner:text(body.sop_owner)||'工单处理员待接单',location:'',deadline:text(body.expected_ship_at),status:'pending',links:[],created_at:t,created_by:text(body.created_by),needs_clarification:!text(body.instruction)};
 const result={ok:true,id:needId,revision:1};
 return [stmt(env,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)','AUTO-'+id,needId,0,'sop_need_auto','system:outbound',text(body.created_by)||'出库计划自动生成','{}',JSON.stringify(data),JSON.stringify(result),t),
 stmt(env,'INSERT INTO sop_records VALUES(?,?,?,?,?,?)',needId,'need',1,department,JSON.stringify(data),t)];
}
