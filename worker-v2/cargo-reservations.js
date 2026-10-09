import {planAuditStatements} from './office-operator.js';
import {chainDate,chainNeed,chainEvent,workChainEnabled} from './work-chain.js';
import {nextOutboundDisplayNo} from './outbound-number.js';
import {allProcessesApproved} from './cargo-processes.js';
const q=(env,sql,...v)=>env.DB.prepare(sql).bind(...v),text=v=>String(v??'').trim();
const frozen=['cancelled','shipped','loaded','completed','loading'];
const modes=['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
export function reservationFields(value){
 if(!value||typeof value!=='object')throw Error('请填写本组出库预约');
 if(!modes.includes(value.outbound_mode))throw Error('请选择本组出库方式');
 return {expected_ship_at:chainDate(value.expected_ship_at),outbound_mode:value.outbound_mode,destination:text(value.destination).slice(0,4000),po_no:text(value.po_no).slice(0,240),outbound_requirement:text(value.outbound_requirement).slice(0,4000)};
}

async function reservationGuard(env,needId,order){
 if(!needId)throw Error('缺少预约所属作业');
 const trip=!!await q(env,"SELECT name FROM sqlite_master WHERE type='table' AND name='ck_load_order_links'").first();
 const active="SELECT 1 FROM v2_ops_jobs j WHERE j.status IN ('working','awaiting_close','pending','paused') AND (j.linked_outbound_order_id=? OR (j.related_doc_type='outbound_order' AND j.related_doc_id=?)"+(trip?" OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=?)":"")+")";
 const args=[order.id,order.id,...(trip?[order.id]:[])];if(await q(env,active,...args).first())throw Error('当前有进行中的出库作业，不能修改或取消预约');
 return q(env,"UPDATE sop_records SET revision=CASE WHEN EXISTS(SELECT 1 FROM v2_outbound_orders WHERE id=? AND status=? AND updated_at IS ?) AND NOT EXISTS("+active+") THEN revision ELSE NULL END WHERE id=?",order.id,order.status,order.updated_at,...args,needId);
}

function amendmentAudit(env,order,next,actor,t){
 const labels={expected_ship_at:'预计出库日期',outbound_mode:'出库方式',destination:'去向',po_no:'PO',outbound_requirement:'运输交接说明',status:'状态'},diff=Object.fromEntries(Object.entries(next).filter(([key,value])=>String(order[key]??'')!==String(value??'')).map(([key,value])=>[key,{from:order[key]??'',to:value}]));
 if(!Object.keys(diff).length)return [];
 const ack=next.status==='cancelled'?0:1,revision=Number(order.revision_no||0)+1,summary=Object.entries(diff).map(([key,value])=>labels[key]+'：'+(value.from||'—')+' → '+(value.to||'—')).join('；');
 return [q(env,"UPDATE v2_outbound_orders SET revision_no=?,last_modified_by=?,last_modified_at=?,warehouse_ack_required=?,warehouse_ack_by='',warehouse_ack_at='' WHERE id=?",revision,actor.name,t,ack,order.id),q(env,"INSERT INTO v2_outbound_order_change_logs(id,order_id,revision_no,change_type,changed_by,changed_at,diff_json,summary_text,warehouse_ack_required,warehouse_ack_by,warehouse_ack_at,ack_source,created_at) VALUES(?,?,?,?,?,?,?,?,?,'','','',?)",crypto.randomUUID(),order.id,revision,'group_reservation',actor.name,t,JSON.stringify(diff),summary,ack,t),...planAuditStatements(env,'outbound',order.id,'update',t)];
}

const groupSnapshot=g=>({group_id:g.id,group_name:g.name,range:g.range,marks:g.marks,identifier:g.identifier,input_box_count:g.box_count,mapping_version:g.mapping_version||1,documents:g.documents||[],public_documents:g.public_documents||[]});
const allocation=g=>{const o=g.outputs?.find(x=>x.id===g.current_output_id);return o&&allProcessesApproved(g)&&o.status==='approved'?{...groupSnapshot(g),output_id:o.id,output_version:o.version,mapping_version:o.mapping_version,quantity:o.quantity,unit:o.unit,input_box_count:o.input_box_count,range:o.range,marks:o.marks,identifier:o.identifier,documents:o.documents,public_documents:o.public_documents}:null;};
export async function createGroupReservation(env,data,department,group,value,actor,t){
 const fields=reservationFields(value),id='OB-'+crypto.randomUUID(),display_no=await nextOutboundDisplayNo(env,data.customer,fields.expected_ship_at);
 const reservation={...fields,status:'waiting_work',groups:[groupSnapshot(group)]};
 (data.links||=[]).push({outbound_id:id,display_no,quantity:1,unit:'组',phase:'planned',cargo_reservation:reservation,by:actor.name,actor_id:actor.id,at:t});
 return q(env,`INSERT INTO v2_outbound_orders(id,order_date,customer,biz_class,outbound_mode,instruction,status,source_inbound_plan_id,created_by,created_at,updated_at,display_no,expected_ship_at,wms_work_order_no,planned_box_count,planned_pallet_count,uses_stock_operation,stock_operation_status,outbound_requirement,destination,po_no)
 VALUES(?,?,?,?,?,?,'pending_issue',?,?,?,?,?,?,?,0,0,0,'pending',?,?,?)`,id,fields.expected_ship_at,data.customer,department==='import'?'bulk':department,fields.outbound_mode,'待作业完成/审核 · '+group.name+' · '+group.range+' · 输入 '+group.box_count+' 箱（出库成果待确认）',data.source_type==='inbound'?data.source_id:'',actor.name,t,t,display_no,fields.expected_ship_at,data.supply_chain_no||'',fields.outbound_requirement,fields.destination,fields.po_no);
}
// Reservation and final allocation are distinct. Never fabricate output capacity from input cartons.
export async function syncGroupReservations(env,data,t,actor,{confirmId='',expectedOutputs=null,needId}={}){
 const statements=[],ids=(data.links||[]).map(l=>l.outbound_id),orders=ids.length?(await q(env,'SELECT * FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(ids)).all()).results:[],byId=new Map(orders.map(o=>[o.id,o]));
 for(const link of data.links||[]){
  const r=link.cargo_reservation;if(!r||link.cargo_allocations?.length)continue;
  const order=byId.get(link.outbound_id);
  if(!order||frozen.includes(order.status))continue;
  statements.push(await reservationGuard(env,needId,order));
  Object.assign(r,reservationFields(order));
  const groups=r.groups.map(x=>data.cargo_groups.groups.find(g=>g.id===x.group_id));if(groups.some(g=>!g))throw Error('已有预约的组不可删除；请先处理该组出库预约');
  r.groups=groups.map(groupSnapshot);const allocations=groups.map(allocation);
  const ready=allocations.every(Boolean),changed=ready&&allocations.some(a=>a.unit!=='箱'||a.quantity!==a.input_box_count||r.outbound_mode==='milk_pallet'&&a.unit!=='托');
  if(confirmId===link.outbound_id){if(!ready)throw Error('本组尚未完成全部审核');if(JSON.stringify(allocations.map(a=>({id:a.output_id,version:a.output_version})))!==JSON.stringify(expectedOutputs))throw Error('成果版本已变化，请重新核对');}
  if(!ready){r.status='waiting_work';r.proposed_outputs=[];statements.push(q(env,'UPDATE v2_outbound_orders SET instruction=?,updated_at=? WHERE id=?','待作业完成/审核 · '+groups.map(g=>g.name+' · '+g.range+' · 输入 '+g.box_count+' 箱（出库成果待确认）').join(' / '),t,link.outbound_id));continue;}
  const other=(data.links||[]).filter(l=>l!==link&&byId.get(l.outbound_id)?.status!=='cancelled').flatMap(l=>l.cargo_allocations||[]);
  // Active duplicate allocation checks include cancelled-order handling in the normal booking API;
  // reservation holders are excluded there so this transaction is the only allocator for these groups.
  if(other.some(a=>allocations.some(b=>a.group_id===b.group_id)))throw Error('本组已有成果分配，请核对出库计划');
  r.proposed_outputs=allocations;
  if(changed&&confirmId!==link.outbound_id){r.status='needs_confirmation';statements.push(q(env,"UPDATE v2_outbound_orders SET instruction=?,updated_at=? WHERE id=? AND status NOT IN ('cancelled','shipped','loaded','completed','loading')",'待确认实际成果 · '+allocations.map(a=>a.group_name+' · '+a.input_box_count+'箱 → '+a.quantity+a.unit).join('\n'),t,link.outbound_id));continue;}
  link.cargo_allocations=allocations;link.phase='confirmed';r.status='ready';r.confirmed_by=actor.name;r.confirmed_at=t;r.automatic=confirmId!==link.outbound_id;
  const boxes=allocations.filter(a=>a.unit==='箱').reduce((n,a)=>n+a.quantity,0),pallets=allocations.filter(a=>a.unit==='托').reduce((n,a)=>n+a.quantity,0);
  statements.push(q(env,"UPDATE v2_outbound_orders SET instruction=?,planned_box_count=?,planned_pallet_count=?,stock_operation_status='completed',stock_operation_result_json=?,updated_at=? WHERE id=? AND status NOT IN ('cancelled','shipped','loaded','completed','loading')",allocations.map(a=>a.group_name+' · '+a.range+' · '+a.quantity+a.unit).join('\n'),boxes,pallets,JSON.stringify({grouped:true,allocations}),t,link.outbound_id));
 }
 return statements;
}
export async function cargoReservations(b,env,u){
 if(!['sop_cargo_reservation_create','sop_cargo_reservation_update','sop_cargo_reservation_confirm','sop_cargo_reservation_cancel'].includes(b.action))return null;
 if(!workChainEnabled(env)||u.scope==='field'||!['manager','service'].includes(u.role))throw Error('请由办公室客服维护本组出库预约');
 const row=await chainNeed(env,b.id,u),data=structuredClone(row.data);if(!data.cargo_groups||['closed','cancelled'].includes(data.status))throw Error('请选择有效分组作业计划');
 const request=text(b.client_req_id),fingerprint=JSON.stringify({action:b.action,id:b.id,revision:b.revision,group_id:b.group_id,outbound_id:b.outbound_id,reservation:b.reservation,outputs:b.outputs,order_updated_at:b.order_updated_at});if(!request)throw Error('缺少请求编号');
 const previous=async()=>{const old=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(!old)return null;const result=JSON.parse(old.result_json);if(old.actor_id!==u.id||old.record_id!==row.id||old.action!==b.action||result.fingerprint!==fingerprint)throw Error('预约请求编号冲突');return result;};const prior=await previous();if(prior)return prior;
 if(Number(b.revision)!==row.revision)throw Error('作业计划已更新，请刷新核对预约');
 const t=new Date().toISOString(),extra=[];
 if(b.action==='sop_cargo_reservation_create'){
  const group=data.cargo_groups.groups.find(g=>g.id===b.group_id);if(!group)throw Error('资料组不属于此计划');if(allocation(group))throw Error('本组已审核，请直接选择已审核组安排出库');
  const ids=(data.links||[]).filter(l=>(l.cargo_reservation?.groups||l.cargo_allocations||[]).some(g=>g.group_id===group.id)).map(l=>l.outbound_id);
  if(ids.length&&(await q(env,"SELECT id FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?)) AND status!='cancelled'",JSON.stringify(ids)).first()))throw Error('本组已有出库计划，请修改原预约；部分发货请先按实际范围拆组');
  extra.push(await createGroupReservation(env,data,row.department,group,b.reservation,u,t));
 }else{
  const link=data.links.find(l=>l.outbound_id===b.outbound_id&&l.cargo_reservation);if(!link)throw Error('预约不属于本计划');
  const order=await q(env,'SELECT * FROM v2_outbound_orders WHERE id=?',link.outbound_id).first();if(!order||frozen.includes(order.status))throw Error('实际出库或已取消记录不能改写');
  if(b.order_updated_at!==order.updated_at)throw Error('出库计划已更新，请刷新核对');
  if(b.action==='sop_cargo_reservation_cancel')extra.push(await reservationGuard(env,row.id,order));
  if(b.action==='sop_cargo_reservation_confirm'&&link.cargo_allocations?.length)throw Error('本预约成果已确认，请刷新查看');
  if(b.action==='sop_cargo_reservation_cancel'){link.cargo_reservation.status='cancelled';link.cargo_reservation.cancelled_by=u.name;link.cargo_reservation.cancelled_at=t;extra.push(q(env,"UPDATE v2_outbound_orders SET status='cancelled',updated_at=? WHERE id=?",t,link.outbound_id));extra.push(...amendmentAudit(env,order,{status:'cancelled'},u,t));}
  if(b.action==='sop_cargo_reservation_update'){
   if(link.cargo_allocations?.length)throw Error('成果已确认，请在原出库计划按现有变更规则调整运输信息');
   const fields=reservationFields(b.reservation);Object.assign(link.cargo_reservation,fields);extra.push(q(env,"UPDATE v2_outbound_orders SET expected_ship_at=?,outbound_mode=?,destination=?,po_no=?,outbound_requirement=?,updated_at=? WHERE id=?",fields.expected_ship_at,fields.outbound_mode,fields.destination,fields.po_no,fields.outbound_requirement,t,link.outbound_id));extra.push(q(env,'UPDATE v2_outbound_orders SET order_date=? WHERE id=?',fields.expected_ship_at,link.outbound_id),...amendmentAudit(env,order,fields,u,t));
  }
 }
 const synchronization=b.action==='sop_cargo_reservation_cancel'?[]:await syncGroupReservations(env,data,t,u,{confirmId:b.action==='sop_cargo_reservation_confirm'?b.outbound_id:'',expectedOutputs:b.outputs,needId:row.id});
 if(b.action==='sop_cargo_reservation_update'){const link=data.links.find(l=>l.outbound_id===b.outbound_id);Object.assign(link.cargo_reservation,reservationFields(b.reservation));}
 extra.unshift(...synchronization);
 const result={ok:true,id:row.id,revision:row.revision+1,fingerprint};try{await env.DB.batch([...chainEvent(env,row,data,u,request,b.action,t,result),...extra]);}catch(error){const committed=await previous();if(committed)return committed;throw error;}return result;
}
