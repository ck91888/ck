import {approvedOutput,reservationAmount,cargoLedger,validateCargoCapacity} from './cargo-capacity.js';
import {expandMarks} from './cargo-groups.js';
import {planAuditStatements} from './office-operator.js';
import {chainDate,chainNeed,chainEvent,workChainEnabled} from './work-chain.js';
import {nextOutboundDisplayNo} from './outbound-number.js';
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
 const labels={expected_ship_at:'预计出库日期',outbound_mode:'出库方式',destination:'去向',po_no:'PO',outbound_requirement:'运输交接说明',status:'状态',reservation_quantity:'本次数量',reservation_unit:'本次单位',shipping_range:'本次箱唛子范围'},diff=Object.fromEntries(Object.entries(next).filter(([key,value])=>String(order[key]??'')!==String(value??'')).map(([key,value])=>[key,{from:order[key]??'',to:value}]));
 if(!Object.keys(diff).length)return [];
 const ack=next.status==='cancelled'?0:1,revision=Number(order.revision_no||0)+1,summary=Object.entries(diff).map(([key,value])=>labels[key]+'：'+(value.from||'—')+' → '+(value.to||'—')).join('；');
 return [q(env,"UPDATE v2_outbound_orders SET revision_no=?,last_modified_by=?,last_modified_at=?,warehouse_ack_required=?,warehouse_ack_by='',warehouse_ack_at='' WHERE id=?",revision,actor.name,t,ack,order.id),q(env,"INSERT INTO v2_outbound_order_change_logs(id,order_id,revision_no,change_type,changed_by,changed_at,diff_json,summary_text,warehouse_ack_required,warehouse_ack_by,warehouse_ack_at,ack_source,created_at) VALUES(?,?,?,?,?,?,?,?,?,'','','',?)",crypto.randomUUID(),order.id,revision,'group_reservation',actor.name,t,JSON.stringify(diff),summary,ack,t),...planAuditStatements(env,'outbound',order.id,'update',t)];
}

const groupSnapshot=g=>({group_id:g.id,group_name:g.name,range:g.range,marks:g.marks,identifier:g.identifier,input_box_count:g.box_count,mapping_version:g.mapping_version||1,documents:g.documents||[],public_documents:g.public_documents||[]});
const allocation=(g,r)=>{const o=approvedOutput(g);if(!o)return null;const amount=reservationAmount(r,g,o);if(!Number.isSafeInteger(amount.quantity)||amount.quantity<1||amount.unit!==o.unit)return null;return {...groupSnapshot(g),allocation_schema:2,output_quantity:o.quantity,output_id:o.id,output_version:o.version,mapping_version:o.mapping_version,quantity:amount.quantity,unit:o.unit,input_box_count:o.input_box_count,range:r.shipping_range||o.range,marks:r.shipping_marks?.length?r.shipping_marks:o.marks,shipping_range:r.shipping_range||'',shipping_marks:r.shipping_marks||[],identifier:o.identifier,documents:o.documents,public_documents:o.public_documents};};
function quantityFields(value,group){
 const quantity=value.quantity==null||value.quantity===''?null:Number(value.quantity),unit=value.unit??'';
 if(quantity!==null&&(!Number.isSafeInteger(quantity)||quantity<1)||!['','箱','托'].includes(unit))throw Error('本次数量须为正整数；未知请留空待确认');
 const shipping_range=text(value.shipping_range),shipping_marks=shipping_range?expandMarks(shipping_range):[];
 if(shipping_marks.some(m=>!group.marks.includes(m)))throw Error('本次箱唛子范围超出本组');
 if(group.one_mark_one_box&&unit==='箱'&&quantity!==null&&shipping_marks.length&&quantity!==shipping_marks.length)throw Error('一唛一箱时，本次数量须与子范围一致');
 return {schema:2,quantity,unit,shipping_range,shipping_marks};
}
const ordersFor=async(env,data)=>(data.links||[]).length?(await q(env,'SELECT * FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(data.links.map(l=>l.outbound_id))).all()).results:[];
export async function createGroupReservation(env,data,department,group,value,actor,t){
 const fields=reservationFields(value),id='OB-'+crypto.randomUUID(),display_no=await nextOutboundDisplayNo(env,data.customer,fields.expected_ship_at);
 const reservation={...fields,...(value.schema===2||Object.hasOwn(value,'quantity')?quantityFields(value,group):{}),status:'waiting_work',groups:[groupSnapshot(group)]};
 (data.links||=[]).push({outbound_id:id,display_no,quantity:1,unit:'组',phase:'planned',cargo_reservation:reservation,by:actor.name,actor_id:actor.id,at:t});
 return q(env,`INSERT INTO v2_outbound_orders(id,order_date,customer,biz_class,outbound_mode,instruction,status,source_inbound_plan_id,created_by,created_at,updated_at,display_no,expected_ship_at,wms_work_order_no,planned_box_count,planned_pallet_count,uses_stock_operation,stock_operation_status,outbound_requirement,destination,po_no)
 VALUES(?,?,?,?,?,?,'pending_issue',?,?,?,?,?,?,?,0,0,0,'pending',?,?,?)`,id,fields.expected_ship_at,data.customer,department==='import'?'bulk':department,fields.outbound_mode,'待作业完成/审核 · '+group.name+' · '+(reservation.shipping_range||group.range)+' · 本次 '+(reservation.schema===2?(reservation.quantity==null?'数量待确认':reservation.quantity+(reservation.unit||'（单位待确认）')):'原整组预约')+' · 本组输入 '+group.box_count+' 箱',data.source_type==='inbound'?data.source_id:'',actor.name,t,t,display_no,fields.expected_ship_at,data.supply_chain_no||'',fields.outbound_requirement,fields.destination,fields.po_no);
}
// Reservation and final allocation are distinct. Never fabricate output capacity from input cartons.
export async function syncGroupReservations(env,data,t,actor,{confirmId='',expectedOutputs=null,needId,freshOrders=[],overrideOrder=null}={}){
 const statements=[],orders=[...await ordersFor(env,data),...freshOrders],byId=new Map(orders.map(o=>[o.id,o]));
 const ledger=cargoLedger(data,orders);
 for(const link of data.links||[]){
  const r=link.cargo_reservation;if(!r||link.cargo_allocations?.length)continue;
  const order=byId.get(link.outbound_id);if(!order||frozen.includes(order.status))continue;
  if(overrideOrder?.id!==order.id&&!freshOrders.some(o=>o.id===order.id))statements.push(await reservationGuard(env,needId,order));
  Object.assign(r,reservationFields(overrideOrder?.id===order.id?{...order,...overrideOrder}:order));const groups=r.groups.map(x=>data.cargo_groups.groups.find(g=>g.id===x.group_id));if(groups.some(g=>!g))throw Error('已有预约的组不可删除；请先处理该组出库预约');
  if(r.schema===2)quantityFields(r,groups[0]);r.groups=groups.map(groupSnapshot);const outputs=groups.map(approvedOutput),ready=outputs.every(Boolean),allocations=groups.map(g=>allocation(g,r));
  if(confirmId===link.outbound_id){if(!ready)throw Error('本组尚未完成全部审核');if(JSON.stringify(outputs.map(o=>({id:o.id,version:o.version})))!==JSON.stringify(expectedOutputs))throw Error('成果版本已变化，请重新核对');if(allocations.some(a=>!a))throw Error('请按实际成果填写本次数量及单位后确认');validateCargoCapacity(data,orders);}
  if(!ready){r.status='waiting_work';r.proposed_outputs=[];statements.push(q(env,'UPDATE v2_outbound_orders SET instruction=?,updated_at=? WHERE id=?','待作业完成/审核 · '+groups.map(g=>g.name+' · '+(r.shipping_range||g.range)).join(' / ')+' · 本次 '+(r.schema===2?(r.quantity==null?'数量待确认':r.quantity+(r.unit||'（单位待确认）')):'原整组预约'),t,link.outbound_id));continue;}
  r.proposed_outputs=groups.map((g,i)=>({...groupSnapshot(g),output_id:outputs[i].id,output_version:outputs[i].version,quantity:outputs[i].quantity,unit:outputs[i].unit}));
  const changed=allocations.some(a=>!a)||groups.some(g=>{const c=ledger.find(c=>c.group_id===g.id),o=approvedOutput(g);return c.overbooked||c.overlap||o.unit!=='箱'||o.quantity!==g.box_count||r.outbound_mode==='milk_pallet'&&o.unit!=='托';});
  if(changed&&confirmId!==link.outbound_id){r.status='needs_confirmation';statements.push(q(env,'UPDATE v2_outbound_orders SET instruction=?,updated_at=? WHERE id=?','待确认本次分配 · 本组成果 '+outputs.map(o=>o.quantity+o.unit).join(' / ')+' · 本次 '+(r.quantity==null?'待填写':r.quantity+(r.unit||'（单位待确认）')),t,link.outbound_id));continue;}
  link.cargo_allocations=allocations;link.phase='confirmed';r.status='ready';r.confirmed_by=actor.name;r.confirmed_at=t;r.automatic=confirmId!==link.outbound_id;
  const boxes=allocations.filter(a=>a.unit==='箱').reduce((n,a)=>n+a.quantity,0),pallets=allocations.filter(a=>a.unit==='托').reduce((n,a)=>n+a.quantity,0);
  statements.push(q(env,"UPDATE v2_outbound_orders SET instruction=?,planned_box_count=?,planned_pallet_count=?,stock_operation_status='completed',stock_operation_result_json=?,updated_at=? WHERE id=?",allocations.map(a=>a.group_name+' · '+a.range+' · 本次 '+a.quantity+a.unit).join('\n'),boxes,pallets,JSON.stringify({grouped:true,allocations}),t,link.outbound_id));
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
 const t=new Date().toISOString(),extra=[],freshOrders=[];
 if(b.action==='sop_cargo_reservation_create'){
  if((data.links||[]).filter(l=>l.cargo_reservation?.groups.some(g=>g.group_id===b.group_id)).length>=50)throw Error('每组最多50条预约记录');
  const group=data.cargo_groups.groups.find(g=>g.id===b.group_id);if(!group)throw Error('资料组不属于此计划');
  const value={...b.reservation,...(b.reservation.schema===2||Object.hasOwn(b.reservation,'quantity')?quantityFields(b.reservation,group):{})};
  extra.push(await createGroupReservation(env,data,row.department,group,value,u,t));const link=data.links.at(-1);freshOrders.push({id:link.outbound_id,status:'pending_issue',...reservationFields(value)});

 }else{
  const link=data.links.find(l=>l.outbound_id===b.outbound_id&&l.cargo_reservation);if(!link)throw Error('预约不属于本计划');
  const order=await q(env,'SELECT * FROM v2_outbound_orders WHERE id=?',link.outbound_id).first();if(!order||frozen.includes(order.status))throw Error('实际出库或已取消记录不能改写');
  if(b.order_updated_at!==order.updated_at)throw Error('出库计划已更新，请刷新核对');
  extra.push(await reservationGuard(env,row.id,order));
  if(b.action==='sop_cargo_reservation_confirm'&&link.cargo_allocations?.length)throw Error('本预约成果已确认，请刷新查看');
  if(b.action==='sop_cargo_reservation_cancel'){link.cargo_reservation.status='cancelled';link.cargo_reservation.cancelled_by=u.name;link.cargo_reservation.cancelled_at=t;extra.push(q(env,"UPDATE v2_outbound_orders SET status='cancelled',updated_at=? WHERE id=?",t,link.outbound_id));extra.push(...amendmentAudit(env,order,{status:'cancelled'},u,t));}
  if(b.action==='sop_cargo_reservation_confirm'&&b.reservation)Object.assign(link.cargo_reservation,quantityFields({...link.cargo_reservation,...b.reservation},data.cargo_groups.groups.find(g=>g.id===link.cargo_reservation.groups[0].group_id)));
  if(b.action==='sop_cargo_reservation_update'){
   const group=data.cargo_groups.groups.find(g=>g.id===link.cargo_reservation.groups[0].group_id),fields=reservationFields(b.reservation),before=structuredClone(link.cargo_reservation),amount=b.reservation.schema===2||Object.hasOwn(b.reservation,'quantity')?quantityFields(b.reservation,group):{},allocationChanged=Object.keys(amount).some(k=>JSON.stringify(before[k])!==JSON.stringify(amount[k]));if(link.cargo_allocations?.length&&allocationChanged){link.allocation_history=[...(link.allocation_history||[]),{allocations:link.cargo_allocations,by:u.name,at:t}];delete link.cargo_allocations;link.phase='planned';extra.push(q(env,"UPDATE v2_outbound_orders SET planned_box_count=0,planned_pallet_count=0,stock_operation_status='pending',stock_operation_result_json='' WHERE id=?",link.outbound_id));}Object.assign(link.cargo_reservation,fields,amount);extra.push(q(env,"UPDATE v2_outbound_orders SET expected_ship_at=?,outbound_mode=?,destination=?,po_no=?,outbound_requirement=?,updated_at=? WHERE id=?",fields.expected_ship_at,fields.outbound_mode,fields.destination,fields.po_no,fields.outbound_requirement,t,link.outbound_id));extra.push(q(env,'UPDATE v2_outbound_orders SET order_date=? WHERE id=?',fields.expected_ship_at,link.outbound_id),...amendmentAudit(env,{...order,reservation_quantity:before.quantity,reservation_unit:before.unit,shipping_range:before.shipping_range},{...fields,...(allocationChanged?{reservation_quantity:amount.quantity,reservation_unit:amount.unit,shipping_range:amount.shipping_range}:{})},u,t));
  }
 }
 if(b.action!=='sop_cargo_reservation_cancel')validateCargoCapacity(data,[...await ordersFor(env,data),...freshOrders]);
 const synchronization=b.action==='sop_cargo_reservation_cancel'?[]:await syncGroupReservations(env,data,t,u,{confirmId:b.action==='sop_cargo_reservation_confirm'?b.outbound_id:'',expectedOutputs:b.outputs,needId:row.id,freshOrders,overrideOrder:b.action==='sop_cargo_reservation_update'?{id:b.outbound_id,...b.reservation}:null});
 if(b.action==='sop_cargo_reservation_update'){const link=data.links.find(l=>l.outbound_id===b.outbound_id);Object.assign(link.cargo_reservation,reservationFields(b.reservation));}
 extra.push(...synchronization);
 const result={ok:true,id:row.id,revision:row.revision+1,fingerprint};try{await env.DB.batch([...chainEvent(env,row,data,u,request,b.action,t,result),...extra]);}catch(error){const committed=await previous();if(committed)return committed;throw error;}return result;
}
