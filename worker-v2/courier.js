import {courierOwners,trackingNumber,trackingNumbers} from '../shared/courier-rules.js';
import {inboundFlowEnabled} from './inbound-flow.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const rows=async(env,sql,...args)=>(await q(env,sql,...args).all()).results||[];
const ownerIds=new Set(courierOwners.map(x=>x[0]));
const fail=m=>{throw Error(m);};
const stamp=()=>new Date().toISOString();
const limited=(v,n=300)=>String(v??'').trim().slice(0,n);
function user(env,write=false){const u=env.SOP_REQUEST_USER;if(!inboundFlowEnabled(env)||!u||(write&&!['manager','dispatcher','reviewer','service'].includes(u.role)))fail('无此操作权限 / 권한이 없습니다');return u;}

export async function validateCourierLines(env,lines,id=''){
 const list=Array.isArray(lines)?lines:[];
 const courier=list.filter(x=>x.unit_type==='courier');
 if(!courier.length)return list;
 if(!inboundFlowEnabled(env))fail('快递收货仅在测试升级环境启用');
 if(courier.length!==1)fail('请把本计划快递单号填写在同一行 / 택배 송장번호를 한 행에 입력하세요');
 const codes=trackingNumbers(courier[0].tracking_nos);
 if(!codes.length)fail('选择快递后必须填写快递单号 / 택배 송장번호가 필요합니다');
 for(const code of codes){const existing=await q(env,`SELECT p.display_no FROM ck_courier_plan_items i JOIN v2_inbound_plans p ON p.id=i.plan_id WHERE i.tracking_no=? AND i.plan_id!=?`,code,id).first();if(existing)fail(code+' 已关联 '+existing.display_no+'，请先核对 / 이미 연결된 송장번호');}
 return list.map(x=>x.unit_type==='courier'?{...x,planned_qty:codes.length,tracking_nos:codes}:x);
}
export function courierPlanStatements(env,planId,lineId,line){
 if(line.unit_type!=='courier')return [];
 return line.tracking_nos.map(code=>q(env,'INSERT INTO ck_courier_plan_items(tracking_no,plan_id,line_id) VALUES(?,?,?)',code,planId,lineId));
}
export async function courierProgress(env,planId){
 if(!inboundFlowEnabled(env))return null;
 const items=await rows(env,`SELECT i.tracking_no,r.id AS receipt_id,r.owner,r.received_at,r.scanner_name,r.status,r.handed_to,r.handed_at FROM ck_courier_plan_items i LEFT JOIN ck_courier_receipts r ON r.tracking_no=i.tracking_no WHERE i.plan_id=? ORDER BY i.tracking_no`,planId);
 return items.length?{total:items.length,received:items.filter(x=>x.receipt_id).length,items}:null;
}
export async function courierReady(env,planId){const p=await courierProgress(env,planId);return !p||p.total===p.received;}
export async function protectCourierEdit(env,id){
 if(!inboundFlowEnabled(env))return;
 if(await q(env,'SELECT 1 FROM ck_courier_plan_items i JOIN ck_courier_receipts r ON r.tracking_no=i.tracking_no WHERE i.plan_id=? LIMIT 1',id).first())fail('本计划已有快递收货记录，不能修改计划或快递单号 / 이미 수령한 계획은 변경할 수 없습니다');
}
export async function syncCourierArrival(env,planId,recalc){
 const progress=await courierProgress(env,planId);if(!progress)return null;
 const p=await q(env,'SELECT * FROM v2_inbound_plans WHERE id=?',planId).first();if(!p||p.is_deleted||p.status==='cancelled')return {progress,status:p?.status||''};
 // Count from immutable receipts, not client-supplied quantities. Safe after retries/concurrent scans.
 await q(env,`UPDATE v2_inbound_plan_lines SET actual_qty=(SELECT COUNT(*) FROM ck_courier_plan_items i JOIN ck_courier_receipts r ON r.tracking_no=i.tracking_no WHERE i.line_id=v2_inbound_plan_lines.id) WHERE plan_id=? AND unit_type='courier'`,planId).run();
 const nonCourier=await q(env,"SELECT 1 FROM v2_inbound_plan_lines WHERE plan_id=? AND unit_type!='courier' AND planned_qty>0 LIMIT 1",planId).first();
 if(progress.received===progress.total&&(!nonCourier||p.unload_completed_at)){
  const last=progress.items.filter(x=>x.received_at).sort((a,b)=>a.received_at.localeCompare(b.received_at)).at(-1),t=last.received_at;
  await q(env,`UPDATE v2_inbound_plans SET status=CASE WHEN status='pending' THEN 'arrived_pending_putaway' ELSE status END,unload_completed_at=COALESCE(NULLIF(unload_completed_at,''),?),unload_completed_by=COALESCE(NULLIF(unload_completed_by,''),?),updated_at=? WHERE id=? AND status NOT IN ('completed','cancelled') AND is_deleted=0`,t,last.scanner_name,stamp(),planId).run();
  await recalc(env,planId,stamp());
 }
 return {progress,status:(await q(env,'SELECT status FROM v2_inbound_plans WHERE id=?',planId).first()).status,plan_id:planId,display_no:p.display_no};
}
async function scanner(env,b,u){
 if(!b.scanner_badge)return {id:u.id,name:u.name};
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);
 const r=await q(env,"SELECT worker_id,name FROM ck_attendance_days WHERE worker_id=? AND day=? AND signed_out=''",limited(b.scanner_badge,100),day).first();
 if(!r)fail('扫描人工牌须已签到且未签退 / 출근한 명찰을 확인하세요');return {id:r.worker_id,name:r.name};
}
async function joinedReceipt(env,id){return q(env,`SELECT r.*,i.plan_id,p.display_no,p.customer,p.status AS plan_status FROM ck_courier_receipts r LEFT JOIN ck_courier_plan_items i ON i.tracking_no=r.tracking_no LEFT JOIN v2_inbound_plans p ON p.id=i.plan_id WHERE r.id=?`,id).first();}
function rangeDate(value){if(!value)return '';const s=String(value);if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||Number.isNaN(Date.parse(s))||new Date(s).toISOString().slice(0,10)!==s)fail('日期无效 / 날짜 오류');return s;}

// Re-evaluate inside the same transaction as the correction, so an earlier page
// cannot rewrite arrivals after downstream work or accounting has appeared.
const correctionGuard=`NOT EXISTS(
 SELECT 1 FROM ck_courier_plan_items i JOIN v2_inbound_plans p ON p.id=i.plan_id
 WHERE i.tracking_no=ck_courier_receipts.tracking_no AND (
 p.status='cancelled' OR p.is_deleted=1 OR p.accounted=1 OR p.force_completed=1 OR COALESCE(p.manual_completed_at,'')!=''
 OR EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.related_doc_id=p.id AND j.job_type!='unload' AND j.status!='cancelled')
 OR EXISTS(SELECT 1 FROM v2_inbound_plan_biz_tasks t WHERE t.plan_id=p.id AND t.status='completed' AND t.completion_source!='unload')
 OR EXISTS(SELECT 1 FROM v2_outbound_orders o WHERE o.source_inbound_plan_id=p.id AND o.status NOT IN ('pending','cancelled'))))
 AND NOT EXISTS(SELECT 1 FROM v2_003_purchase_shipments s WHERE s.id=ck_courier_receipts.shipment_id AND (s.received_at!='' OR s.status NOT IN ('pending','cancelled')))`;

function rollbackArrivalStatements(env,planId,eventId,t){
 const applied='EXISTS(SELECT 1 FROM ck_courier_events WHERE id=?)';
 const missing=`EXISTS(SELECT 1 FROM ck_courier_plan_items i WHERE i.plan_id=? AND NOT EXISTS(SELECT 1 FROM ck_courier_receipts r WHERE r.tracking_no=i.tracking_no))`;
 const truck="EXISTS(SELECT 1 FROM v2_inbound_plan_lines l WHERE l.plan_id=v2_inbound_plans.id AND l.unit_type!='courier' AND l.planned_qty>0)";
 return [
  q(env,`UPDATE v2_inbound_plan_lines SET actual_qty=(SELECT COUNT(*) FROM ck_courier_plan_items i JOIN ck_courier_receipts r ON r.tracking_no=i.tracking_no WHERE i.line_id=v2_inbound_plan_lines.id) WHERE plan_id=? AND unit_type='courier' AND ${applied}`,planId,eventId),
  q(env,`UPDATE v2_inbound_plan_biz_tasks SET status='pending',completed_at='',completed_by='',completion_source='',completion_note='',updated_at=? WHERE plan_id=? AND completion_source='unload' AND ${applied} AND ${missing}`,t,planId,eventId,planId),
  q(env,`UPDATE v2_inbound_plans SET status=CASE WHEN EXISTS(SELECT 1 FROM v2_inbound_plan_jobs j WHERE j.plan_id=v2_inbound_plans.id AND j.job_type='unload' AND j.status IN ('pending','working','awaiting_close')) THEN 'unloading' WHEN ${truck} AND COALESCE(unload_completed_at,'')!='' THEN 'arrived_pending_putaway' ELSE 'pending' END,unload_completed_at=CASE WHEN ${truck} THEN unload_completed_at ELSE '' END,unload_completed_by=CASE WHEN ${truck} THEN unload_completed_by ELSE '' END,updated_at=? WHERE id=? AND ${applied} AND ${missing}`,t,planId,eventId,planId)
 ];
}
export async function handleCourier(b,env,recalc){
 const write=!['sop_courier_config','sop_courier_list','sop_courier_detail'].includes(b.action),u=user(env,write);
 if(b.action==='sop_courier_config')return {ok:true,owners:courierOwners,user:{id:u.id,name:u.name},formats:'11–14位数字；LP/EZ + 9位数字 + CN；JJD + 18位数字'};
 if(b.action==='sop_courier_list'){
  const owner=limited(b.owner,30),keyword=limited(b.keyword,80).replace(/[ -]/g,'').toUpperCase(),from=rangeDate(b.from),to=rangeDate(b.to);
  if(owner&&!ownerIds.has(owner))fail('所属无效');if(from&&to&&from>to)fail('起止日期顺序不正确');
  const where=['1=1'],args=[];
  if(owner){where.push('r.owner=?');args.push(owner);}if(keyword){where.push("r.tracking_no LIKE ? ESCAPE '\\'");args.push('%'+keyword.replace(/[\\%_]/g,'\\$&')+'%');}
  if(from){where.push('r.received_at>=?');args.push(new Date(from+'T00:00:00+09:00').toISOString());}
  if(to){where.push('r.received_at<?');args.push(new Date(Date.parse(to+'T00:00:00+09:00')+86400000).toISOString());}
  if(b.status){if(!['received','handed_over'].includes(b.status))fail('状态无效');where.push('r.status=?');args.push(b.status);}
  const condition=where.join(' AND '),limit=Math.min(100,Math.max(1,parseInt(b.limit)||50)),offset=Math.max(0,parseInt(b.offset)||0);
  const total=(await q(env,'SELECT COUNT(*) n FROM ck_courier_receipts r WHERE '+condition,...args).first()).n;
  const items=await rows(env,`SELECT r.*,i.plan_id,p.display_no,p.customer FROM ck_courier_receipts r LEFT JOIN ck_courier_plan_items i ON i.tracking_no=r.tracking_no LEFT JOIN v2_inbound_plans p ON p.id=i.plan_id WHERE ${condition} ORDER BY r.received_at DESC,r.id DESC LIMIT ? OFFSET ?`,...args,limit,offset);
  return {ok:true,items,total,limit,offset};
 }
 if(b.action==='sop_courier_detail'){
  const item=await joinedReceipt(env,limited(b.id,100));if(!item)fail('记录不存在');return {ok:true,item,events:await rows(env,'SELECT * FROM ck_courier_events WHERE receipt_id=? ORDER BY created_at,id',item.id)};
 }
 if(b.action==='sop_courier_receive'){
  const owner=limited(b.owner,30);if(!ownerIds.has(owner))fail('请先点选快递所属 / 먼저 소속을 선택하세요');
  const code=trackingNumber(b.tracking_no),who=await scanner(env,b,u);
  const purchase=await q(env,"SELECT id,shipment_no FROM v2_003_purchase_shipments WHERE tracking_no=? AND status!='cancelled'",code).first();
  if(purchase&&!['supplies','purchase'].includes(owner))fail('该单命中耗材／采购登记，请核对后选择仓库耗材或采购物品 / 구매 등록 내역을 확인하세요');
  const linked=await q(env,'SELECT p.* FROM ck_courier_plan_items i JOIN v2_inbound_plans p ON p.id=i.plan_id WHERE i.tracking_no=?',code).first();
  if(linked&&(linked.status==='cancelled'||linked.is_deleted))fail('关联入库计划已取消或删除，请办公室核实 / 취소된 계획입니다');
  if(linked&&purchase)fail('此单同时关联入库计划和采购，请办公室核实后收货');
  const id='CR-'+crypto.randomUUID(),t=stamp();
  await env.DB.batch([
   q(env,`INSERT OR IGNORE INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name,location,note,shipment_id,status,version) VALUES(?,?,?,?,?,?,?,?,?,?,?,'received',1)`,id,code,owner,t,who.id,who.name,u.id,u.name,limited(b.location,50),limited(b.note),purchase?.id||''),
   q(env,`INSERT INTO ck_courier_events(id,receipt_id,kind,actor_id,actor_name,detail,created_at) SELECT ?,id,'receive',?,?,?,? FROM ck_courier_receipts WHERE id=?`,'CE-'+crypto.randomUUID(),u.id,u.name,JSON.stringify({owner,scanner:who}),t,id)
  ]);
  const r=await q(env,'SELECT * FROM ck_courier_receipts WHERE tracking_no=?',code).first();
  const plan=linked?await syncCourierArrival(env,linked.id,recalc):null;
  return {ok:true,duplicate:r.id!==id,owner_conflict:r.owner!==owner,item:await joinedReceipt(env,r.id),plan};
 }
 if(b.action==='sop_courier_update'){
  const old=await q(env,'SELECT * FROM ck_courier_receipts WHERE id=?',limited(b.id,100)).first();if(!old)fail('记录不存在');
  if(Number(b.version)!==old.version)fail('记录已更新，请刷新后再操作 / 새로고침하세요');
  const owner=limited(b.owner||old.owner,30);if(!ownerIds.has(owner))fail('所属无效');
  const status=b.status||old.status;if(!['received','handed_over'].includes(status))fail('状态无效');
  if(old.status==='handed_over'&&status!=='handed_over')fail('交接已完成，不能重复交接');
  const recipient=limited(b.handed_to||old.handed_to,100);if(status==='handed_over'&&!recipient)fail('请填写接收人 / 인수자를 입력하세요');
  const code=trackingNumber(b.tracking_no??old.tracking_no),changed=code!==old.tracking_no;
  const note=limited(b.note),storedNote=b.note===undefined?old.note:note,t=stamp();
  let oldPlan=null,newPlan=null,purchase=null;
  if(changed){
   if(await q(env,'SELECT 1 FROM ck_courier_receipts WHERE tracking_no=? AND id!=?',code,old.id).first())fail('该单号已经收过，不能覆盖另一条收货记录 / 이미 수령한 송장번호입니다');
   if(!await q(env,`SELECT 1 FROM ck_courier_receipts WHERE id=? AND ${correctionGuard}`,old.id).first())fail('关联单据已有理货、出库或记账，请办公室先核实后续记录，不能直接更改单号 / 후속 작업 내역을 확인하세요');
   oldPlan=await q(env,'SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?',old.tracking_no).first();
   newPlan=await q(env,'SELECT p.* FROM ck_courier_plan_items i JOIN v2_inbound_plans p ON p.id=i.plan_id WHERE i.tracking_no=?',code).first();
   if(newPlan&&(newPlan.status==='cancelled'||newPlan.is_deleted||newPlan.accounted))fail('新单号关联计划已取消、删除或记账，请办公室核实');
  }
  purchase=await q(env,"SELECT id FROM v2_003_purchase_shipments WHERE tracking_no=? AND status!='cancelled'",code).first();
  if(purchase&&!['supplies','purchase'].includes(owner))fail('该单命中耗材／采购登记，请核对所属');
  if(changed&&newPlan&&purchase)fail('新单号同时关联入库与采购，请办公室核实');
  const eventId='CE-'+crypto.randomUUID();
  const result=await env.DB.batch([
   q(env,`INSERT INTO ck_courier_events(id,receipt_id,kind,actor_id,actor_name,detail,created_at) SELECT ?,id,'update',?,?,?,? FROM ck_courier_receipts WHERE id=? AND version=? ${changed?'AND '+correctionGuard:''}`,eventId,u.id,u.name,JSON.stringify({before:{tracking_no:old.tracking_no,owner:old.owner,status:old.status,handed_to:old.handed_to},after:{tracking_no:code,owner,status,handed_to:recipient},note}),t,old.id,old.version),
   q(env,`UPDATE ck_courier_receipts SET tracking_no=?,shipment_id=?,owner=?,status=?,handed_to=?,handed_at=CASE WHEN handed_at='' AND ?='handed_over' THEN ? ELSE handed_at END,note=?,version=version+1 WHERE id=? AND version=? AND EXISTS(SELECT 1 FROM ck_courier_events WHERE id=?)`,code,changed?(purchase?.id||''):old.shipment_id,owner,status,recipient,status,t,storedNote,old.id,old.version,eventId),
   ...(changed&&oldPlan?rollbackArrivalStatements(env,oldPlan.plan_id,eventId,t):[])
  ]);
  if(result[1].meta.changes!==1)fail('记录已更新或已有后续作业，请刷新后核实');
  if(changed&&newPlan)await syncCourierArrival(env,newPlan.id,recalc);
  return {ok:true,item:await joinedReceipt(env,old.id)};
 }
 fail('未知快递操作');
}
