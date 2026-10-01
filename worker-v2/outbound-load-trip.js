// Staging only: a truck owns one labor job; each order owns its own loading result.
import {ensureSchema} from './schema-ready.js';
import {workChainEnabled} from './work-chain.js';
import {documentCodeError} from '../shared/document-code.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const parse=s=>JSON.parse(s||'{}'),active="('pending','working','awaiting_close')",loadable=['issued','working','ready_to_ship','preparing_outbound'];
const fail=m=>{throw Error(m);},no=o=>o.display_no||o.id;
export const loadTripEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_UPGRADE_ENABLED==='true';
const needMatch="(json_extract(n.state,'$.source_id')=o.id OR EXISTS(SELECT 1 FROM json_each(n.state,'$.links') l WHERE json_extract(l.value,'$.outbound_id')=o.id))";
const versionGuard=(value)=>`SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_outbound_orders o WHERE o.id=NEW.order_id AND o.updated_at=json_extract(${value},'$.order_version') AND COALESCE(o.revision_no,0)=json_extract(${value},'$.revision_no') AND o.status IN ('issued','working','ready_to_ship','preparing_outbound') AND COALESCE(o.warehouse_ack_required,0)=0 AND COALESCE(o.pickup_confirm_required,0)=0) THEN RAISE(ABORT,'load_order_changed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM json_each(${value},'$.needs') v LEFT JOIN sop_records n ON n.id=json_extract(v.value,'$.id') WHERE n.id IS NULL OR n.revision!=json_extract(v.value,'$.revision')) OR (SELECT COUNT(*) FROM v2_outbound_orders o JOIN sop_records n ON n.kind='need' AND ${needMatch} WHERE o.id=NEW.order_id)!=json_array_length(${value},'$.needs') THEN RAISE(ABORT,'load_order_changed') END;`;
export const LOAD_SCHEMA=[
 "CREATE TABLE IF NOT EXISTS ck_load_trips(job_id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,signature_json TEXT NOT NULL,owner_id TEXT NOT NULL,vehicle_no TEXT NOT NULL DEFAULT '',driver_name TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,finished_at TEXT NOT NULL DEFAULT '')",
 "CREATE TABLE IF NOT EXISTS ck_load_order_links(job_id TEXT NOT NULL,order_id TEXT NOT NULL,position INTEGER NOT NULL,snapshot_json TEXT NOT NULL,result_json TEXT NOT NULL DEFAULT '',PRIMARY KEY(job_id,order_id))",
 'CREATE INDEX IF NOT EXISTS ck_load_order_lookup ON ck_load_order_links(order_id,job_id)',
 'CREATE TABLE IF NOT EXISTS ck_load_order_claims(order_id TEXT PRIMARY KEY,job_id TEXT NOT NULL)',
 `CREATE TRIGGER IF NOT EXISTS ck_load_link_guard BEFORE INSERT ON ck_load_order_links BEGIN ${versionGuard('NEW.snapshot_json')}
 SELECT CASE WHEN EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id!=NEW.job_id AND j.job_type='load_outbound' AND j.status IN ${active} AND (j.related_doc_id=NEW.order_id OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=NEW.order_id))) THEN RAISE(ABORT,'load_order_busy') END; END`,
 `CREATE TRIGGER IF NOT EXISTS ck_load_result_guard BEFORE UPDATE OF result_json ON ck_load_order_links WHEN NEW.result_json!='' BEGIN ${versionGuard('NEW.result_json')} END`,
 `CREATE TRIGGER IF NOT EXISTS ck_load_legacy_guard BEFORE INSERT ON v2_ops_jobs WHEN NEW.job_type='load_outbound' AND NEW.related_doc_id!='' AND NEW.status IN ${active} AND EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id!=NEW.id AND j.job_type='load_outbound' AND j.status IN ${active} AND (j.related_doc_id=NEW.related_doc_id OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=NEW.related_doc_id))) BEGIN SELECT RAISE(ABORT,'load_order_busy'); END`,
 `CREATE TRIGGER IF NOT EXISTS ck_load_release_cancel AFTER UPDATE OF status ON v2_ops_jobs WHEN NEW.status='cancelled' BEGIN DELETE FROM ck_load_order_claims WHERE job_id=NEW.id; END`,
 `CREATE TRIGGER IF NOT EXISTS ck_load_shipped_guard BEFORE UPDATE OF status ON v2_outbound_orders WHEN NEW.status='shipped' AND OLD.status!='shipped' AND EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.job_type='load_outbound' AND j.status IN ${active} AND (j.related_doc_id=OLD.id OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=OLD.id))) BEGIN SELECT CASE WHEN OLD.status NOT IN ('issued','working','ready_to_ship','preparing_outbound') OR COALESCE(OLD.warehouse_ack_required,0)!=0 OR COALESCE(OLD.pickup_confirm_required,0)!=0 OR (OLD.planned_box_count>0 AND NEW.actual_box_count!=OLD.planned_box_count) OR (OLD.planned_pallet_count>0 AND NEW.actual_pallet_count!=OLD.planned_pallet_count) THEN RAISE(ABORT,'load_order_changed') END; END`,
 `CREATE TRIGGER IF NOT EXISTS ck_load_worker_guard BEFORE INSERT ON v2_ops_job_workers WHEN EXISTS(SELECT 1 FROM ck_load_trips WHERE job_id=NEW.job_id) AND EXISTS(SELECT 1 FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.worker_id=NEW.worker_id AND w.left_at='' AND w.job_id!=NEW.job_id AND j.status IN ${active}) BEGIN SELECT RAISE(ABORT,'load_worker_busy'); END`
];
export async function ensureLoadSchema(e){if(loadTripEnabled(e))await ensureSchema(e.DB,'load-trip-v1',()=>e.DB.batch(LOAD_SCHEMA.map(s=>e.DB.prepare(s))));}
async function checked(e,orders,own=''){
 if(!orders.length)return [];
 const ids=JSON.stringify(orders.map(o=>o.id));
 const [ns,js]=await e.DB.batch([
  q(e,`SELECT o.id AS order_id,n.id,n.revision,n.state FROM v2_outbound_orders o JOIN sop_records n ON n.kind='need' AND ${needMatch} WHERE o.id IN (SELECT value FROM json_each(?))`,ids),
  q(e,`SELECT DISTINCT o.id AS order_id,j.id AS job_id FROM v2_outbound_orders o JOIN v2_ops_jobs j ON j.job_type='load_outbound' AND j.status IN ${active} AND j.id!=? AND (j.related_doc_id=o.id OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=o.id)) WHERE o.id IN (SELECT value FROM json_each(?))`,own,ids)
 ]);
 return orders.map(order=>{
  const needs=ns.results.filter(n=>n.order_id===order.id),busy=js.results.find(j=>j.order_id===order.id);
  let reason='';
  if(!loadable.includes(order.status))reason=order.status==='pending_issue'?'尚未下发，请先打印下发':order.status==='shipped'?'已出库，不得重复装货':order.status==='cancelled'?'已取消':'当前状态不允许装货：'+order.status;
  else if(workChainEnabled(e)&&!needs.length)reason='缺少关联作业计划';
  else if(needs.some(n=>{const d=parse(n.state);return workChainEnabled(e)?!d.result||d.status==='cancelled':!(d.result&&(d.links||[]).some(l=>l.outbound_id===order.id))&&!['linked','closed'].includes(d.status);}))reason='关联作业尚未审核完成；直接转发须先确认收货和可发货数量';
  else if(Number(order.warehouse_ack_required))reason='出库要求或资料已更新，请先查看并确认最新变更';
  else if(Number(order.pickup_confirm_required))reason='提货安排已更新，请先查看并确认提货信息';
  else if(busy)reason='已有进行中的装货任务，请继续原任务';
  return {order,reason,active_job_id:busy?.job_id||'',needs:needs.map(n=>({id:n.id,revision:n.revision})).sort((a,b)=>a.id.localeCompare(b.id)),planned_box_count:Number(order.planned_box_count||0),planned_pallet_count:Number(order.planned_pallet_count||0)};
 });
}
const assertOrder=x=>{if(x.reason)fail(no(x.order)+'：'+x.reason);};
const snapshot=x=>({order_version:x.order.updated_at,revision_no:Number(x.order.revision_no||0),needs:x.needs,planned_box_count:x.planned_box_count,planned_pallet_count:x.planned_pallet_count});
async function conflictMessage(e,items,own=''){
 const current=await checked(e,await rows(e,'SELECT * FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(items.map(x=>x.order.id))),own);
 const changed=items.flatMap(x=>{const c=current.find(c=>c.order.id===x.order.id);if(!c)return [no(x.order)+'：单据已不存在'];if(c.reason)return [no(c.order)+'：'+c.reason];return JSON.stringify(snapshot(c))!==JSON.stringify(snapshot(x))?[no(c.order)+'：出库单或关联作业刚被修改，请重新核对']:[];});
 return changed.join('；')||items.map(x=>no(x.order)).join('、')+'：单据状态刚发生变化，请刷新核对';
}
export async function validateLegacyLoadFinish(e,b){
 if(!loadTripEnabled(e)||!e.SOP_GROUP_FINISH)return null;
 const job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',b.job_id).first();
 if(job?.status==='completed')return {ok:true,already_completed:true,job_id:job.id};
 if(!job||!job.related_doc_id)return null;
 const [item]=await checked(e,await rows(e,'SELECT * FROM v2_outbound_orders WHERE id=?',job.related_doc_id),job.id);if(!item)fail('关联出库单不存在，请核对原任务');assertOrder(item);
 const box=Number(b.box_count||0),pallet=Number(b.pallet_count||0);
 if(!Number.isSafeInteger(box)||!Number.isSafeInteger(pallet)||box<0||pallet<0||(!box&&!pallet))fail(no(item.order)+'：请核对实际装货数量');
 if((item.planned_box_count>0&&box!==item.planned_box_count)||(item.planned_pallet_count>0&&pallet!==item.planned_pallet_count))fail(no(item.order)+'：实装数量与应装数量不一致，未装完不得结束');
 return null;
}
export async function loadCandidates(e,b){
 const offset=Math.max(0,Math.floor(Number(b.offset)||0)),search=String(b.search||'').trim().slice(0,100),like='%'+search+'%';
 const orders=await rows(e,"SELECT * FROM v2_outbound_orders WHERE status IN ('issued','working','ready_to_ship','preparing_outbound','pending_issue') AND (?='' OR display_no LIKE ? OR customer LIKE ? OR wms_work_order_no LIKE ?) ORDER BY expected_ship_at,created_at LIMIT 51 OFFSET ?",search,like,like,like,offset);
 return {ok:true,items:await checked(e,orders.slice(0,50)),more:orders.length>50,offset};
}
export async function resolveLoadOrder(e,b){
 let code=String(b.code||'').normalize('NFKC').trim();const invalid=documentCodeError(code);if(invalid)fail(invalid);
 if(/^https?:\/\//i.test(code)){const url=new URL(code);code=url.searchParams.get('outbound')||url.searchParams.get('order_id')||url.searchParams.get('id')||code;}
 if(!code||code.length>500)fail('请扫描出库单号 / 출고번호를 스캔하세요');
 const orders=await rows(e,'SELECT * FROM v2_outbound_orders WHERE display_no=? OR id=? OR (wms_work_order_no=? AND wms_work_order_no!=\'\') LIMIT 2',code,code,code);
 if(orders.length!==1)fail(code+'：'+(orders.length?'匹配多张出库单，请扫描系统出库单号':'未找到出库单'));
 const [item]=await checked(e,orders);return {ok:true,kind:item.reason?'status_not_allowed':'system',...item,message:item.reason?no(item.order)+'：'+item.reason:''};
}
export async function loadOrdersForJob(e,job){
 if(!loadTripEnabled(e)||job.job_type!=='load_outbound')return [];
 const links=await rows(e,'SELECT l.*,o.* FROM ck_load_order_links l JOIN v2_outbound_orders o ON o.id=l.order_id WHERE l.job_id=? ORDER BY l.position',job.id);
 const orders=links.length?links:await rows(e,'SELECT * FROM v2_outbound_orders WHERE id=?',job.related_doc_id);
 return (await checked(e,orders,job.id)).map(x=>({...x,result:x.order.result_json?parse(x.order.result_json):null}));
}
export async function startLoadTrip(e,p,staff){
 await ensureLoadSchema(e);
 const ids=p.order_ids||[p.order_id],req=String(p.client_req_id||'');
 if(!Array.isArray(ids)||!ids.length||ids.length>20||ids.some(id=>typeof id!=='string'||!id||id.length>120)||new Set(ids).size!==ids.length)fail('请选择1～20张不同出库单');
 if(!req||req.length>100)fail('缺少请求编号，请刷新重试');
 const driver=String(p.driver_name||'').trim().slice(0,100),vehicle=String(p.vehicle_no||'').trim().slice(0,50);
 const signature=JSON.stringify({ids:[...ids].sort(),workers:staff.workers.map(w=>({...w})).sort((a,b)=>a.id.localeCompare(b.id)),lead_id:staff.lead_id,department:staff.department,minutes:staff.estimated_minutes,driver,vehicle});
 const replay=async()=>{const trip=await q(e,'SELECT * FROM ck_load_trips WHERE request_id=?',req).first();if(!trip)return null;if(trip.owner_id!==e.SOP_REQUEST_USER.id||trip.signature_json!==signature)fail('此次派工请求已变更，请重新核对');return {ok:true,job_id:trip.job_id,is_new_job:false,order_count:ids.length,lead:staff.workers.find(w=>w.id===staff.lead_id),assigned_workers:staff.workers};};
 const old=await replay();if(old)return old;
 const orders=await rows(e,'SELECT * FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(ids));
 if(orders.length!==ids.length)fail('本车清单中有不存在的出库单，请刷新核对');
 const items=await checked(e,ids.map(id=>orders.find(o=>o.id===id)));items.forEach(assertOrder);
 const busy=await q(e,`SELECT worker_name FROM v2_ops_job_workers WHERE worker_id IN (SELECT value FROM json_each(?)) AND left_at='' LIMIT 1`,JSON.stringify(staff.workers.map(w=>w.id))).first();if(busy)fail(busy.worker_name+'仍在另一任务中，请先交接');
 const id='JOB-LD-'+crypto.randomUUID(),t=new Date().toISOString(),lead=staff.workers.find(w=>w.id===staff.lead_id);
 const data={title:'本车装货 · '+ids.length+' 单',owner_id:e.SOP_REQUEST_USER.id,owner:e.SOP_REQUEST_USER.name,lead_id:staff.lead_id,workers:staff.workers,estimated_minutes:staff.estimated_minutes,job_type:'load_outbound',status:'working',created_at:t,source_type:'outbound_order',source_id:ids[0],labor_department:staff.department};
 const result={ok:true,job_id:id,is_new_job:true,order_count:ids.length,lead,assigned_workers:staff.workers};
 const sql=[q(e,'INSERT INTO ck_load_trips(job_id,request_id,signature_json,owner_id,vehicle_no,driver_name,created_at) VALUES(?,?,?,?,?,?,?)',id,req,signature,e.SOP_REQUEST_USER.id,vehicle,driver,t),
  q(e,"INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count) VALUES(?,'outbound',?,'load_outbound','outbound_order',?,'working',?,?,?,?)",id,items[0].order.biz_class||'',ids[0],lead.id,t,t,staff.workers.length),
  q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)','DISPATCH-'+req,id,0,'sop_native_start',e.SOP_REQUEST_USER.id,e.SOP_REQUEST_USER.name,'{}',JSON.stringify(data),JSON.stringify(result),t),
  q(e,'INSERT INTO sop_records VALUES(?,?,?,?,?,?)',id,'dispatch',1,staff.department,JSON.stringify(data),t),
  q(e,'INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)',req,'v2_outbound_load_start',JSON.stringify(result),t)];
 for(const [i,x] of items.entries())sql.push(q(e,`DELETE FROM ck_load_order_claims WHERE order_id=? AND NOT EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=ck_load_order_claims.job_id AND j.status IN ${active})`,x.order.id),q(e,'INSERT INTO ck_load_order_claims VALUES(?,?)',x.order.id,id),q(e,'INSERT INTO ck_load_order_links VALUES(?,?,?,?,\'\')',id,x.order.id,i,JSON.stringify(snapshot(x))));
 for(const w of staff.workers)sql.push(q(e,'INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES(?,?,?,?,?)','WS-'+crypto.randomUUID(),id,w.id,w.name,t));
 // Preserve the original single-order transition and snapshots through the full validation batch.
 for(const x of items)sql.push(q(e,"UPDATE v2_outbound_orders SET status='ready_to_ship',updated_at=? WHERE id=?",t,x.order.id));
 try{await e.DB.batch(sql);}catch(error){const same=await replay();if(same)return same;if(/load_order_busy|ck_load_order_claims|load_order_changed/.test(error.message))fail(await conflictMessage(e,items));if(/load_worker_busy/.test(error.message))fail('人员已加入另一任务，请刷新并重新核对工牌');throw error;}
 return result;
}
export async function finishLoadTrip(e,b){
 if(!loadTripEnabled(e))return null;
 const trip=await q(e,'SELECT * FROM ck_load_trips WHERE job_id=?',b.job_id).first();if(!trip)return null;
 if(!e.SOP_GROUP_FINISH)fail('请由本车派审员核对并结束任务');
 if(trip.finished_at)return {ok:true,already_completed:true,job_id:trip.job_id,order_count:(await rows(e,'SELECT order_id FROM ck_load_order_links WHERE job_id=?',trip.job_id)).length};
 const job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',trip.job_id).first();if(!['working','awaiting_close'].includes(job.status)||b.complete_job!==true||b.leave_only)fail('请从本车装货页面逐单核对并结束');
 const items=await loadOrdersForJob(e,job),input=b.order_results|| (items.length===1?[{order_id:items[0].order.id,box_count:Number(b.box_count||0),pallet_count:Number(b.pallet_count||0)}]:null);
 if(!Array.isArray(input)||input.length!==items.length||new Set(input.map(x=>x.order_id)).size!==items.length||input.some(x=>!items.some(y=>y.order.id===x.order_id)))fail('请逐单填写本车全部出库单的实装数量');
 const prepared=items.map(x=>{
  assertOrder(x);const r=input.find(r=>r.order_id===x.order.id);
  const box=r.box_count,pallet=r.pallet_count;
  if(!Number.isSafeInteger(box)||!Number.isSafeInteger(pallet)||box<0||pallet<0||box>1e8||pallet>1e8)fail(no(x.order)+'：实装箱数和托数必须为非负整数');
  if(!box&&!pallet)fail(no(x.order)+'：尚未填写实际装货数量，不能完成');
  if((x.planned_box_count>0&&box!==x.planned_box_count)||(x.planned_pallet_count>0&&pallet!==x.planned_pallet_count))fail(no(x.order)+'：实装数量与应装数量不一致，应装 '+x.planned_box_count+' 箱 / '+x.planned_pallet_count+' 托；未装完不得结束，请核对单据');
  return {order_id:x.order.id,display_no:no(x.order),customer:x.order.customer,box_count:box,pallet_count:pallet,status:'shipped',...snapshot(x)};
 });
 const t=new Date().toISOString(),remark=String(b.remark||'').trim().slice(0,2000),output={box_count:prepared.reduce((n,x)=>n+x.box_count,0),pallet_count:prepared.reduce((n,x)=>n+x.pallet_count,0),order_results:prepared,remark};
 const sql=[q(e,'INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)','RES-FINAL-'+job.id,job.id,output.box_count,output.pallet_count,remark,JSON.stringify(output),e.SOP_REQUEST_USER.id,t)];
 for(const x of prepared)sql.push(q(e,'UPDATE ck_load_order_links SET result_json=? WHERE job_id=? AND order_id=?',JSON.stringify(x),job.id,x.order_id));
 for(const x of prepared)sql.push(q(e,"UPDATE v2_outbound_orders SET status='shipped',actual_box_count=?,actual_pallet_count=?,updated_at=? WHERE id=?",x.box_count,x.pallet_count,t,x.order_id));
 sql.push(q(e,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='dispatcher_finish' WHERE job_id=? AND left_at=''",t,t,job.id),
  q(e,"UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,shared_result_json=?,updated_at=? WHERE id=?",JSON.stringify(output),t,job.id),
  q(e,'UPDATE ck_load_trips SET finished_at=? WHERE job_id=? AND finished_at=\'\'',t,job.id),q(e,'DELETE FROM ck_load_order_claims WHERE job_id=?',job.id));
 try{await e.DB.batch(sql);}catch(error){if((await q(e,'SELECT finished_at FROM ck_load_trips WHERE job_id=?',job.id).first())?.finished_at)return {ok:true,already_completed:true,job_id:job.id};if(/load_order_changed/.test(error.message))fail((await conflictMessage(e,items,job.id))+'；本车结果未保存');throw error;}
 return {ok:true,job_id:job.id,order_count:prepared.length,status:'completed'};
}
