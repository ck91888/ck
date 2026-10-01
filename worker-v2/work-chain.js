import {recordNumbers} from './document-numbers.js';
import {legacyLoadClaim} from './legacy-load.js';
// One upstream requirement owns the work instructions and materials. Shipping
// plans allocate its quantities; they never create a second work requirement.
export const workChainEnabled = env => env.SOP_UPGRADE_ENABLED === 'true' && env.SOP_WORK_CHAIN_ENABLED === 'true';
const q = (env, sql, ...args) => env.DB.prepare(sql).bind(...args);
const str = value => String(value ?? '').trim();
const frozen = ['shipped', 'completed', 'cancelled'];
const positive = value => { const n = Number(value); if (!Number.isSafeInteger(n) || n < 1) throw Error('数量必须为正整数'); return n; };
export function chainDate(value) {
 const s = str(value); if (!/^\d{4}-\d{2}-\d{2}$/.test(s) || !Number.isFinite(Date.parse(s)) || new Date(s).toISOString().slice(0,10) !== s) throw Error('请填写有效的出库日期'); return s;
}
export async function chainNeed(env, id, user, write = false) {
 const row = await q(env, "SELECT * FROM sop_records WHERE kind='need' AND id=?", str(id)).first();
 if (!row) throw Error('请先选择关联作业需求 / 작업 요청을 선택하세요');
 if (!user || (user.role !== 'manager' && (!(user.departments || []).includes(row.department) || (write && !['service','dispatcher','reviewer'].includes(user.role))))) throw Error('无此作业需求权限');
 return {...row, data: JSON.parse(row.state)};
}
export async function chainAllocation(env, data, knownOrders) {
 const links = data.links || [], ids = [...new Set(links.map(l => l.outbound_id))];
 const orders = knownOrders || (ids.length ? (await q(env, `SELECT id,status,display_no,expected_ship_at FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))`, JSON.stringify(ids)).all()).results : []);
 const current = links.filter(l => orders.some(o => o.id === l.outbound_id && o.status !== 'cancelled'));
 // Input cargo and packed output need not have the same unit (cartons -> pallets).
 const basis = data.result || data.shipping_basis;
 const quantity = Number(basis?.quantity ?? data.planned_quantity) || 0, unit = basis?.unit || data.planned_unit || '';
 return {quantity, unit, used: current.reduce((sum,l) => sum + Number(l.quantity), 0), links: current, orders};
}
export function assertShippingResult(result, links) {
 const planned = links.filter(l => l.phase === 'planned');
 if (!planned.length) return;
 const quantities = new Map();
 for (const l of planned) quantities.set(l.unit, (quantities.get(l.unit) || 0) + Number(l.quantity));
 const booked = [...quantities].map(([u,n]) => n + ' ' + u).join('、');
 if (planned.some(l => l.unit !== result.unit)) throw Error(`成果单位不一致：本次填写 ${result.quantity} ${result.unit}，已预约出库 ${booked}。箱数与托数不能直接比较，请联系办公室在作业计划中调整出库数量与单位。 / 출고 예약과 완료 단위가 다릅니다.`);
 const total = quantities.get(result.unit);
 if (total > Number(result.quantity)) throw Error(`实际成果不足：本次填写 ${result.quantity} ${result.unit}，已预约出库 ${total} ${result.unit}，相差 ${total-Number(result.quantity)} ${result.unit}。请核实实际成果或调整出库预约。 / 완료 수량이 출고 예약보다 적습니다.`);
}
export async function shippingBasisStatements(env, row, data, body, user, t) {
 if (!workChainEnabled(env) || data.result || ['closed','cancelled'].includes(data.status)) throw Error('只能调整尚未完成的出库预约');
 if (!['manager','service'].includes(user.role)) throw Error('请由办公室调整出库数量与单位');
 if (data.operation_kind === 'direct_forward') throw Error('直接转发未加工，请按原货物单位安排出库');
 const quantity = positive(body.quantity), unit = str(body.unit), reason = str(body.reason);
 if (!['箱','件','托','单','批'].includes(unit)) throw Error('出库单位无效');
 if (!reason) throw Error('请填写调整说明');
 const a = await chainAllocation(env,data), allocations = body.allocations;
 if (!Array.isArray(allocations) || allocations.length !== a.links.length || new Set(allocations.map(l=>l.outbound_id)).size !== allocations.length || allocations.some(l=>!a.links.some(old=>old.outbound_id===l.outbound_id))) throw Error('出库预约已变化，请刷新后逐单核对');
 if (a.links.some(l=>l.phase!=='planned') || a.orders.some(o=>frozen.includes(o.status)&&o.status!=='cancelled')) throw Error('已确认或已出库的计划不能调整');
 const values = new Map(allocations.map(l=>[l.outbound_id,positive(l.quantity)]));
 if ([...values.values()].reduce((n,v)=>n+v,0)>quantity) throw Error('预约合计超过预计可出库总数量');
 const statements=[];
 for (const l of a.links) {
  const multi=env.SOP_ENVIRONMENT==='staging';
  const loading=await q(env,"SELECT id FROM v2_ops_jobs j WHERE job_type='load_outbound' AND status IN ("+(multi?"'pending','working','awaiting_close','completed'":"'pending','working','completed'")+") AND ((related_doc_type='outbound_order' AND related_doc_id=?)"+(multi?" OR EXISTS(SELECT 1 FROM ck_load_order_links l WHERE l.job_id=j.id AND l.order_id=?)":"")+") LIMIT 1",l.outbound_id,...(multi?[l.outbound_id]:[])).first();
  if (loading) throw Error('已开始装货的计划不能调整');
  statements.push(q(env,'UPDATE v2_outbound_orders SET planned_box_count=?,planned_pallet_count=?,updated_at=? WHERE id=?',unit==='箱'?values.get(l.outbound_id):0,unit==='托'?values.get(l.outbound_id):0,t,l.outbound_id));
 }
 data.shipping_basis={quantity,unit,reason:reason.slice(0,4000),by:user.name,at:t};
 data.links=(data.links||[]).map(l=>values.has(l.outbound_id)?{...l,quantity:values.get(l.outbound_id),unit,adjustment_reason:reason.slice(0,4000)}:l);
 return [...statements,...notifyMaterialChange(env,row,user,t,'出库数量与单位已调整：'+reason,{type:'shipping_adjustment',diff:id=>{const before=(row.data.links||[]).find(l=>l.outbound_id===id),after=data.links.find(l=>l.outbound_id===id);return {shipping_quantity:{from:before?before.quantity+' '+before.unit:'',to:after?after.quantity+' '+after.unit:''}};}})];
}
export function chainEvent(env, row, data, user, request, action, t, result = {}) {
 const revision = row.revision + 1;
 return [q(env, 'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)', request, row.id, row.revision, action, user.id, user.name, row.state, JSON.stringify(data), JSON.stringify({ok:true,id:row.id,revision,...result}), t),
 q(env, 'UPDATE sop_records SET revision=?,state=?,updated_at=? WHERE id=?', revision, JSON.stringify(data), t, row.id)];
}
export async function chainOutboundStatements(env, body, id, t) {
 const user = env.SOP_REQUEST_USER;
 const row = await chainNeed(env, body.sop_existing_need_id, user, true), d = row.data;
 if (Number(body.sop_need_revision) !== row.revision) throw Error('作业需求已更新，请重新选择后安排出库');
 if (['cancelled','closed'].includes(d.status) || d.needs_clarification) throw Error('作业需求已关闭或要求不完整');
 chainDate(body.expected_ship_at);
 if (str(body.customer) !== d.customer || body.biz_class !== (row.department === 'import' ? 'bulk' : row.department)) throw Error('客户和业务部门须与关联作业需求一致');
 if (Number(body.uses_stock_operation) === 1 || (str(body.instruction) && str(body.instruction) !== d.instructions) || body.lines?.length) throw Error('操作要求、货物明细请在作业需求中维护');
 const a = await chainAllocation(env,d), quantity = positive(body.sop_link_quantity);
 if (!a.quantity || !a.unit) throw Error('请先在作业需求中填写计划数量和单位，再安排出库');
 if (a.used + quantity > a.quantity) throw Error('本次出库数量超过该作业剩余可安排数量');
 d.links = [...(d.links || []), {outbound_id:id,quantity,unit:a.unit,phase:d.result?'confirmed':'planned',by:user.name,at:t}];
 if (d.result) d.status = a.used + quantity === a.quantity ? 'linked' : 'waiting_customer';
 return [...chainEvent(env,row,d,user,'LINK-'+id,'sop_need_schedule',t), q(env, `UPDATE v2_outbound_orders SET instruction=?,source_inbound_plan_id=?,wms_work_order_no=?,planned_box_count=?,planned_pallet_count=?,uses_stock_operation=0,stock_operation_status=?,stock_operation_result_json=?,updated_at=? WHERE id=?`, d.instructions,d.source_type==='inbound'?d.source_id:'',d.supply_chain_no||'',a.unit==='箱'?quantity:0,a.unit==='托'?quantity:0,d.result?'completed':'pending',d.result?JSON.stringify(d.result):'',t,id)];
}
export async function chainLinked(env, id) {
 return (await q(env, `SELECT * FROM sop_records WHERE kind='need' AND (json_extract(state,'$.source_id')=? OR EXISTS(SELECT 1 FROM json_each(state,'$.links') l WHERE json_extract(l.value,'$.outbound_id')=?))`,id,id).all()).results.map(row => ({...row,data:JSON.parse(row.state)}));
}
export async function guardWorkChain(body, env) {
 if (!workChainEnabled(env)) return null;
 if (body.action === 'v2_inbound_plan_create' && (body.auto_create_outbound || body.link_outbound || body.linked_outbounds?.length || body.outbound_orders?.length)) return '出库计划必须建在对应作业需求下';
 if (body.action === 'v2_outbound_order_create' && !body.sop_existing_need_id) return '请先建立或选择作业需求，再安排出库；直接转发也须建立需求';
 const load = /^v2_outbound_load_(start|finish)$/.test(body.action || '');
 if (!load && !/^v2_outbound_order_update(?:_status)?$/.test(body.action || '')) return null;
 let id = body.order_id || body.id || body.related_doc_id;
 if (load && body.job_id) id = (await q(env,'SELECT related_doc_id FROM v2_ops_jobs WHERE id=?',body.job_id).first())?.related_doc_id || id;
 if (!id) return null;
 const needs = await chainLinked(env,id);
 if (!needs.length && (load || body.status === 'issued') && !(body.action==='v2_outbound_load_finish'&&await legacyLoadClaim(env,body.job_id))) return '此出库计划缺少作业需求，请先关联作业需求';
 if (load && needs.some(n => !n.data.result || ['cancelled'].includes(n.data.status))) return '关联作业尚未完成审核；直接转发须先确认收货和可发货数量';
 if (load) {const order=await q(env,'SELECT status,warehouse_ack_required FROM v2_outbound_orders WHERE id=?',id).first();if(!order||frozen.includes(order.status))return '出库计划已取消或已完成';if(Number(order.warehouse_ack_required))return '作业要求或资料已更新，请先确认最新变更再装货';}
 if (body.action === 'v2_outbound_order_update' && needs.length) {
  const old = await q(env,'SELECT * FROM v2_outbound_orders WHERE id=?',id).first();
  for (const key of ['customer','biz_class','instruction','uses_stock_operation','source_inbound_plan_id','wms_work_order_no','planned_box_count','planned_pallet_count']) if (Object.hasOwn(body,key) && str(body[key]) !== str(old[key])) return '客户、操作要求和分配数量由关联作业需求统一管理，请从作业需求修改';
 }
 return null;
}
// Old source attachments remain retrievable and are labelled as historical.
// No copying of blobs: all consumers receive the same attachment ID/file key.
export async function workMaterials(env, rows) {
 if (!rows.length) return [];
 const files=(await q(env,`SELECT DISTINCT a.* FROM v2_attachments a JOIN sop_records n ON n.kind='need' AND n.id IN (SELECT value FROM json_each(?))
 WHERE NOT EXISTS(SELECT 1 FROM json_each(n.state,'$.removed_material_ids') r WHERE r.value=a.id) AND (
 (a.related_doc_type='sop_need' AND a.related_doc_id=n.id) OR
 (a.related_doc_type='inbound_plan' AND a.attachment_category IN ('inbound_material','batch_work_material') AND json_extract(n.state,'$.source_type')='inbound' AND a.related_doc_id=json_extract(n.state,'$.source_id')) OR
 (a.related_doc_type='outbound_order' AND a.attachment_category='outbound_material' AND (
 (json_extract(n.state,'$.source_type')='outbound' AND a.related_doc_id=json_extract(n.state,'$.source_id')) OR
 EXISTS(SELECT 1 FROM json_each(n.state,'$.links') l WHERE json_extract(l.value,'$.outbound_id')=a.related_doc_id)))) ORDER BY a.created_at DESC`,JSON.stringify(rows.map(r=>r.id))).all()).results;
 return files.map(f=>({...f,historical:f.related_doc_type!=='sop_need'&&f.attachment_category!=='batch_work_material',batch:f.attachment_category==='batch_work_material',material_kind:f.attachment_category}));
}
export async function workMaterialRead(body,env,user) {
 if (!workChainEnabled(env)) return null;
 if (body.action==='sop_work_materials') {const row=await chainNeed(env,body.id,user);return {ok:true,items:await workMaterials(env,[row]),revision:row.revision};}
 if (body.action!=='sop_work_need_search') return null;
 if(!user||!['manager','service','dispatcher','reviewer'].includes(user.role))throw Error('无此操作权限');
 const search=str(body.search).slice(0,100),offset=Math.max(0,Math.floor(Number(body.offset)||0)),dep=user.role==='manager'?'':` AND department IN (${(user.departments||[]).map(()=>'?').join(',')||"''"})`;
 const rows=(await q(env,`SELECT * FROM sop_records WHERE kind='need' AND json_extract(state,'$.status') NOT IN ('closed','cancelled') AND (?='' OR id LIKE ? OR json_extract(state,'$.customer') LIKE ? OR json_extract(state,'$.title') LIKE ? OR EXISTS(SELECT 1 FROM sop_document_numbers dn WHERE dn.record_id=sop_records.id AND dn.display_no LIKE ?) OR EXISTS(SELECT 1 FROM v2_inbound_plans ip WHERE ip.id=json_extract(state,'$.source_id') AND ip.display_no LIKE ?))${dep} ORDER BY updated_at DESC LIMIT 31 OFFSET ?`,search,...Array(5).fill('%'+search+'%'),...(user.role==='manager'?[]:user.departments||[]),offset).all()).results;
 const ids=[...new Set(rows.flatMap(row=>(JSON.parse(row.state).links||[]).map(l=>l.outbound_id)))];
 const orders=ids.length?(await q(env,'SELECT id,status,display_no,expected_ship_at FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(ids)).all()).results:[];
 const items=await Promise.all(rows.slice(0,30).map(async row=>{const data=JSON.parse(row.state),a=await chainAllocation(env,data,orders);return {id:row.id,revision:row.revision,department:row.department,...data,remaining:Math.max(0,a.quantity-a.used),schedule_unit:a.unit,schedule_quantity:a.quantity,shipping_plans:a.orders.filter(o=>(data.links||[]).some(l=>l.outbound_id===o.id))};}));
 return {ok:true,items:await recordNumbers(env,items),more:rows.length>30,offset};
}
export function notifyMaterialChange(env,row,user,t,summary,change={}) {
 const ids=[...new Set([...(row.data.links||[]).map(l=>l.outbound_id),...(row.data.source_type==='outbound'?[row.data.source_id]:[])])],out=[];
 for(const id of ids){
  out.push(q(env,`UPDATE v2_outbound_orders SET revision_no=COALESCE(revision_no,0)+1,warehouse_ack_required=1,warehouse_ack_by='',warehouse_ack_at='',last_modified_by=?,last_modified_at=?,updated_at=? WHERE id=? AND status NOT IN ('shipped','completed','cancelled')`,user.name,t,t,id));
  out.push(q(env,`INSERT INTO v2_outbound_order_change_logs(id,order_id,revision_no,change_type,changed_by,changed_at,diff_json,summary_text,warehouse_ack_required) SELECT ?,id,revision_no,?,?,?,?, ?,1 FROM v2_outbound_orders WHERE id=? AND status NOT IN ('shipped','completed','cancelled')`,'CHG-'+crypto.randomUUID(),change.type||'material_update',user.name,t,JSON.stringify(typeof change.diff==='function'?change.diff(id):change.diff||{}),summary,id));
 }
 return out;
}
export async function uploadWorkMaterial(form,env) {
 const user=env.SOP_REQUEST_USER,row=await chainNeed(env,form.get('related_doc_id'),user,true),file=form.get('file'),req=str(form.get('client_req_id'));
 if(!req)throw Error('上传请求编号缺失，请刷新页面');
 const prior=await q(env,'SELECT actor_id,action,result_json FROM sop_events WHERE request_id=?',req).first();
 if(prior){if(prior.actor_id!==user.id||prior.action!=='sop_work_material_upload')throw Error('上传请求编号冲突');return JSON.parse(prior.result_json);}
 if(['closed','cancelled'].includes(row.data.status))throw Error('作业已关闭，不能新增作业资料');
 if(Number(form.get('revision'))!==row.revision)throw Error('作业资料已更新，请刷新后重新上传');
 const ext=str(file?.name).split('.').pop().toLowerCase(),types={pdf:'application/pdf',xlsx:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',xls:'application/vnd.ms-excel',csv:'text/csv',jpg:'image/jpeg',jpeg:'image/jpeg',png:'image/png',webp:'image/webp'};
 if(!types[ext]||!file.size||file.size>20*1024*1024)throw Error('支持20MB以内的PDF、Excel、CSV、JPG、PNG、WebP');
 const category=str(form.get('material_kind'))||'work_material';if(!['work_material','pallet_label','shipping_document','product_label'].includes(category))throw Error('作业资料类型无效');
 const feedback=category==='work_material';
 if(feedback&&user.scope!=='field'&&!['manager','service'].includes(user.role))throw Error('作业说明／明细请由现场人员上传，或由办公室代录仓库反馈');
 if(!feedback&&(user.scope==='field'||!['manager','service'].includes(user.role)))throw Error('托唛、出库单和标签请由办公室客服上传');
 const id='ATT-'+crypto.randomUUID(),key='v2/sop_need/'+row.id+'/'+id+'.'+ext,t=new Date().toISOString(),d=structuredClone(row.data);
 d.material_version=(d.material_version||0)+1;if(!feedback)d.requirement_version=(d.requirement_version||1)+1;
 d.last_material_change={action:'upload',attachment_id:id,file_name:str(file.name).slice(0,240),kind:category,by:user.name,at:t};
 const result={ok:true,id,file_key:key,revision:row.revision+1};
 const batch=chainEvent(env,row,d,user,req,'sop_work_material_upload',t,{...result,need_id:row.id});
 batch.push(q(env,'INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',id,'sop_need',row.id,category,str(file.name).slice(0,240),key,file.size,types[ext],user.name,t));
 if(!feedback)batch.push(...notifyMaterialChange(env,row,user,t,'客服作业资料已更新，请查看关联作业计划'));
 await env.R2_BUCKET.put(key,file.stream(),{httpMetadata:{contentType:types[ext]}});
 // On an ambiguous D1 failure keep the object; never delete a possibly committed
 // attachment. Retrying this request ID returns the stored result.
 await env.DB.batch(batch);return {...result,need_id:row.id};
}

// A single correlated SQL expression serves list counts and the material filter.
export const workMaterialCountSql = `(SELECT COUNT(DISTINCT a.id) FROM v2_attachments a WHERE
 (a.related_doc_type='outbound_order' AND a.related_doc_id=v2_outbound_orders.id AND a.attachment_category='outbound_material') OR
 EXISTS(SELECT 1 FROM sop_records n WHERE n.kind='need' AND
 (json_extract(n.state,'$.source_id')=v2_outbound_orders.id OR EXISTS(SELECT 1 FROM json_each(n.state,'$.links') l WHERE json_extract(l.value,'$.outbound_id')=v2_outbound_orders.id))
 AND NOT EXISTS(SELECT 1 FROM json_each(n.state,'$.removed_material_ids') r WHERE r.value=a.id)
 AND ((a.related_doc_type='sop_need' AND a.related_doc_id=n.id) OR
 (a.related_doc_type='inbound_plan' AND a.attachment_category IN ('inbound_material','batch_work_material') AND json_extract(n.state,'$.source_type')='inbound' AND a.related_doc_id=json_extract(n.state,'$.source_id')) OR
 (a.related_doc_type='outbound_order' AND a.attachment_category='outbound_material' AND (a.related_doc_id=json_extract(n.state,'$.source_id') OR EXISTS(SELECT 1 FROM json_each(n.state,'$.links') l WHERE json_extract(l.value,'$.outbound_id')=a.related_doc_id))))))`;
