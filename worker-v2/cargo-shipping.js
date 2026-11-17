import {chainNeed,chainEvent,chainDate,workChainEnabled} from './work-chain.js';
import {nextOutboundDisplayNo} from './outbound-number.js';
import {cargoAvailability} from './cargo-allocation.js';
const q=(env,sql,...a)=>env.DB.prepare(sql).bind(...a),text=(v,n=4000)=>String(v??'').trim().slice(0,n);
export async function cargoShipping(b,env,u){
 if(!['sop_cargo_shipping','sop_cargo_outbounds_create'].includes(b.action))return null;
 if(!workChainEnabled(env))throw Error('资料分组未启用');
 const row=await chainNeed(env,b.id,u),data=structuredClone(row.data);if(!data.cargo_groups)throw Error('此计划请沿用整单出库');
 if(u.scope==='field'||!['manager','service'].includes(u.role))throw Error('请由办公室客服安排出库');
 if(b.action==='sop_cargo_shipping')return {ok:true,id:row.id,revision:row.revision,...await cargoAvailability(env,data)};
 const request=text(b.client_req_id,100),fingerprint=JSON.stringify({id:row.id,revision:b.revision,outbounds:b.outbounds});if(!request)throw Error('缺少请求编号');
 const previous=async()=>{const p=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(!p)return null;const r=JSON.parse(p.result_json);if(p.actor_id!==u.id||p.record_id!==row.id||p.action!==b.action||r.fingerprint!==fingerprint)throw Error('出库请求编号冲突');return r;};const old=await previous();if(old)return old;
 if(Number(b.revision)!==row.revision)throw Error('资料组已更新，请刷新后选择');
 if(['cancelled','closed'].includes(data.status)||data.needs_clarification)throw Error('计划已关闭或要求不完整');
 if(!Array.isArray(b.outbounds)||!b.outbounds.length||b.outbounds.length>20)throw Error('每次安排1至20条出库计划');
 const available=await cargoAvailability(env,data),selected=new Set(),outbounds=[],t=new Date().toISOString();
 for(const item of b.outbounds){
  const expected_ship_at=chainDate(item.expected_ship_at),outbound_mode=text(item.outbound_mode,40);if(!['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'].includes(outbound_mode))throw Error('请选择出库方式');
  if(!Array.isArray(item.groups)||!item.groups.length)throw Error('请勾选本次出库的已审核组');
  const allocations=item.groups.map(selection=>{
   const group=available.groups.find(g=>g.id===selection.group_id);if(!group||!group.available||selected.has(group.id))throw Error('所选组未审核、已占用或不属于此计划');
   const output=group.output;if(selection.output_id!==output.id||Number(selection.output_version)!==output.version)throw Error('成果版本已更新，请重新选择');
   if(selection.quantity!==undefined&&Number(selection.quantity)!==output.quantity||selection.unit!==undefined&&selection.unit!==output.unit)throw Error('组内部分发货须先按可识别实际范围拆组');
   selected.add(group.id);return {group_id:group.id,group_name:group.name,output_id:output.id,output_version:output.version,mapping_version:output.mapping_version,quantity:output.quantity,unit:output.unit,input_box_count:output.input_box_count,range:output.range,marks:output.marks,identifier:output.identifier,documents:output.documents,public_documents:output.public_documents};
  });
  const id='OB-'+crypto.randomUUID(),display_no=await nextOutboundDisplayNo(env,data.customer,expected_ship_at);outbounds.push({id,display_no,expected_ship_at,outbound_mode,destination:text(item.destination),po_no:text(item.po_no,240),outbound_requirement:text(item.outbound_requirement),allocations,boxes:allocations.filter(a=>a.unit==='箱').reduce((n,a)=>n+a.quantity,0),pallets:allocations.filter(a=>a.unit==='托').reduce((n,a)=>n+a.quantity,0)});
 }
 data.links=[...(data.links||[]),...outbounds.map(o=>({outbound_id:o.id,quantity:o.allocations.length,unit:'组',phase:'confirmed',cargo_allocations:o.allocations,by:u.name,actor_id:u.id,at:t}))];
 const result={ok:true,id:row.id,revision:row.revision+1,fingerprint,outbounds:outbounds.map(o=>({id:o.id,display_no:o.display_no,expected_ship_at:o.expected_ship_at,group_count:o.allocations.length}))},statements=chainEvent(env,row,data,u,request,b.action,t,result);
 for(const o of outbounds)statements.push(q(env,`INSERT INTO v2_outbound_orders(id,order_date,customer,biz_class,outbound_mode,instruction,status,source_inbound_plan_id,created_by,created_at,updated_at,display_no,expected_ship_at,wms_work_order_no,planned_box_count,planned_pallet_count,uses_stock_operation,stock_operation_status,stock_operation_result_json,outbound_requirement,destination,po_no)
 VALUES(?,?,?,?,?,?,'pending_issue',?,?,?,?,?,?,?,?,?,0,'completed',?,?,?,?)`,o.id,o.expected_ship_at,data.customer,row.department==='import'?'bulk':row.department,o.outbound_mode,o.allocations.map(a=>a.group_name+' · '+a.range+' · '+a.quantity+' '+a.unit).join('\n'),data.source_type==='inbound'?data.source_id:'',u.name,t,t,o.display_no,o.expected_ship_at,data.supply_chain_no||'',o.boxes,o.pallets,JSON.stringify({grouped:true,allocations:o.allocations}),o.outbound_requirement,o.destination,o.po_no));
 try{await env.DB.batch(statements);}catch(error){const committed=await previous();if(committed)return committed;throw error;}return result;
}
