import {chainNeed,chainAllocation,chainDate,chainEvent,workChainEnabled} from './work-chain.js';
import {nextOutboundDisplayNo} from './outbound-number.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args),str=(v,n=4000)=>String(v??'').trim().slice(0,n);
const modes=['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'];
export async function createOutboundBookingBatch(body,env){
 if(env.SOP_ENVIRONMENT!=='staging'||!workChainEnabled(env))throw Error('多条出库预约功能未启用');
 const user=env.SOP_REQUEST_USER;if(!user||user.scope==='field'||!['manager','service'].includes(user.role))throw Error('请由办公室客服或管理员安排出库');
 const row=await chainNeed(env,body.id,user,true),request=str(body.client_req_id,80),revision=Number(body.revision);
 if(!/^[a-z0-9-]{10,80}$/i.test(request)||!Number.isSafeInteger(revision)||revision<1)throw Error('请求编号或作业版本无效');
 if(!Array.isArray(body.outbounds)||!body.outbounds.length||body.outbounds.length>20)throw Error('每次请安排1至20条出库计划');
 const orders=body.outbounds.map((o,i)=>{const quantity=Number(o.quantity);if(!Number.isSafeInteger(quantity)||quantity<1)throw Error('第'+(i+1)+'条出库数量必须为正整数');if(!modes.includes(o.outbound_mode))throw Error('第'+(i+1)+'条请选择出库方式');return{quantity,unit:str(o.unit,10),expected_ship_at:chainDate(o.expected_ship_at),outbound_mode:o.outbound_mode,destination:str(o.destination),po_no:str(o.po_no,240),outbound_requirement:str(o.outbound_requirement)};});
 const fingerprint=[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify({id:row.id,revision,outbounds:orders}))))].map(v=>v.toString(16).padStart(2,'0')).join('');
 const previous=async()=>{const event=await q(env,'SELECT actor_id,record_id,action,result_json FROM sop_events WHERE request_id=?',request).first();if(!event)return null;const result=JSON.parse(event.result_json);if(event.actor_id!==user.id||event.record_id!==row.id||event.action!=='v2_outbound_order_batch_create'||result.request_fingerprint!==fingerprint)throw Error('出库请求编号已用于其他内容');return result;};
 const prior=await previous();if(prior)return prior;
 if(row.revision!==revision)throw Error('作业需求已更新，请刷新后安排出库');
 const d=row.data;if(['cancelled','closed'].includes(d.status)||d.needs_clarification)throw Error('作业需求已关闭或要求不完整');
 const a=await chainAllocation(env,d),total=orders.reduce((sum,o)=>sum+o.quantity,0);
 if(!a.quantity||!a.unit)throw Error('请先填写作业数量和单位');
 if(!Number.isSafeInteger(total)||a.used+total>a.quantity)throw Error('本批出库合计超过剩余可安排数量');
 if(orders.some(o=>o.unit&&o.unit!==a.unit))throw Error('出库单位须沿用该作业的可出库成果单位');
 const t=new Date().toISOString(),outbounds=[];for(const o of orders)outbounds.push({...o,unit:a.unit,id:'OB-'+crypto.randomUUID(),display_no:await nextOutboundDisplayNo(env,d.customer,o.expected_ship_at)});
 const data=structuredClone(d);data.links=[...(d.links||[]),...outbounds.map(o=>({outbound_id:o.id,quantity:o.quantity,unit:a.unit,phase:d.result?'confirmed':'planned',by:user.name,at:t}))];if(d.result)data.status=a.used+total===a.quantity?'linked':'waiting_customer';
 const result={ok:true,id:row.id,need_id:row.id,revision:row.revision+1,request_fingerprint:fingerprint,outbounds:outbounds.map(o=>({id:o.id,display_no:o.display_no,quantity:o.quantity,unit:o.unit,expected_ship_at:o.expected_ship_at}))};
 const statements=chainEvent(env,row,data,user,request,'v2_outbound_order_batch_create',t,result);
 for(const o of outbounds)statements.push(q(env,`INSERT INTO v2_outbound_orders(id,order_date,customer,biz_class,outbound_mode,instruction,status,source_inbound_plan_id,created_by,created_at,updated_at,display_no,expected_ship_at,wms_work_order_no,planned_box_count,planned_pallet_count,uses_stock_operation,stock_operation_status,stock_operation_result_json,outbound_requirement,destination,po_no)
  VALUES(?,?,?,?,?,?,'pending_issue',?,?,?,?,?,?,?,?,?,0,?,?,?,?,?)`,o.id,o.expected_ship_at,d.customer,row.department==='import'?'bulk':row.department,o.outbound_mode,d.instructions,d.source_type==='inbound'?d.source_id:'',user.name,t,t,o.display_no,o.expected_ship_at,d.supply_chain_no||'',a.unit==='箱'?o.quantity:0,a.unit==='托'?o.quantity:0,d.result?'completed':'pending',d.result?JSON.stringify(d.result):'',o.outbound_requirement,o.destination,o.po_no));
 try{await env.DB.batch(statements);}catch(error){const committed=await previous();if(committed)return committed;throw error;}
 return result;
}
