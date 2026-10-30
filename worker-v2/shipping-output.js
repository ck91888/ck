import {chainNeed,chainAllocation,chainEvent,workChainEnabled} from './work-chain.js';
import {shippingComponents,assertShippingCapacity,shippingCapacity,shippingFullyAllocated} from './shipping-quantities.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const text=value=>String(value??'').trim().slice(0,4000);
// Amend only the shipping basis. Original job feedback, reviews, work metrics and
// every existing outbound snapshot stay untouched. SOP's event revision trigger
// serializes this with all bookings and retains before/after audit history.
export async function shippingOutput(body,env,user) {
 if(!['sop_shipping_output_propose','sop_shipping_output_review'].includes(body.action))return null;
 if(env.SOP_ENVIRONMENT!=='staging'||!workChainEnabled(env))throw Error('出货构成更正未启用');
 if(user?.scope==='field'||!['manager','service','reviewer','dispatcher'].includes(user?.role))throw Error('请由办公室授权人员处理出货构成更正');
 const row=await chainNeed(env,body.id,user),data=structuredClone(row.data),review=body.action==='sop_shipping_output_review';
 if(review){
  if(!['manager','reviewer','dispatcher'].includes(user.role))throw Error('客服只能申请更正，请交有权审核人员审核');
  if(user.role==='dispatcher'){
   const task=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='task'",data.task_id||'').first(),d=task?JSON.parse(task.state):{};
   if(!(d.owner_id===user.id||(d.delegates||[]).includes(user.id)&&!(d.owner_transfers||[]).some(x=>x.from_id===user.id)))throw Error('只能审核本人派发或获授权的任务');
  }
 }
 const request=text(body.client_req_id),revision=Number(body.revision),reason=text(body.reason);
 if(!request||request.length>100||!Number.isSafeInteger(revision)||revision<1)throw Error('请求编号或作业版本无效');
 const components=review?null:shippingComponents(body.components);
 if(!reason)throw Error(review?'请填写审核说明':'请填写更正原因及核实依据');
 if(review&&!['pass','return'].includes(body.decision))throw Error('审核结论无效');
 const fingerprint=JSON.stringify({id:row.id,revision,components,reason,decision:review?body.decision:null,change_id:review?body.change_id:null});
 const previous=async()=>{const e=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(!e)return null;const r=JSON.parse(e.result_json);if(e.record_id!==row.id||e.actor_id!==user.id||e.action!==body.action||r.fingerprint!==fingerprint)throw Error('请求编号已用于其他内容');return r;};
 const prior=await previous();if(prior)return prior;
 if(row.revision!==revision)throw Error('作业或出库安排已更新，请刷新后核对');
 if(data.cargo_groups)throw Error('分组及多工序计划请沿用分组成果审核与出库流程');
 if(!data.result||!['waiting_customer','linked'].includes(data.status)||data.operation_kind==='direct_forward')throw Error('仅已审核的整单加工作业可更正实际出货构成');
 const allocation=await chainAllocation(env,data),t=new Date().toISOString();
 if(!review){
  if(data.shipping_output_change?.status==='pending')throw Error('已有待审核更正，请先审核');
  assertShippingCapacity(components,allocation.links);
  data.shipping_output_change={id:request,status:'pending',components,reason,by:user.name,actor_id:user.id,at:t,base_version:data.shipping_output?.version||0};
 }else{
  const change=data.shipping_output_change;
  if(!change||change.status!=='pending'||change.id!==body.change_id)throw Error('更正申请已变化或已审核，请刷新');
  if(change.base_version!==(data.shipping_output?.version||0))throw Error('出货构成版本已变化，请重新申请');
  change.status=body.decision==='pass'?'approved':'returned';change.review={decision:body.decision,reason,by:user.name,actor_id:user.id,at:t};
  if(body.decision==='pass'){
   assertShippingCapacity(change.components,allocation.links);
   data.shipping_output={version:change.base_version+1,components:change.components,change_id:change.id,reason:change.reason,by:change.by,actor_id:change.actor_id,at:change.at,review:change.review};
   data.status=shippingFullyAllocated(shippingCapacity(data,allocation.links))?'linked':'waiting_customer';
  }
 }
 const result={ok:true,id:row.id,revision:row.revision+1,fingerprint};
 try{await env.DB.batch(chainEvent(env,row,data,user,request,body.action,t,result));}catch(error){const committed=await previous();if(committed)return committed;throw error;}
 return result;
}
