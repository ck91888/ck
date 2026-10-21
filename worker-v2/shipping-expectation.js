import {chainNeed,chainAllocation,chainEvent,workChainEnabled} from './work-chain.js';
import {expectedShippingSources,assertShippingCapacity} from './shipping-quantities.js';
export async function shippingExpectation(b,env,u){
 if(b.action!=='sop_shipping_expectation_save')return null;
 if(env.SOP_ENVIRONMENT!=='staging'||!workChainEnabled(env)||u.scope==='field'||!['manager','service'].includes(u.role))throw Error('请由办公室客服或管理员维护预计包装');
 const row=await chainNeed(env,b.id,u),data=structuredClone(row.data),revision=Number(b.revision),request=String(b.client_req_id||''),reason=String(b.reason||'').trim().slice(0,4000);
 if(!request||request.length>100||!Number.isSafeInteger(revision)||!reason)throw Error('请填写版本、请求编号及预计包装依据');
 const fingerprint=JSON.stringify({id:row.id,revision,sources:b.sources,reason});
 const previous=async()=>{const event=await env.DB.prepare('SELECT * FROM sop_events WHERE request_id=?').bind(request).first();if(!event)return null;const result=JSON.parse(event.result_json);if(event.actor_id!==u.id||event.record_id!==row.id||event.action!==b.action||result.fingerprint!==fingerprint)throw Error('请求编号已用于其他内容');return result;};
 const prior=await previous();if(prior)return prior;
 if(revision!==row.revision)throw Error('作业或预约已更新，请刷新后核对');
 if(data.result||data.cargo_groups||data.operation_kind==='direct_forward'||['closed','cancelled'].includes(data.status))throw Error('仅尚未审核的整单加工作业可填写预计包装；分组计划沿用分组预约');
 const expected=expectedShippingSources(data,b.sources),a=await chainAllocation(env,data);try{assertShippingCapacity(expected.components,a.links);}catch{throw Error('预计包装不能少于已有预约的各单位占用；请先逐单调整出库数量与单位，或取消旧预约后再填写预计包装');}
 const t=new Date().toISOString();data.shipping_basis={...expected,version:(data.shipping_basis?.version||0)+1,source_quantity:data.planned_quantity,source_unit:data.planned_unit,reason,by:u.name,actor_id:u.id,at:t};
 const result={ok:true,id:row.id,revision:revision+1,fingerprint};
 try{await env.DB.batch(chainEvent(env,row,data,u,request,b.action,t,result));}catch(error){const committed=await previous();if(committed)return committed;throw error;}
 return result;
}
