const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const digest=async text=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text)))].map(n=>n.toString(16).padStart(2,'0')).join('');
const ignored=new Set('client_req_id action k sop_key ops_key created_by by actor modified_by operator operator_name requested_by uploaded_by'.split(' '));
function stable(value,root=false){if(Array.isArray(value))return value.map(v=>stable(v));if(value&&typeof value==='object')return Object.fromEntries(Object.keys(value).sort().filter(k=>!root||!ignored.has(k)).map(k=>[k,stable(value[k])]));return value;}
const context=(e,u)=>digest(JSON.stringify([u.id,u.scope||'',e.SOP_OPERATION_CONTEXT?.context_id||'legacy-personal-session']));
const signature=b=>digest(JSON.stringify(stable(b,true)));
const allowed=(u,department='')=>u&&u.scope!=='field'&&u.scope!=='kiosk'&&['manager','service'].includes(u.role)&&(!department||u.role==='manager'||u.departments?.includes(department));
const result=r=>({ok:true,id:r.id,revision:r.revision});
export async function needCreateReceiptMeta(b,e,u){return b.action==='sop_need_create'&&e.SOP_ENVIRONMENT==='staging'?{_submit_context:await context(e,u),_create_signature:await signature(b)}:{};}
export async function needCreateReplay(b,e,u){
 if(b.action!=='sop_need_create'||e.SOP_ENVIRONMENT!=='staging'||!b.client_req_id)return null;
 const row=await q(e,'SELECT * FROM sop_events WHERE request_id=?',b.client_req_id).first();if(!row)return null;const saved=JSON.parse(row.result_json);
 if(!saved._create_signature)return null;
 const record=await q(e,'SELECT department FROM sop_records WHERE id=? AND kind=?',row.record_id,'need').first();
 if(!record||row.action!==b.action||row.actor_id!==u.id||saved._submit_context!==await context(e,u)||!allowed(u,'bulk'))throw Error('此提交不属于当前登录身份，不能复用原请求');
 if(saved._create_signature!==await signature(b))throw Error('原提交的内容已冻结，请先查询原保存结果；不能用同一编号提交不同内容');
 return result(saved);
}
export async function needCreateStatus(b,e,u){
 if(b.action!=='sop_need_create_status')return null;
 if(e.SOP_ENVIRONMENT!=='staging'||!allowed(u))throw Error('无此提交查询权限');
 const id=String(b.request_id||'');if(!id||id.length>100)throw Error('缺少有效提交编号');
 const unknown={ok:true,state:'unknown'};const row=await q(e,'SELECT * FROM sop_events WHERE request_id=?',id).first();
 if(!row||row.action!=='sop_need_create'||row.actor_id!==u.id)return unknown;
 const saved=JSON.parse(row.result_json);if(!saved._submit_context||saved._submit_context!==await context(e,u))return unknown;
 const record=await q(e,'SELECT department FROM sop_records WHERE id=? AND kind=?',row.record_id,'need').first();if(!record||!allowed(u,'bulk'))return unknown;
 const data=JSON.parse(row.after_json);if(data.source_type!==b.source_type||(data.source_id||'')!==(b.source_id||''))return unknown;
 return {ok:true,state:'committed',result:result(saved)};
}
