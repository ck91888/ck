import {executionDepartment} from './work-execution.js';
// Work-plan print requests are audit events, independent of dispatch and completion.
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const parse=s=>{try{return JSON.parse(s||'{}');}catch{return{};}};
export async function workNeedDocumentKey(x){
 const document={id:x.id,display_no:x.display_no||'',source_type:x.source_type||'',source_display_no:x.source_display_no||'',customer:x.customer||'',supply_chain_no:x.supply_chain_no||'',requirement_version:x.requirement_version||1,title:x.title||'',deadline:x.deadline||'',scope_text:x.scope_text||'',instructions:x.instructions||''};
 // Only printed mapping content affects the key; execution/review progress does not.
 if(x.cargo_groups?.groups?.length){const c=x.cargo_groups,files=xs=>(xs||[]).map(f=>f.file_name||'');document.cargo_groups={total_range:c.total_range||'',box_count:c.box_count,groups:c.groups.map(g=>({name:g.name||'',range:g.range||'',box_count:g.box_count,identifier:g.identifier||'',instructions:g.instructions||'',documents:files([...(g.public_documents||c.public_documents||[]),...(g.documents||[])])}))};}
 return [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(document))))].map(v=>v.toString(16).padStart(2,'0')).join('');
}
export async function workNeedPrintFacts(env,items){
 const needs=items.filter(x=>x.kind==='need');if(!needs.length)return items;
 const events=(await q(env,"SELECT record_id,actor_name,created_at,after_json FROM sop_events WHERE action='sop_need_print_request' AND record_id IN (SELECT value FROM json_each(?)) ORDER BY created_at DESC,request_id DESC",JSON.stringify(needs.map(x=>x.id))).all()).results||[];
 for(const x of needs){const document_key=await workNeedDocumentKey(x),own=events.filter(p=>p.record_id===x.id),last=own[0],current=own.filter(p=>parse(p.after_json).document_key===document_key);x.print_status={state:!last?'unknown':current.length?'requested':'reprint',document_key,request_count:own.length,current_count:current.length,last_at:last?.created_at||'',last_by:last?.actor_name||'',last_requirement_version:last?parse(last.after_json).requirement_version:null};}
 return items;
}
export function workNeedPrintSummary(items){
 const active=items.filter(x=>x.status!=='cancelled'&&!x.source_cancelled),count=active.length,requested=active.filter(x=>x.print_status?.state==='requested').length,reprint=active.filter(x=>x.print_status?.state==='reprint').length;
 return {count,requested,reprint,unknown:count-requested-reprint,state:!count?'cancelled':requested===count?'requested':reprint?'reprint':requested?'partial':'unknown'};
}
export async function recordWorkNeedPrint(b,env,u,numbered){
 if(b.action!=='sop_need_print_request')return null;
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_WORK_CHAIN_ENABLED!=='true')throw Error('作业单打印记录尚未启用');
 if(!u||!['manager','service','dispatcher','reviewer','viewer'].includes(u.role))throw Error('无打印权限');
 const request=String(b.client_req_id||'').trim();if(!request||request.length>100)throw Error('打印请求编号无效');
 if(!Array.isArray(b.items)||!b.items.length||b.items.length>100)throw Error('请选择1至100份作业单');
 const expected=b.items.map(x=>({id:String(x.id||''),revision:Number(x.revision),document_key:String(x.document_key||'')})).sort((a,c)=>a.id.localeCompare(c.id));
 if(new Set(expected.map(x=>x.id)).size!==expected.length||expected.some(x=>!x.id||!Number.isSafeInteger(x.revision)||!/^[a-f0-9]{64}$/.test(x.document_key)))throw Error('打印作业单版本无效');
 const fingerprint=JSON.stringify(expected),prior=async()=>{const p=await q(env,'SELECT actor_id,action,result_json FROM sop_events WHERE request_id=?',request).first();if(!p)return null;const r=parse(p.result_json);if(p.actor_id!==u.id||p.action!==b.action||r.fingerprint!==fingerprint)throw Error('打印请求编号已用于其他内容');return r;};
 const old=await prior();if(old)return old;
 const rows=(await q(env,"SELECT * FROM sop_records WHERE kind='need' AND id IN (SELECT value FROM json_each(?))",JSON.stringify(expected.map(x=>x.id))).all()).results||[];
 if(rows.length!==expected.length)throw Error('作业单已变更，请刷新后重新打印');
 const documents=await numbered(rows.map(x=>({id:x.id,kind:x.kind,revision:x.revision,department:x.department,...parse(x.state)})));
 const t=new Date().toISOString(),result={ok:true,print_requested:true,ids:expected.map(x=>x.id),fingerprint,at:t};
 const statements=[];for(const [i,want]of expected.entries()){
  const row=rows.find(x=>x.id===want.id),x=documents.find(x=>x.id===want.id);
  if(u.role!=='manager'&&!(u.departments||[]).includes(executionDepartment(row)))throw Error('无此部门打印权限');
  if(x.status==='cancelled'||x.source_cancelled)throw Error('已取消的作业单不能打印');
  if(['inbound','outbound'].includes(x.source_type)){
   const table=x.source_type==='inbound'?'v2_inbound_plans':'v2_outbound_orders',source=await q(env,'SELECT status,is_deleted FROM '+table+' WHERE id=?',x.source_id).first();
   if(!source||source.is_deleted||source.status==='cancelled')throw Error('来源单据已取消、删除或缺失，不能记录打印');
  }
  if(row.revision!==want.revision||await workNeedDocumentKey(x)!==want.document_key)throw Error('作业单内容已更新，请重新打印最新版本');
  const snapshot={document_key:want.document_key,requirement_version:x.requirement_version||1,display_no:x.display_no,print_requested:true};
  statements.push(q(env,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',i?request+':'+i:request,row.id,row.revision,b.action,u.id,u.name,'{}',JSON.stringify(snapshot),JSON.stringify(result),t));
 }
 try{await env.DB.batch(statements);}catch(error){const committed=await prior();if(committed)return committed;if(String(error.message||error).includes('revision_conflict'))throw Error('作业单内容已更新，请刷新后重新打印');throw error;}
 return result;
}
