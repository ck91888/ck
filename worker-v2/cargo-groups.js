import {processNames} from './cargo-processes.js';
import {cargoTask} from './cargo-execution.js';
import {chainNeed,chainEvent,workMaterials,workChainEnabled} from './work-chain.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const text=v=>String(v??'').trim();
const positive=v=>{const n=Number(v);if(!Number.isSafeInteger(n)||n<1)throw Error('请明确填写正整数箱数 / 박스 수량을 입력하세요');return n;};
// Expand only bounded, explicit numeric ranges. Never infer grouping from notes or filenames.
export function expandMarks(value){
 const input=text(value);if(!input||input.length>20000)throw Error('请填写有效箱唛范围 / 박스 표시 범위를 입력하세요');
 const result=[],seen=new Set();let previousPrefix='';
 for(const token of input.split(/[，,;；\n]+/).map(text).filter(Boolean)){
  const range=token.match(/^([^\s~～–—]+?)\s*[~～–—-]\s*([^\s~～–—]+)$/);
  let marks;
  if(range){
   const a=range[1].match(/^(.*?)(\d+)$/),b=range[2].match(/^(.*?)(\d+)$/);
   if(!a||!b||b[1]&&b[1]!==a[1]||a[2].length!==b[2].length)throw Error('范围前缀和数字位数须一致：'+token);
   const start=Number(a[2]),end=Number(b[2]);if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||end<start||end-start>50000)throw Error('范围无效或过大：'+token);
   const prefix=a[1]||previousPrefix;previousPrefix=prefix;
   marks=Array.from({length:end-start+1},(_,i)=>prefix+String(start+i).padStart(a[2].length,'0'));
  }else{if(!/^[\p{L}\p{N}_.:-]+$/u.test(token))throw Error('箱唛格式无效：'+token);const m=token.match(/^(.*?)(\d+)$/);const mark=m&&!m[1]&&previousPrefix?previousPrefix+token:token;if(m?.[1])previousPrefix=m[1];marks=[mark];}
  for(const mark of marks){if(seen.has(mark))throw Error('同组箱唛重复：'+mark);seen.add(mark);result.push(mark);if(result.length>50000)throw Error('每次最多 50000 个箱唛');}
 }
 if(!result.length)throw Error('箱唛范围不能为空');return result;
}
export function normalizeCargoGroups(input){
 if(!Array.isArray(input.groups)||!input.groups.length||input.groups.length>200)throw Error('请填写 1–200 个资料组');
 const all=new Map(),ids=new Set();let count=0,expanded=0;
 const groups=input.groups.map((g,i)=>{
  const id=text(g.id)||'CG-'+crypto.randomUUID();if(ids.has(id))throw Error('资料组编号重复');ids.add(id);
  const marks=expandMarks(g.range);expanded+=marks.length;if(expanded>50000)throw Error('每个计划最多50000个箱唛定位');const identifier=text(g.identifier),one=g.one_mark_one_box===true,box_count=positive(g.box_count);
  if(one&&box_count!==marks.length)throw Error('已确认一唛一箱，箱数须与范围数量一致：'+(i+1));
  if(identifier.length>120)throw Error('区分标记过长');
  for(const mark of marks){const previous=all.get(mark)||[];if(previous.some(p=>one||p.one||!identifier||!p.identifier||p.identifier===identifier))throw Error('箱唛范围交叉，请拆分或明确不同实物标记：'+mark);previous.push({identifier,one});all.set(mark,previous);}
  const instructions=text(g.instructions),name=text(g.name)||'组 '+(i+1);if(!instructions||instructions.length>4000||name.length>120)throw Error('请填写组名与处理要求（要求最多4000字）');
  const attachment_ids=[...new Set((g.attachment_ids||[]).map(text))];if(attachment_ids.length>200)throw Error('每组最多200份资料');
  count+=box_count;return {id,name,process_names:processNames(g.process_names),range:text(g.range),marks,identifier,one_mark_one_box:one,box_count,instructions,attachment_ids,no_documents_reason:text(g.no_documents_reason).slice(0,400)};
 });
 if(!Number.isSafeInteger(count))throw Error('合计箱数过大');
 const total_range=text(input.total_range),total=total_range?expandMarks(total_range):null;
 if(total){const allowed=new Set(total);for(const mark of all.keys())if(!allowed.has(mark))throw Error('分组范围超出总范围：'+mark);}
 const public_attachment_ids=[...new Set((input.public_attachment_ids||[]).map(text))];if(public_attachment_ids.length>200)throw Error('公共资料最多200份');
 return {schema:1,total_range,groups,public_attachment_ids,box_count:count,coverage:total?{checked:true,unassigned:total.filter(m=>!all.has(m))}:{checked:false,unassigned:[]}};
}
const canEdit=u=>u.scope!=='field'&&['manager','service'].includes(u.role);
export async function cargoGroups(b,env,u){
 if(!['sop_cargo_groups','sop_cargo_groups_preview','sop_cargo_groups_save'].includes(b.action))return null;
 if(!workChainEnabled(env))throw Error('资料分组未启用');
 const row=await chainNeed(env,b.id,u),current=row.data.cargo_groups||null;
 if(b.action==='sop_cargo_groups'){const access=await cargoTask(env,row,u,b.task_id);return {ok:true,id:row.id,revision:row.revision,cargo_groups:current,can_edit:canEdit(u),can_complete:access.can_complete,can_review:access.can_review,task_status:access.task?.data.status||'',task_id:access.task?.id||'',task:access.task?{id:access.task.id,...access.task.data}:null,files:await workMaterials(env,[row])};}
 if(row.data.operation_kind==='direct_forward')throw Error('直接转发沿用原出库流程；资料分组用于实际作业计划');
 if(!canEdit(u))throw Error('资料范围请由办公室客服维护 / 사무실 담당자만 수정할 수 있습니다');
 const request=text(b.client_req_id),fingerprint=JSON.stringify({id:row.id,revision:b.revision,groups:b.groups,total_range:b.total_range||'',public_attachment_ids:b.public_attachment_ids||[],reason:b.reason||''});
 if(b.action==='sop_cargo_groups_save'){
  if(!request)throw Error('缺少请求编号');
  const prior=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();
  if(prior){const result=JSON.parse(prior.result_json);if(prior.actor_id!==u.id||prior.record_id!==row.id||prior.action!==b.action||result.fingerprint!==fingerprint)throw Error('请求编号冲突');return result;}
 }
 if(Number(b.revision)!==row.revision)throw Error('作业计划已更新，请刷新后核对 / 작업 계획이 변경되었습니다');
 if(['closed','cancelled'].includes(row.data.status))throw Error('作业计划已关闭');
 const normalized=normalizeCargoGroups(b);
 if(row.data.planned_unit==='箱'&&normalized.box_count!==Number(row.data.planned_quantity))throw Error('分组箱数合计须等于计划箱数：'+row.data.planned_quantity);
 const previousIds=new Set(current?.groups.map(g=>g.id)||[]);for(const g of b.groups)if(g.id&&!previousIds.has(g.id))throw Error('资料组不属于此计划');
 const files=await workMaterials(env,[row]),byId=new Map(files.map(f=>[f.id,f]));
 const snapshot=id=>{const f=byId.get(id);if(!f)throw Error('资料不存在、已撤下或不属于此计划');return {id:f.id,file_key:f.file_key,file_name:f.file_name,created_at:f.created_at,uploaded_by:f.uploaded_by};};
 normalized.public_documents=normalized.public_attachment_ids.map(snapshot);
 for(const g of normalized.groups){g.documents=g.attachment_ids.map(snapshot);if(!g.documents.length&&!normalized.public_documents.length&&!g.no_documents_reason)throw Error('请选择组资料或明确无需资料的原因：'+g.name);}
 if(b.action==='sop_cargo_groups_preview')return {ok:true,cargo_groups:normalized};
 const t=new Date().toISOString(),data=structuredClone(row.data),extra=[];
 const started=!!row.data.task_id;
 if(!current&&(started||row.data.result||(row.data.links||[]).length))throw Error('既有已派工或整单成果不能自动迁移分组；请沿用原版本变更流程');
 if(started&&(normalized.groups.length!==current.groups.length||normalized.groups.some(g=>!previousIds.has(g.id))))throw Error('开工后增减资料组须使用可定位拆组流程');
 const content=g=>JSON.stringify({process_names:processNames(g.process_names),name:g.name,range:g.range,marks:g.marks,identifier:g.identifier||'',one:g.one_mark_one_box===true,boxes:g.box_count,instructions:g.instructions,files:g.attachment_ids,none:g.no_documents_reason||''});
 const publicChanged=current&&JSON.stringify(normalized.public_attachment_ids)!==JSON.stringify(current.public_attachment_ids);
 if(publicChanged&&current.groups.some(g=>g.outputs?.some(o=>o.status==='approved'||o.status==='awaiting_review')))throw Error('已有提交或审核成果，公共版本保留；剩余组请单独选择新版文件');
 let changed=false;
 normalized.groups=normalized.groups.map(g=>{const old=current?.groups.find(x=>x.id===g.id);const different=!old||content(old)!==content(g)||publicChanged;
  if(!different)return old;
  if(old?.outputs?.some(o=>o.status==='approved'||o.status==='awaiting_review'))throw Error('已提交、审核或出库组的范围和资料已冻结；请先处理成果版本：'+g.name);
  if(started&&old&&JSON.stringify(processNames(old.process_names))!==JSON.stringify(g.process_names))throw Error('开工后不能改变必需工序，请按原工序核对');
  changed=true;return {...old,...g,mapping_version:(old?.mapping_version||0)+1,ack_version:started?0:undefined,public_documents:normalized.public_documents,changed_by:u.name,changed_at:t};
 });
 if(started&&changed){if(!text(b.reason))throw Error('开工后变更请填写原因，并由现场确认');const {task}=await cargoTask(env,row,u);if(!task)throw Error('关联任务不存在，请核对');task.data.cargo_pending_change_at=t;extra.push(...chainEvent(env,task,task.data,u,request+'-task',b.action,t));normalized.change_reason=text(b.reason).slice(0,4000);}
 if(current?.archived_groups)normalized.archived_groups=current.archived_groups;
 normalized.version=(current?.version||0)+1;normalized.by=u.name;normalized.actor_id=u.id;normalized.at=t;data.cargo_groups=normalized;
 const result={cargo_version:normalized.version,fingerprint};
 try{await env.DB.batch([...chainEvent(env,row,data,u,request,b.action,t,result),...extra]);}catch(error){const prior=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(prior&&prior.actor_id===u.id&&prior.action===b.action&&JSON.parse(prior.result_json).fingerprint===fingerprint)return JSON.parse(prior.result_json);throw error;}
 return {ok:true,id:row.id,revision:row.revision+1,...result};
}
