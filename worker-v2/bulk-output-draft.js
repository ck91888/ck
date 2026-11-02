import {executionDepartment} from './work-execution.js';
import {nativeOwner} from './sop-dispatch.js';
import {isActivePartner,isBulkShareRecord,partnerShareGuard,shareConflict} from './dispatch-share.js';
import {taskGroups,taskProcess} from './cargo-processes.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),text=v=>String(v??'').trim();
const open=['pending','assigned','working','paused','system_closed','awaiting_close','rework'];
const fields=new Set('quantity unit customer packed_sku_count packed_count packed_box_count used_carton_large_count used_carton_small_count repaired_box_count reboxed_count label_count operated_box_count total_operated_box_count pallet_count forklift_location_count shipping_box_count shipping_pallet_count used_forklift description location reason'.split(' '));
const conflict='草稿或任务已更新，请重新打开后核对；本次输入尚未保存 / 초안이 변경되었습니다';
async function read(e,id){const r=await q(e,'SELECT * FROM sop_records WHERE id=?',id).first();return r?{...r,data:JSON.parse(r.state)}:null;}
function permit(u,row){if(!['manager','dispatcher','reviewer'].includes(u.role)||u.role!=='manager'&&!(u.departments||[]).includes(executionDepartment(row)))throw Error('无此操作权限');}
async function externalId(u,code){const bytes=new TextEncoder().encode(u.id+'\0'+code);return 'BOD-'+Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),x=>x.toString(16).padStart(2,'0')).join('');}
async function target(e,b,u){
 let row,source=null,shared=false,group=null,processIndex=0;
 if(b.external_code){const code=text(b.external_code);if(!code||code.length>200)throw Error('外部作业单号无效');permit(u,{department:'bulk',data:{job_type:'bulk_op'}});
  const existing=await q(e,"SELECT id FROM v2_ops_jobs WHERE job_type='bulk_op' AND related_doc_id=? AND status NOT IN ('completed','cancelled')",code).first();if(existing)throw Error('此单已派工，请打开当前任务保存产出');
  const id=await externalId(u,code);row=await read(e,id)||{id,kind:'bulk_output_draft',revision:0,department:'bulk',data:{owner_id:u.id,external_code:code,status:'pending'}};
 }else row=await read(e,text(b.id));
 if(!row||!['task','dispatch','need','bulk_output_draft'].includes(row.kind))throw Error('大货作业单不存在');
 permit(u,row);
 if(row.kind==='bulk_output_draft'){if(row.data.owner_id!==u.id)throw Error('无此操作权限');if(await q(e,"SELECT id FROM v2_ops_jobs WHERE job_type='bulk_op' AND related_doc_id=? LIMIT 1",row.data.external_code).first())throw Error('此单已有任务，请打开任务操作；已结束任务不能修改草稿');}
 else if(row.kind==='need'){const grouped=b.phase==='group'&&row.data.cargo_groups;if(grouped?!['pending','assigned'].includes(row.data.status):row.data.status!=='pending'||row.data.task_id||row.data.bulk_batch_id)throw Error('此单已派工或结束，请打开当前任务');if(row.data.operation_kind==='direct_forward')throw Error('直接转发不登记操作产出');}
 else{
  const d=row.data,job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',row.id).first();
  if(d.job_type!=='bulk_op'||!job||job.job_type!=='bulk_op'||!open.includes(job.status)||!open.includes(d.status))throw Error('任务已结束、取消或已提交审核，不能修改草稿');
  if(row.kind==='dispatch'){const probe={job_id:row.id};if(!await nativeOwner(probe,e))throw Error('仅本任务派审员或获授权人员可操作');}
  else{const delegate=(d.delegates||[]).includes(u.id)&&!(d.owner_transfers||[]).some(x=>x.from_id===u.id);shared=u.role==='dispatcher'&&d.owner_id!==u.id&&!delegate&&isBulkShareRecord(row)&&await isActivePartner(e,u.id,d.owner_id);if(u.role!=='manager'&&d.owner_id!==u.id&&!delegate&&!shared)throw Error('仅本任务派审员或获授权人员可操作');}
  if(row.kind==='dispatch')shared=u.role!=='manager'&&d.owner_id!==u.id&&isBulkShareRecord(row)&&await isActivePartner(e,u.id,d.owner_id);
  if(d.need_id)source=await read(e,d.need_id);
  else if(row.kind==='dispatch'&&job.related_doc_type==='work_order')source=await read(e,await externalId({id:d.owner_transfers?.[0]?.from_id||d.owner_id},job.related_doc_id));
  if(source&&['cancelled','closed'].includes(source.data.status))throw Error('作业计划已关闭');
 }
 const need=row.kind==='need'?row:source,cargo=need?.data.cargo_groups;
 const phase=b.phase||'output';if(!['output','group','metrics'].includes(phase))throw Error('草稿类型无效');
 if(cargo){
  if(phase==='output')throw Error('此计划请按资料组填写产出草稿');if(row.kind==='need'&&phase==='metrics')throw Error('工耗请在派工后按实际任务预填写，避免跨任务重复');
  if(phase==='group'){
   group=(row.kind==='need'?cargo.groups:taskGroups(cargo,row)).find(g=>g.id===b.group_id);if(!group)throw Error('资料组不属于此任务');
   processIndex=Number(row.kind==='need'?b.process_index||0:row.data.cargo_process_index||0);const processes=group.processes||group.process_names||['作业'];if(!Number.isSafeInteger(processIndex)||processIndex<0||processIndex>=processes.length)throw Error('本组工序不存在');if(row.kind==='need'&&group.processes?.[processIndex]?.task_id)throw Error('此工序已派工，请从对应任务保存草稿');const process=row.kind==='need'?null:taskProcess(group,row);if(row.kind!=='need'&&group.processes&&!process)throw Error('此工序不属于当前任务');const output=process?.result||(!group.processes?group.outputs?.find(o=>o.id===group.current_output_id):null);
   if(output&&output.status!=='returned')throw Error('本组已提交成果，不能修改草稿');
  }
 }else if(phase!=='output')throw Error('此任务没有资料分组');
 const baseSlot=phase==='group'?'group:'+group.id:phase,sourceSlot=phase==='group'?baseSlot+':process:'+processIndex:baseSlot;
 const slot=row.kind==='need'&&phase==='group'?sourceSlot:baseSlot;
 const own=row.data.output_drafts?.[slot],inherited=source?.data.output_drafts?.[sourceSlot];
 const draft=own||inherited||null;
 return {row,source,shared,group,slot,draft,own,scope_version:group?.mapping_version||need?.data.requirement_version||1};
}
function values(input){if(!input||typeof input!=='object'||Array.isArray(input))throw Error('草稿内容无效');const out={};for(const[k,v]of Object.entries(input)){if(!fields.has(k))throw Error('草稿字段无效：'+k);if(k==='used_forklift'){if(typeof v!=='boolean')throw Error('叉车字段无效');out[k]=v;}else{if(!['string','number'].includes(typeof v)||String(v).length>4000)throw Error('草稿内容过长或无效');if(k.endsWith('_count')||k==='quantity'){if(v!==''&&(!Number.isSafeInteger(Number(v))||Number(v)<0))throw Error('草稿数量须为非负整数或留空');}if(k==='unit'&&v!==''&&!['箱','件','托','单','批'].includes(v))throw Error('成果单位无效');out[k]=String(v);}}return out;}
export async function bulkOutputDraft(b,e,u){
 if(!['sop_bulk_output_draft_read','sop_bulk_output_draft_save'].includes(b.action))return null;
 const x=await target(e,b,u),{row,source,slot,draft,own,shared,scope_version}=x;
 const result={ok:true,id:row.id,revision:row.revision,slot,version:own?.version||0,scope_version,draft};
 if(b.action==='sop_bulk_output_draft_read')return result;
 const request=text(b.client_req_id);if(!request)throw Error('缺少请求编号');
 const signature=JSON.stringify({id:row.id,slot,revision:b.revision,version:b.version,scope_version:b.scope_version,values:b.values});
 const previous=async()=>{const p=await q(e,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(!p)return null;const r=JSON.parse(p.result_json);if(p.actor_id!==u.id||p.action!==b.action||r.signature!==signature)throw Error('请求编号已使用，内容已变化');return r;};
 const prior=await previous();if(prior)return prior;
 if(Number(b.revision)!==row.revision||Number(b.version)!==(own?.version||0)||Number(b.scope_version)!==scope_version)throw Error(conflict);
 const t=new Date().toISOString(),saved={values:values(b.values),version:(own?.version||0)+1,scope_version,actor_id:u.id,by:u.name,at:t};
 const next={...row.data,output_drafts:{...row.data.output_drafts,[slot]:saved}},reply={...result,revision:row.revision+1,version:saved.version,draft:saved,signature};
 // Revision guards serialize crew/owner changes; the status guard also covers native completion which does not update the dispatch record.
 await q(e,`CREATE TRIGGER IF NOT EXISTS ck_bulk_output_draft_open BEFORE INSERT ON sop_events WHEN NEW.action='sop_bulk_output_draft_save' BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.id=NEW.record_id AND j.status NOT IN ('pending','working','paused','system_closed','awaiting_close')) THEN RAISE(ABORT,'output_draft_closed') END;
 SELECT CASE WHEN json_extract(NEW.after_json,'$.external_code') IS NOT NULL AND EXISTS(SELECT 1 FROM v2_ops_jobs j WHERE j.job_type='bulk_op' AND j.related_doc_id=json_extract(NEW.after_json,'$.external_code')) THEN RAISE(ABORT,'output_draft_closed') END;
 END`).run();
 const statements=[...await partnerShareGuard(e,row,u,shared),q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request,row.id,row.revision,b.action,u.id,u.name,row.state||'{}',JSON.stringify(next),JSON.stringify(reply),t),q(e,'INSERT INTO sop_records VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,revision=excluded.revision,updated_at=excluded.updated_at',row.id,row.kind,row.revision+1,row.department,JSON.stringify(next),t)];
 // The inherited plan must still have the version the operator actually saw.
 if(source?.kind==='need')statements.unshift(q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',request+'-source',source.id,source.revision,'sop_bulk_output_source_guard',u.id,u.name,source.state,source.state,'{}',t));
 try{await e.DB.batch(statements);}catch(error){const p=await previous();if(p)return p;if(/revision_conflict|output_draft_closed/.test(error.message)||shareConflict.test(error.message))throw Error(conflict);throw error;}
 return reply;
}
export function bulkOutputFinalGuard(e,b,row,u){
 if(b.output_draft_revision===undefined)return [];
 if(!row||!Number.isSafeInteger(Number(b.output_draft_revision)))throw Error(conflict);
 const t=new Date().toISOString();return [q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)',text(b.client_req_id)+'-output-guard',row.id,Number(b.output_draft_revision),'sop_bulk_output_final_guard',u.id||'',u.name||'',row.state,row.state,'{}',t)];
}
