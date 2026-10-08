import {cargoReferencesFile} from './cargo-allocation.js';
import {chainNeed,chainEvent,notifyMaterialChange,workChainEnabled} from './work-chain.js';
import {batchStateEnabled,ensureBatchMaterialState} from './batch-material-state.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const category='batch_work_material';
async function batchContext(env,id,user,write=false){
 const need=await chainNeed(env,id,user,write);
 if(need.data.source_type!=='inbound'||!need.data.source_id)throw Error('仅入库计划支持本批总作业明细');
 const plan=await q(env,'SELECT id,display_no,is_deleted,status FROM v2_inbound_plans WHERE id=?',need.data.source_id).first();
 if(!plan||plan.is_deleted)throw Error('入库计划不存在');
 if(write&&(user.scope==='field'||!['manager','service'].includes(user.role)||plan.status==='cancelled'))throw Error('请由办公室客服或管理员上传本批总作业明细');
 return {need,plan};
}
export async function readBatchMaterials(body,env,user){
 if(body.action!=='sop_batch_work_materials'||!workChainEnabled(env))return null;
 const {plan}=await batchContext(env,body.id,user);
 const managed=batchStateEnabled(env);if(managed)await ensureBatchMaterialState(env);
 const files=(await q(env,`SELECT a.*${managed?',COALESCE(s.revision,0) AS material_revision,COALESCE(s.removed,0) AS removed,s.actor_name AS changed_by,s.changed_at':''} FROM v2_attachments a ${managed?'LEFT JOIN ck_batch_material_state s ON s.attachment_id=a.id':''} WHERE a.related_doc_type='inbound_plan' AND a.related_doc_id=? AND a.attachment_category=? ORDER BY a.created_at DESC,a.id DESC`,plan.id,category).all()).results;
 const can_manage=managed&&user.scope!=='field'&&['manager','service'].includes(user.role)&&plan.status!=='cancelled';
 const format=f=>({...f,batch:true,material_kind:category});
 return {ok:true,plan,can_manage,items:files.filter(f=>!f.removed).map(format),removed_items:can_manage?files.filter(f=>f.removed).map(format):[]};
}
export async function changeBatchMaterial(body,env,user){
 if(!['sop_batch_work_material_remove','sop_batch_work_material_restore'].includes(body.action))return null;
 if(!batchStateEnabled(env))throw Error('本批资料移除功能未启用');
 const {plan}=await batchContext(env,body.id,user,true);await ensureBatchMaterialState(env);
 const attachment=String(body.attachment_id||''),request=String(body.client_req_id||''),expected=Number(body.material_revision),removed=body.action.endsWith('_remove')?1:0;
 if(!/^[a-z0-9-]{10,80}$/i.test(request)||!attachment||!Number.isSafeInteger(expected)||expected<0)throw Error('资料版本或请求编号无效，请刷新');
 const previous=async()=>{const event=await q(env,'SELECT * FROM ck_batch_material_events WHERE request_id=?',request).first();if(!event)return null;if(event.actor_id!==user.id||event.plan_id!==plan.id||event.need_id!==body.id||event.attachment_id!==attachment||event.removed!==removed||event.expected_revision!==expected)throw Error('资料操作请求编号冲突');return JSON.parse(event.result_json);};
 const prior=await previous();if(prior)return prior;
 const file=await q(env,`SELECT a.*,COALESCE(s.revision,0) AS material_revision,COALESCE(s.removed,0) AS removed FROM v2_attachments a LEFT JOIN ck_batch_material_state s ON s.attachment_id=a.id WHERE a.id=? AND a.related_doc_type='inbound_plan' AND a.related_doc_id=? AND a.attachment_category=?`,attachment,plan.id,category).first();
 if(!file)throw Error('资料不属于本入库计划');
 if(file.material_revision!==expected)throw Error('资料状态已更新，请刷新后重试');
 if(file.removed===removed)return {ok:true,id:attachment,removed:!!removed,material_revision:expected,already_applied:true};
 const t=new Date().toISOString(),result={ok:true,id:attachment,removed:!!removed,material_revision:expected+1};
 const rows=(await q(env,"SELECT * FROM sop_records WHERE kind='need' AND json_extract(state,'$.source_type')='inbound' AND json_extract(state,'$.source_id')=?",plan.id).all()).results;
 if(removed&&rows.some(row=>cargoReferencesFile(JSON.parse(row.state),attachment)))throw Error('资料仍被资料组或已确认成果引用，请保留历史文件版本');
 const statements=[q(env,'INSERT INTO ck_batch_material_events VALUES(?,?,?,?,?,?,?,?,?,?,?)',request,attachment,plan.id,body.id,expected,expected+1,removed,user.id,user.name,t,JSON.stringify(result)),
  q(env,`INSERT INTO ck_batch_material_state VALUES(?,?,?,?,?,?,?) ON CONFLICT(attachment_id) DO UPDATE SET removed=excluded.removed,revision=excluded.revision,actor_id=excluded.actor_id,actor_name=excluded.actor_name,changed_at=excluded.changed_at`,attachment,plan.id,removed,expected+1,user.id,user.name,t)];
 const notified=new Set();
 for(const row of rows){const d=JSON.parse(row.state);if(['closed','cancelled'].includes(d.status))continue;row.data=d;
  const data={...d,material_version:(d.material_version||0)+1,requirement_version:(d.requirement_version||1)+1,last_material_change:{action:removed?'batch_remove':'batch_restore',attachment_id:attachment,file_name:file.file_name,by:user.name,at:t}};
  statements.push(...chainEvent(env,row,data,user,request+':'+row.id,body.action,t));
  const links=(d.links||[]).filter(l=>{if(notified.has(l.outbound_id))return false;notified.add(l.outbound_id);return true;});
  statements.push(...notifyMaterialChange(env,{...row,data:{...d,links}},user,t,(removed?'本批总作业明细已移除：':'本批总作业明细已恢复：')+file.file_name));
 }
 try{await env.DB.batch(statements);}catch(error){const committed=await previous();if(committed)return committed;if(/batch_material_stale|sop_revision|revision_conflict/.test(error.message))throw Error('资料或关联作业已更新，请刷新后重试');throw error;}
 return result;
}
export async function uploadBatchMaterial(form,env){
 if(!workChainEnabled(env))throw Error('作业计划功能未启用');
 const user=env.SOP_REQUEST_USER,{plan}=await batchContext(env,form.get('need_id'),user,true);
 if(form.get('related_doc_type')!=='inbound_plan'||form.get('related_doc_id')!==plan.id)throw Error('上传目标与本批作业不一致');
 const req=String(form.get('client_req_id')||'');if(!/^[a-z0-9-]{10,80}$/i.test(req))throw Error('上传请求编号无效');
 const file=form.get('file'),ext=String(file?.name||'').split('.').pop().toLowerCase();
 const types={xlsx:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',xls:'application/vnd.ms-excel',csv:'text/csv',pdf:'application/pdf',jpg:'image/jpeg',jpeg:'image/jpeg',png:'image/png',webp:'image/webp'};
 if(!types[ext]||!file.size||file.size>20*1024*1024)throw Error('支持20MB以内的Excel、CSV、PDF或图片');
 const bytes=await file.arrayBuffer(),hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join('');
 const id='ATT-BATCH-'+req,key='v2/inbound_plan/'+plan.id+'/'+id+'-'+hash+'.'+ext;
 const previous=async()=>{const f=await q(env,'SELECT * FROM v2_attachments WHERE id=?',id).first();if(!f)return null;if(f.related_doc_id!==plan.id||f.file_key!==key||f.uploaded_by!==user.name)throw Error('上传请求编号冲突');return {ok:true,id,file_key:key};};
 const prior=await previous();if(prior)return prior;
 const rows=(await q(env,"SELECT * FROM sop_records WHERE kind='need' AND json_extract(state,'$.source_type')='inbound' AND json_extract(state,'$.source_id')=?",plan.id).all()).results;
 const t=new Date().toISOString(),name=String(file.name).slice(0,240),statements=[q(env,'INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',id,'inbound_plan',plan.id,category,name,key,file.size,types[ext],user.name,t)];
 for(const row of rows){const d=JSON.parse(row.state);if(['closed','cancelled'].includes(d.status))continue;row.data=d;const data={...d,material_version:(d.material_version||0)+1,requirement_version:(d.requirement_version||1)+1,last_material_change:{action:'batch_upload',attachment_id:id,file_name:name,by:user.name,at:t}};statements.push(...chainEvent(env,row,data,user,req+':'+row.id,'sop_batch_work_material_upload',t),...notifyMaterialChange(env,row,user,t,'本批总作业明细已更新：'+name));}
 await env.R2_BUCKET.put(key,bytes,{httpMetadata:{contentType:types[ext]}});
 try{await env.DB.batch(statements);}catch(error){const committed=await previous();if(committed)return committed;throw error;}
 return {ok:true,id,file_key:key};
}
