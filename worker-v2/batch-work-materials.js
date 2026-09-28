import {chainNeed,chainEvent,notifyMaterialChange,workChainEnabled} from './work-chain.js';
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
 const items=(await q(env,"SELECT * FROM v2_attachments WHERE related_doc_type='inbound_plan' AND related_doc_id=? AND attachment_category=? ORDER BY created_at DESC,id DESC",plan.id,category).all()).results;
 return {ok:true,plan,items:items.map(f=>({...f,batch:true,material_kind:category}))};
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
