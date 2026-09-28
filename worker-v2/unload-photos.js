import {nativeOwner} from './sop-dispatch.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const fail=message=>{throw Error(message);};
const category='unload_photo';
export function inboundAttachmentRead(env,id){
 return q(env,`SELECT a.* FROM v2_attachments a WHERE (a.related_doc_type='inbound_plan' AND a.related_doc_id=?) OR
  (a.attachment_category='unload_photo' AND a.related_doc_type='field_feedback' AND (a.related_doc_id IN (SELECT id FROM v2_field_feedbacks WHERE inbound_plan_id=?) OR a.related_doc_id IN (SELECT source_feedback_id FROM v2_inbound_plans WHERE id=?))) ORDER BY a.created_at DESC,a.id`,id,id,id);
}
export async function unloadPhotos(env,type,id){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true')return [];
 if(type==='inbound_plan')return ((await inboundAttachmentRead(env,id).all()).results||[]).filter(p=>p.attachment_category===category);
 if(type!=='field_feedback')return [];
 return (await q(env,'SELECT * FROM v2_attachments WHERE related_doc_type=? AND related_doc_id=? AND attachment_category=? ORDER BY created_at,id',type,id,category).all()).results||[];
}
export async function uploadUnloadPhoto(form,env){
 const user=env.SOP_REQUEST_USER,jobId=String(form.get('job_id')||''),type=String(form.get('related_doc_type')||''),target=String(form.get('related_doc_id')||''),request=String(form.get('client_req_id')||''),file=form.get('file');
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true'||!user||!await nativeOwner({job_id:jobId},env))fail('请由本任务派审员上传到货照片 / 담당자가 도착 사진을 등록하세요');
 const job=await q(env,"SELECT * FROM v2_ops_jobs WHERE id=? AND job_type='unload'",jobId).first();
 if(!job||!['pending','working','awaiting_close','completed'].includes(job.status))fail('卸货任务不存在或已取消 / 하차 작업을 확인하세요');
 let belongs=false;
 if(type==='inbound_plan')belongs=!!await q(env,`SELECT p.id FROM v2_inbound_plans p WHERE p.id=? AND COALESCE(p.is_deleted,0)=0 AND p.status!='cancelled' AND (
  EXISTS(SELECT 1 FROM ck_unload_plan_links WHERE job_id=? AND plan_id=p.id) OR (?='inbound_plan' AND p.id=?) OR EXISTS(SELECT 1 FROM v2_field_feedbacks WHERE id=? AND inbound_plan_id=p.id))`,target,jobId,job.related_doc_type,job.related_doc_id,job.related_doc_type==='field_feedback'?job.related_doc_id:'').first();
 if(type==='field_feedback')belongs=!!await q(env,"SELECT id FROM v2_field_feedbacks WHERE id=? AND COALESCE(is_deleted,0)=0 AND (related_doc_id=? OR (?='field_feedback' AND id=?))",target,jobId,job.related_doc_type,job.related_doc_id).first();
 if(!belongs)fail('照片不属于本车入库计划 / 이 차량의 입고계획이 아닙니다');
 if(!/^[a-f0-9-]{36}$/i.test(request))fail('缺少照片上传编号，请重新选择照片');
 if(!file||!['image/jpeg','image/png','image/webp'].includes(file.type)||!file.size||file.size>10*1024*1024)fail('请选择10MB以内的JPG、PNG或WebP照片 / 10MB 이하 사진을 선택하세요');
 const bytes=await file.arrayBuffer(),head=new Uint8Array(bytes,0,Math.min(bytes.byteLength,12));
 const valid=file.type==='image/jpeg'?head[0]===255&&head[1]===216&&head[2]===255:file.type==='image/png'?[137,80,78,71,13,10,26,10].every((v,i)=>head[i]===v):String.fromCharCode(...head.slice(0,4))==='RIFF'&&String.fromCharCode(...head.slice(8,12))==='WEBP';
 if(!valid)fail('照片格式无效，请重新选择 / 사진 형식 오류');
 const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
 const id='ATT-UL-'+request.toLowerCase(),ext={'image/jpeg':'jpg','image/png':'png','image/webp':'webp'}[file.type];
 const key=`v2/unload-photos/${jobId}/${id}-${digest}.${ext}`;
 const prior=await q(env,'SELECT * FROM v2_attachments WHERE id=?',id).first();
 if(prior){if(prior.related_doc_type!==type||prior.related_doc_id!==target||prior.attachment_category!==category||prior.file_key!==key)fail('上传编号已用于其他照片，请重新选择');return {ok:true,id,file_key:key,attachment:prior,already_uploaded:true};}
 const count=await q(env,'SELECT COUNT(*) n FROM v2_attachments WHERE related_doc_type=? AND related_doc_id=? AND attachment_category=?',type,target,category).first();
 if(count.n>=12)fail('每份到货记录最多12张照片 / 최대 12장');
 const attachment={id,related_doc_type:type,related_doc_id:target,attachment_category:category,file_name:String(file.name||'arrival.'+ext).slice(0,240),file_key:key,file_size:file.size,content_type:file.type,uploaded_by:user.name||user.id,created_at:new Date().toISOString()};
 await env.R2_BUCKET.put(key,bytes,{httpMetadata:{contentType:file.type}});
 try{
  await q(env,`INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at)
   SELECT ?,?,?,?,?,?,?,?,?,? WHERE (SELECT COUNT(*) FROM v2_attachments WHERE related_doc_type=? AND related_doc_id=? AND attachment_category=?)<12 ON CONFLICT(id) DO NOTHING`,...Object.values(attachment),type,target,category).run();
 }catch(error){
  // A retry must never delete a successfully committed concurrent upload.
  if(!await q(env,'SELECT id FROM v2_attachments WHERE id=?',id).first())await env.R2_BUCKET.delete(key);
  throw error;
 }
 const saved=await q(env,'SELECT * FROM v2_attachments WHERE id=?',id).first();
 if(!saved||saved.file_key!==key||saved.related_doc_type!==type||saved.related_doc_id!==target){if(!saved||saved.file_key!==key)await env.R2_BUCKET.delete(key);fail('照片数量已满或上传编号冲突，请重新打开确认');}
 return {ok:true,id,file_key:key,attachment:saved};
}
