// Only the two 003 photo entrances opt in. Legacy uploads and records are unchanged.
export const PHOTO003_PROTOCOL='003-photo-v1';
const admitted=new WeakSet(),ready=new WeakMap();
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const fail=(message,status=409)=>{throw Object.assign(Error(message),{status});};
export function normalize003PhotoForm(form){
 if(!form.has('photo_protocol'))return false;
 if(form.get('photo_protocol')!==PHOTO003_PROTOCOL||form.get('action')!=='v2_attachment_upload'||form.get('related_doc_type')!=='asset'||form.get('attachment_category')!=='arrival_photo')fail('照片服务版本不兼容，请保留文件并刷新后重试',400);
 const type=form.get('photo_target_type');
 if(!['asset','material_shipment'].includes(type)||!/^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$/.test(String(form.get('client_req_id')||''))||!/^[a-f0-9-]{36}$/i.test(form.get('client_req_id')))fail('照片上传编号或目标无效',400);
 // On an old server asset + arrival_photo is rejected before any object write.
 // Normalize before the existing authorization; never introduce an alternate role.
 form.set('related_doc_type',type);form.set('attachment_category',type==='asset'?'asset_photo':'arrival_photo');admitted.add(form);return true;
}
export const is003PhotoForm=form=>admitted.has(form);
async function ensure(env){
 let pending=ready.get(env.DB);if(!pending){pending=q(env,`CREATE TABLE IF NOT EXISTS ck_003_photo_upload_receipts_v1 (
 request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, attachment_id TEXT NOT NULL UNIQUE,
 file_key TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','complete')),
 created_at TEXT NOT NULL, completed_at TEXT
 )`).run();ready.set(env.DB,pending);pending.catch(()=>ready.delete(env.DB));}await pending;
}
async function hash(bytes){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');}
// Bounded memory even for preserved asset originals. This is a versioned chunk
// manifest fingerprint, not a claim that it is the SHA256 of the raw whole file.
export async function photo003Fingerprint(file,metadata){
 const chunks=[];for(let p=0;p<file.size;p+=1048576)chunks.push(await hash(await file.slice(p,p+1048576).arrayBuffer()));
 return hash(new TextEncoder().encode(JSON.stringify(['003-sha256-chunks-1MiB-v1',metadata,file.size,chunks])));
}
function validObject(object,fingerprint,file){return object&&object.size===file.size&&object.customMetadata?.photo003===fingerprint&&object.httpMetadata?.contentType===file.type;}
export async function upload003Photo(form,env,{type,target,category,uploadedBy,operator}){
 if(!admitted.has(form))fail('Invalid photo protocol',400);
 const file=form.get('file'),request=String(form.get('client_req_id')).toLowerCase();
 if(!file||typeof file.stream!=='function'||!file.size)fail('文件为空或无效 / 빈 파일입니다',400);
 if(type==='material_shipment'&&(!String(file.type||'').startsWith('image/')||file.size>15*1048576))fail(file.size>15*1048576?'file_too_large':'image_required',400);
 const principal=[env.SOP_REQUEST_USER?.scope||'',env.SOP_REQUEST_USER?.id||'',env.SOP_OPERATION_CONTEXT?.context_id||'',operator.operator_id,operator.operator_name,uploadedBy];
 const fingerprint=await photo003Fingerprint(file,[type,target,category,file.name,file.type,principal]);
 await ensure(env);
 const id='ATT-003P-'+request,key=`v2/${type}/${target}/${request}-${file.name}`,time=new Date().toISOString();
 await q(env,"INSERT OR IGNORE INTO ck_003_photo_upload_receipts_v1(request_id,fingerprint,attachment_id,file_key,state,created_at) VALUES(?,?,?,?,'pending',?)",request,fingerprint,id,key,time).run();
 const receipt=await q(env,'SELECT * FROM ck_003_photo_upload_receipts_v1 WHERE request_id=?',request).first();
 if(!receipt||receipt.fingerprint!==fingerprint||receipt.attachment_id!==id||receipt.file_key!==key)fail('上传编号已用于不同文件、目标或操作身份，请核对原记录');
 const params=[id,type,target,category,file.name,key,file.size,file.type,uploadedBy,receipt.created_at];
 const matching="id=? AND related_doc_type=? AND related_doc_id=? AND attachment_category=? AND file_name=? AND file_key=? AND file_size=? AND content_type=? AND uploaded_by=? AND created_at=?";
 const saved=()=>q(env,'SELECT * FROM v2_attachments WHERE '+matching,...params).first();
 const response=attachment=>({ok:true,id,file_key:key,photo_protocol:PHOTO003_PROTOCOL,client_req_id:request,attachment});
 if(receipt.state==='complete'){
  const attachment=await saved(),object=await env.R2_BUCKET.head(key);if(!attachment||!validObject(object,fingerprint,file))fail('已保存照片状态不完整，请保留文件并联系主管');return response(attachment);
 }
 const targetSQL=type==='asset'?"SELECT 1 FROM v2_003_assets WHERE id=?":"SELECT 1 FROM v2_003_purchase_shipments WHERE id=? AND status='pending'";
 if(!await q(env,targetSQL,target).first())fail(type==='asset'?'asset_not_found':'shipment_not_pending');
 // Conditional creation protects the first bytes. A pending receipt owns exactly
// one deterministic key; faults leave it resumable, never delete an old object.
 let object=await env.R2_BUCKET.head(key);
 if(!object){await env.R2_BUCKET.put(key,file.stream(),{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:file.type},customMetadata:{photo003:fingerprint}});object=await env.R2_BUCKET.head(key);}
 if(!validObject(object,fingerprint,file))fail('照片对象状态冲突，请保留文件并联系主管');
 try{
  await env.DB.batch([
   q(env,`INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at)
    SELECT ?,?,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM ck_003_photo_upload_receipts_v1 WHERE request_id=? AND fingerprint=? AND (state='complete' OR EXISTS(${targetSQL}))) ON CONFLICT(id) DO NOTHING`,...params,request,fingerprint,target),
   q(env,`UPDATE ck_003_photo_upload_receipts_v1 SET state=CASE WHEN EXISTS(SELECT 1 FROM v2_attachments WHERE ${matching}) THEN 'complete' ELSE 'conflict' END,completed_at=COALESCE(completed_at,?) WHERE request_id=? AND fingerprint=?`,...params,time,request,fingerprint)
  ]);
 }catch(error){
  const current=await q(env,'SELECT state FROM ck_003_photo_upload_receipts_v1 WHERE request_id=? AND fingerprint=?',request,fingerprint).first(),attachment=await saved();
  if(current?.state==='complete'&&attachment)return response(attachment);
  throw Object.assign(Error('照片保存未确认；保留此文件，以相同编号重试 / 같은 파일로 다시 시도하세요'),{status:409,cause:error});
 }
 const complete=await q(env,"SELECT state FROM ck_003_photo_upload_receipts_v1 WHERE request_id=? AND fingerprint=?",request,fingerprint).first(),attachment=await saved();
 if(complete?.state!=='complete'||!attachment)fail('照片保存未确认，请保留原文件及上传编号');return response(attachment);
}
