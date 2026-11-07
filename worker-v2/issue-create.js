// Issue drafts use the existing private R2 bucket, SOP revision guard and attachment table.
// A single D1 batch publishes the ticket and its files; unfinished drafts are never public files.
import {chainEvent} from './work-chain.js';
import {issueFileTypes as types,validateIssueFile} from './issue-file-validation.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const maxFiles=20,maxSize=20*1024*1024;
const stamp=()=>new Date().toISOString();
function owner(env){
 const u=env.SOP_REQUEST_USER;
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true'||u?.scope!=='office'||u.role!=='manager')throw Error('请由办公室登录后上传问题附件 / 사무실 로그인 후 첨부하세요');
 return {actor_id:u.id,session:env.SOP_OPERATION_SESSION||u.id};
}
function validId(id,prefix){if(typeof id!=='string'||!new RegExp('^'+prefix+'-[a-f0-9-]{36}$').test(id))throw Error('附件草稿编号无效 / 첨부 초안 번호가 올바르지 않습니다');return id;}
function owned(data,env){const a=owner(env);if(data.actor_id!==a.actor_id||data.session!==a.session)throw Error('附件不属于本次登录的草稿 / 현재 로그인 세션의 첨부가 아닙니다');}
async function draft(env,id){
 validId(id,'ISD');const a=owner(env),t=stamp();
 await q(env,'INSERT OR IGNORE INTO sop_records VALUES(?,?,?,?,?,?)',id,'issue_draft',0,'bulk',JSON.stringify({...a,status:'draft',files:[]}),t).run();
 const row=await q(env,'SELECT * FROM sop_records WHERE id=?',id).first();
 if(row.kind!=='issue_draft')throw Error('草稿编号冲突');
 row.data=JSON.parse(row.state);owned(row.data,env);return row;
}
const open=row=>{if(row.data.status!=='draft')throw Error('草稿已提交或取消，请新建问题 / 제출 또는 취소된 초안입니다');};
async function fileRecord(env,id){const r=await q(env,"SELECT state FROM sop_records WHERE id=? AND kind='issue_file'",id).first();return r&&JSON.parse(r.state);}
function sameFile(f,id,hash,name,env){owned(f,env);if(f.draft!==id||f.hash!==hash||f.file_name!==name)throw Error('附件请求已变化，请重新选择文件 / 첨부 요청이 변경되었습니다');}
export async function uploadIssueDraft(form,env){
 const a=owner(env),id=validId(form.get('related_doc_id'),'ISD'),fileId=validId(form.get('client_req_id'),'ISF'),file=form.get('file');
 const name=String(file?.name||''),ext=name.split('.').pop().toLowerCase();
 if(!types[ext]||!file?.size||typeof file.arrayBuffer!=='function'||file.size>maxSize||name.length>240||/[\x00-\x1f]/.test(name))throw Error('支持20MB以内的PDF、Excel、CSV、JPG、PNG、WebP；文件名最多240字 / 파일당 20MB, 파일명 최대 240자');
 const bytes=await file.arrayBuffer(),hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),n=>n.toString(16).padStart(2,'0')).join('');
 validateIssueFile(bytes,ext,file.type);
 let row=await draft(env,id);open(row);
 const prior=await fileRecord(env,fileId);
 if(prior){sameFile(prior,id,hash,name,env);if(!row.data.files.includes(fileId))throw Error('附件已移除，请重新选择');return {ok:true,id:fileId};}
 if(row.data.files.length>=maxFiles)throw Error('每个问题最多20个附件 / 문제당 최대 20개');
 const t=stamp(),f={...a,id:fileId,draft:id,hash,file_name:name,file_key:'issue-drafts/'+fileId+'/'+hash+'.'+ext,file_size:file.size,content_type:types[ext],uploaded_by:env.SOP_REQUEST_USER.name,created_at:t};
 await env.R2_BUCKET.put(f.file_key,bytes,{httpMetadata:{contentType:f.content_type}});
 for(let attempt=0;attempt<3;attempt++){
  row=await draft(env,id);
  if(row.data.status==='cancelled'){await env.R2_BUCKET.delete?.(f.file_key);open(row);}
  open(row);
  const existing=await fileRecord(env,fileId);if(existing){sameFile(existing,id,hash,name,env);return {ok:true,id:fileId};}
  if(row.data.files.length>=maxFiles)throw Error('每个问题最多20个附件 / 문제당 최대 20개');
  try{
   await env.DB.batch([...chainEvent(env,row,{...row.data,files:[...row.data.files,fileId]},env.SOP_REQUEST_USER,fileId,'v2_issue_draft_upload',t),q(env,'INSERT INTO sop_records VALUES(?,?,?,?,?,?)',fileId,'issue_file',1,'bulk',JSON.stringify(f),t)]);
   return {ok:true,id:fileId};
  }catch(e){if(attempt===2||!/revision_conflict|UNIQUE constraint/.test(e.message))throw e;}
 }
}
export async function removeIssueDraftFile(body,env){
 const row=await draft(env,body.draft_id);open(row);
 const id=validId(body.file_id,'ISF'),f=await fileRecord(env,id);
 if(!f)return {ok:true};
 owned(f,env);if(f.draft!==row.id)throw Error('附件不属于此问题 / 이 문제의 첨부가 아닙니다');
 if(row.data.files.includes(id))await env.DB.batch(chainEvent(env,row,{...row.data,files:row.data.files.filter(n=>n!==id)},env.SOP_REQUEST_USER,'REMOVE-'+id,'v2_issue_draft_remove',stamp()));
 await env.R2_BUCKET.delete?.(f.file_key);return {ok:true};
}
export async function cancelIssueDraft(body,env){
 const row=await draft(env,body.draft_id);
 if(row.data.status==='created')return {ok:true,id:row.data.issue_id,created:true};
 if(row.data.status==='draft')await env.DB.batch(chainEvent(env,row,{...row.data,status:'cancelled'},env.SOP_REQUEST_USER,'CANCEL-'+row.id,'v2_issue_draft_cancel',stamp()));
 for(const id of row.data.files){const f=await fileRecord(env,id);if(f)await env.R2_BUCKET.delete?.(f.file_key);}
 return {ok:true};
}
export async function createIssueWithAttachments(body,env){
 const input=body.issue_attachments;
 if(!input||!Array.isArray(input.attachment_ids)||input.attachment_ids.length>maxFiles)throw Error('附件清单无效 / 첨부 목록이 올바르지 않습니다');
 const ids=[...new Set(input.attachment_ids)].sort();for(const id of ids)validId(id,'ISF');
 const data={biz_class:String(body.biz_class||''),customer:String(body.customer||'').trim(),related_doc_no:String(body.related_doc_no||'').trim(),issue_description:String(body.issue_description||'').trim(),attachment_ids:ids};
 if(!data.customer||!data.issue_description||!['bulk','direct_ship','return','import'].includes(data.biz_class))throw Error('请填写客户、业务分类和问题描述 / 고객·업무·문제 설명을 입력하세요');
 const signature=JSON.stringify(data),row=await draft(env,input.draft_id);
 const replay=r=>{if(r.data.status!=='created')return null;if(r.data.signature!==signature)throw Error('此草稿已提交，请先核对已创建问题 / 이미 제출된 문제를 확인하세요');return {ok:true,id:r.data.issue_id};};
 const previous=replay(row);if(previous)return previous;open(row);
 const files=[];
 for(const id of ids){const f=await fileRecord(env,id);if(!f||f.draft!==row.id||!row.data.files.includes(id))throw Error('附件未上传完成或不属于此问题 / 첨부 업로드 또는 문제 연결을 확인하세요');owned(f,env);files.push(f);}
 const id='ISS-'+crypto.randomUUID(),t=stamp();
 const statements=chainEvent(env,row,{...row.data,status:'created',signature,issue_id:id},env.SOP_REQUEST_USER,'CREATE-'+row.id,'v2_issue_create',t,{id});
 statements.push(q(env,"INSERT INTO v2_issue_tickets(id,biz_class,customer,related_doc_no,issue_description,priority,submitted_by,status,created_at,updated_at) VALUES(?,?,?,?,?,'normal',?,'pending',?,?)",id,data.biz_class,data.customer,data.related_doc_no,data.issue_description,env.SOP_REQUEST_USER.name,t,t));
 for(const f of files)statements.push(q(env,'INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',f.id,'issue_ticket',id,'issue_original',f.file_name,f.file_key,f.file_size,f.content_type,f.uploaded_by,f.created_at));
 try{await env.DB.batch(statements);}catch(e){const result=replay(await draft(env,row.id));if(result)return result;throw e;}
 // Unselected files are disposable. Cleanup failure cannot change an already committed ticket.
 for(const unused of row.data.files.filter(n=>!ids.includes(n))){try{const f=await fileRecord(env,unused);if(f)await env.R2_BUCKET.delete?.(f.file_key);}catch{}}
 return {ok:true,id};
}
