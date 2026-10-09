import {normalizeCargoGroups} from './cargo-groups.js';
import {chainEvent,workChainEnabled} from './work-chain.js';
const q=(env,sql,...v)=>env.DB.prepare(sql).bind(...v);
function permit(env,u){if(!workChainEnabled(env)||!u||u.scope==='field'||!['manager','service'].includes(u.role))throw Error('请由办公室客服维护新建作业资料');}
export async function uploadCargoDraft(form,env){
 const u=env.SOP_REQUEST_USER;permit(env,u);const file=form.get('file'),id=String(form.get('client_req_id')||''),draft=String(form.get('related_doc_id')||'');
 if(!/^CGF-[a-f0-9-]{36}$/.test(id)||!/^CGD-[a-f0-9-]{36}$/.test(draft))throw Error('无效资料草稿编号');
 if(!file||!file.size||file.size>20*1024*1024||! /\.(pdf|xlsx?|csv|jpe?g|png|webp)$/i.test(file.name))throw Error('请选择20MB以内的PDF、Excel、CSV或图片');
 const bytes=await file.arrayBuffer(),hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),n=>n.toString(16).padStart(2,'0')).join('');
 const old=await q(env,"SELECT * FROM sop_records WHERE id=?",id).first();if(old){const d=JSON.parse(old.state);if(old.kind!=='cargo_file'||d.actor_id!==u.id||d.draft!==draft||d.hash!==hash||d.file_name!==file.name||d.status==='cancelled')throw Error('资料请求已变化，请重新选择文件');return {ok:true,attachment:d};}
 const t=new Date().toISOString(),data={id,draft,actor_id:u.id,status:'draft',hash,file_name:file.name,file_key:'cargo-drafts/'+id+'/'+hash,file_size:file.size,content_type:file.type,uploaded_by:u.name,created_at:t,material_kind:String(form.get('material_kind')||'shipping_document')};
 if(!['shipping_document','pallet_label','product_label'].includes(data.material_kind))throw Error('资料类型无效');
 await env.R2_BUCKET.put(data.file_key,bytes,{httpMetadata:{contentType:file.type}});
 await q(env,'INSERT OR IGNORE INTO sop_records VALUES(?,?,?,?,?,?)',id,'cargo_file',1,'bulk',JSON.stringify(data),t).run();const saved=JSON.parse((await q(env,'SELECT state FROM sop_records WHERE id=?',id).first()).state);if(saved.hash!==hash||saved.actor_id!==u.id||saved.draft!==draft)throw Error('资料请求冲突');return {ok:true,attachment:saved};
}
export async function cancelCargoDraft(b,env,u){if(b.action!=='sop_cargo_draft_cancel')return null;permit(env,u);const rows=(await q(env,"SELECT * FROM sop_records WHERE kind='cargo_file' AND json_extract(state,'$.draft')=? AND json_extract(state,'$.actor_id')=? AND json_extract(state,'$.status')='draft'",b.draft,u.id).all()).results;for(const row of rows){const d=JSON.parse(row.state),t=new Date().toISOString();await env.DB.batch(chainEvent(env,row,{...d,status:'cancelled'},u,'CANCEL-'+row.id,'sop_cargo_draft_cancel',t));await env.R2_BUCKET.delete?.(d.file_key);}return {ok:true};}
export async function cargoCreateStatements(env,item,data,id,actor,t){
 if(!item.cargo_groups)return [];permit(env,actor);if(data.operation_kind==='direct_forward'||item.outbounds?.length)throw Error('资料分组用于加工计划，出库请在分组审核后安排');
 const cargo=normalizeCargoGroups(item.cargo_groups);if(data.planned_unit!=='箱'||cargo.box_count!==data.planned_quantity)throw Error('分组合计须等于本计划明确箱数');
 const ids=[...new Set([...cargo.public_attachment_ids,...cargo.groups.flatMap(g=>g.attachment_ids)])],files=new Map(),statements=[];
 for(const fileId of ids){const row=await q(env,"SELECT * FROM sop_records WHERE id=? AND kind='cargo_file'",fileId).first(),f=row&&JSON.parse(row.state);if(!f||f.actor_id!==actor.id||f.draft!==item.cargo_groups.draft_id||f.status!=='draft')throw Error('资料草稿不存在、已使用或不属于当前作业');files.set(fileId,{id:fileId,file_key:f.file_key,file_name:f.file_name,created_at:f.created_at,uploaded_by:f.uploaded_by});statements.push(...chainEvent(env,row,{...f,status:'attached',need_id:id},actor,'ATTACH-'+fileId,'sop_cargo_draft_attach',t),q(env,'INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,file_size,content_type,uploaded_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',fileId,'sop_need',id,f.material_kind,f.file_name,f.file_key,f.file_size,f.content_type,actor.name,f.created_at));}
 cargo.public_documents=cargo.public_attachment_ids.map(id=>files.get(id));for(const g of cargo.groups){g.documents=g.attachment_ids.map(id=>files.get(id));g.public_documents=cargo.public_documents;g.mapping_version=1;}
 Object.assign(cargo,{version:1,by:actor.name,actor_id:actor.id,at:t});data.cargo_groups=cargo;return statements;
}
