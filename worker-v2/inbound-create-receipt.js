// Staging-only create receipts. The receipt and all business rows commit together.
import {ensureSchema} from './schema-ready.js';
const q=(env,sql,...values)=>env.DB.prepare(sql).bind(...values);
export const INBOUND_CREATE_LEASE_MS=60000;
const ignored=new Set('action client_req_id create_intent k sop_key ops_key created_by by actor modified_by operator operator_name requested_by uploaded_by'.split(' '));
const stable=(value,root=false)=>Array.isArray(value)?value.map(v=>stable(v)):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().filter(k=>!root||!ignored.has(k)).map(k=>[k,stable(value[k])])):value;
const hash=async value=>[...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))].map(v=>v.toString(16).padStart(2,'0')).join('');
const actor=env=>env.SOP_REQUEST_USER;
const allowed=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true'&&actor(env)?.scope==='office'&&['manager','service'].includes(actor(env)?.role);
const context=env=>hash(JSON.stringify([actor(env)?.id,actor(env)?.scope,env.SOP_OPERATION_CONTEXT?.context_id||'legacy-personal-session']));
const response=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store'}});
const uncertain=(message='原提交结果尚未确认，请查询原结果后继续 / 원래 저장 결과를 확인하세요.',state='unknown')=>response({ok:false,error:message,receipt_state:state},409);
const validId=value=>typeof value==='string'&&value.length>=8&&value.length<=200&&/^[A-Za-z0-9_-]+$/.test(value);
export const INBOUND_CREATE_RECEIPT_SQL="CREATE TABLE IF NOT EXISTS ck_inbound_create_receipts(request_id TEXT PRIMARY KEY,intent_id TEXT NOT NULL UNIQUE,actor_id TEXT NOT NULL,context_hash TEXT NOT NULL,fingerprint TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('pending','committed','rejected')),run_token TEXT NOT NULL,lease_until INTEGER NOT NULL,result_json TEXT NOT NULL DEFAULT '{}',error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)";
export async function ensureInboundCreateReceipts(env){
 return ensureSchema(env.DB,'inbound-create-receipts-v1',()=>env.DB.batch([
  q(env,INBOUND_CREATE_RECEIPT_SQL)
 ]));
}
const lookup=(env,id)=>q(env,'SELECT * FROM ck_inbound_create_receipts WHERE request_id=?',id).first();
const sameActor=async(env,row)=>row.actor_id===actor(env)?.id&&row.context_hash===await context(env);
const publicStatus=row=>row.state==='committed'?{ok:true,state:'committed',result:JSON.parse(row.result_json),request_id:row.request_id,create_intent:row.intent_id}:row.state==='rejected'?{ok:true,state:'rejected',not_saved:true,error:row.error,request_id:row.request_id,create_intent:row.intent_id}:{ok:true,state:'pending',can_retry:row.lease_until<=Date.now(),request_id:row.request_id,create_intent:row.intent_id};
export async function inboundCreateStatus(body,env){
 if(!allowed(env))return response({ok:false,error:'无此提交查询权限'},403);
 if(!validId(body.request_id)||!validId(body.create_intent))return response({ok:false,error:'缺少有效的原提交编号'},400);
 await ensureInboundCreateReceipts(env);const row=await lookup(env,body.request_id);
 if(!row||row.intent_id!==body.create_intent||!await sameActor(env,row))return response({ok:true,state:'unknown',can_retry:true});
 return response(publicStatus(row));
}
async function begin(body,env){
 if(!validId(body.client_req_id)||!validId(body.create_intent))return {response:response({ok:false,error:'缺少有效的创建提交编号',not_saved:true,receipt_state:'rejected'},400)};
 await ensureInboundCreateReceipts(env);
 const now=new Date().toISOString(),token=crypto.randomUUID(),fingerprint=await hash(JSON.stringify(stable(body,true))),ctx=await context(env);
 let row=await q(env,'SELECT * FROM ck_inbound_create_receipts WHERE request_id=? OR intent_id=?',body.client_req_id,body.create_intent).first();
 if(!row){
  // An old unrelated cache entry is not evidence that this new intent saved.
  if(await q(env,'SELECT 1 FROM v2_idempotency_keys WHERE idem_key=?',body.client_req_id).first())return {response:uncertain('此编号已有历史提交，请先核对原记录，不能覆盖 / 기존 제출 번호입니다.')};
  await q(env,"INSERT OR IGNORE INTO ck_inbound_create_receipts(request_id,intent_id,actor_id,context_hash,fingerprint,state,run_token,lease_until,created_at,updated_at) VALUES(?,?,?,?,?,'pending',?,?,?,?)",body.client_req_id,body.create_intent,actor(env).id,ctx,fingerprint,token,Date.now()+INBOUND_CREATE_LEASE_MS,now,now).run();
  row=await q(env,'SELECT * FROM ck_inbound_create_receipts WHERE request_id=? OR intent_id=?',body.client_req_id,body.create_intent).first();
 }
 if(!row||!await sameActor(env,row))return {response:response({ok:false,error:'原提交不属于当前登录人员，请用原身份核对',receipt_state:'unknown'},403)};
 if(row.intent_id!==body.create_intent||row.fingerprint!==fingerprint)return {response:uncertain('原提交内容已冻结，请先查询原保存结果；不能改内容另行重提 / 원래 제출 내용을 확인하세요.')};
 if(row.state==='committed')return {response:response(JSON.parse(row.result_json))};
 if(row.state==='rejected')return {response:response({ok:false,error:row.error,not_saved:true,receipt_state:'rejected'},422)};
 if(row.request_id!==body.client_req_id)return {response:uncertain('请沿用原提交编号查询和继续，不会新建另一套单据。','pending')};
 if(row.run_token===token)return {row};
 if(row.lease_until<=Date.now()){
  await q(env,"UPDATE ck_inbound_create_receipts SET run_token=?,lease_until=?,updated_at=? WHERE request_id=? AND state='pending' AND run_token=? AND lease_until<=?",token,Date.now()+INBOUND_CREATE_LEASE_MS,now,row.request_id,row.run_token,Date.now()).run();
  row=await lookup(env,row.request_id);if(row?.run_token===token&&row.state==='pending')return {row};
  if(row?.state==='committed')return {response:response(JSON.parse(row.result_json))};
 }
 return {response:uncertain('原提交仍在处理，请查询保存结果；尚未建立新单 / 원래 제출 처리 중입니다.','pending')};
}
export function inboundReceiptStatements(env,row,result){
 if(!row)return {before:[],after:[]};
 return {
  before:[q(env,"SELECT CASE WHEN EXISTS(SELECT 1 FROM ck_inbound_create_receipts WHERE request_id=? AND run_token=? AND state='pending') THEN 1 ELSE json('inbound_create_receipt_changed') END",row.request_id,row.run_token)],
  after:[q(env,"UPDATE ck_inbound_create_receipts SET state='committed',result_json=?,lease_until=0,updated_at=? WHERE request_id=? AND run_token=? AND state='pending'",JSON.stringify(result),new Date().toISOString(),row.request_id,row.run_token)]
 };
}
export async function withInboundCreateReceipt(body,env,create){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true')return create(null);
 if(!body.create_intent){
  if(allowed(env)&&validId(body.client_req_id)){await ensureInboundCreateReceipts(env);if(await lookup(env,body.client_req_id))return uncertain('原提交编号已受保护，请沿用原草稿查询结果。');}
  return create(null);
 }
 if(!allowed(env))return response({ok:false,error:'请由办公室客服创建入库计划'},403);
 let opened;try{opened=await begin(body,env);}catch{return response({ok:false,error:'提交回执暂不可用；尚未确认保存，请查询原结果',receipt_state:'unknown'},503);}
 if(opened.response)return opened.response;const row=opened.row;let out,parsed;
 try{out=await create(row);parsed=await out.clone().json();}catch(error){parsed={ok:false,error:error.message||'保存未完成'};}
 try{
  let current=await lookup(env,row.request_id);
  // Includes a committed transaction whose HTTP/post-processing response failed.
  if(current?.state==='committed')return response(JSON.parse(current.result_json));
  if(!current||current.run_token!==row.run_token||current.state!=='pending')return uncertain();
  if(parsed?.ok){
   // Existing feedback association may return an original plan without a new create.
   const statements=inboundReceiptStatements(env,row,parsed);await env.DB.batch([...statements.before,...statements.after]);return response(parsed);
  }
  await q(env,"UPDATE ck_inbound_create_receipts SET state='rejected',error=?,lease_until=0,updated_at=? WHERE request_id=? AND run_token=? AND state='pending'",String(parsed?.error||parsed?.message||'本次未保存，请核对后重试'),new Date().toISOString(),row.request_id,row.run_token).run();
  current=await lookup(env,row.request_id);if(current?.state==='committed')return response(JSON.parse(current.result_json));
  if(current?.state==='rejected')return response({ok:false,error:current.error,not_saved:true,receipt_state:'rejected'},422);
 }catch{}
 return response({ok:false,error:'保存结果尚未确认，原提交编号保留，请查询原结果',receipt_state:'unknown'},503);
}
