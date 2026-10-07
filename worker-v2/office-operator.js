// Self-entered operation labels are business audit metadata, not account identities.
// No password, role, cookie, session lifetime or field-badge policy is changed here.
import {accessEnabled,digest} from './access-control.js';
import {ensureSchema} from './schema-ready.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const publicContext=r=>({id:r.context_id,name:r.operator_name,self_entered:true});
export async function ensureOfficeAudit(env){
 return ensureSchema(env.DB,'office-operation-audit-v1',()=>env.DB.batch([
  env.DB.prepare("CREATE TABLE IF NOT EXISTS ck_office_operation_context(session_hash TEXT PRIMARY KEY,context_id TEXT NOT NULL,operator_name TEXT NOT NULL DEFAULT '',confirmed_at TEXT NOT NULL DEFAULT '')"),
  env.DB.prepare("CREATE TABLE IF NOT EXISTS ck_plan_operation_audit(id TEXT PRIMARY KEY,doc_type TEXT NOT NULL,doc_id TEXT NOT NULL,kind TEXT NOT NULL,actor_id TEXT NOT NULL,actor_name TEXT NOT NULL,created_at TEXT NOT NULL)"),
  env.DB.prepare('CREATE INDEX IF NOT EXISTS ck_plan_audit_doc ON ck_plan_operation_audit(doc_type,doc_id,created_at)')
 ]));
}
export async function officeOperationContext(request,env){
 if(!accessEnabled(env)||env.SOP_REQUEST_USER?.scope!=='office')return null;
 const raw=(request.headers.get('Cookie')||'').split(';').map(v=>v.trim()).find(v=>v.startsWith('ck_office_access='))?.slice(17);
 if(!/^[a-f0-9]{64}$/.test(raw||''))return null;
 await ensureOfficeAudit(env);const key=await digest(raw);
 let row=await q(env,'SELECT * FROM ck_office_operation_context WHERE session_hash=?',key).first();
 if(!row){await q(env,'INSERT OR IGNORE INTO ck_office_operation_context(session_hash,context_id) VALUES(?,?)',key,crypto.randomUUID()).run();row=await q(env,'SELECT * FROM ck_office_operation_context WHERE session_hash=?',key).first();}
 env.SOP_OPERATION_SESSION=key;env.SOP_OPERATION_CONTEXT=row;
 const u=env.SOP_REQUEST_USER;
 env.SOP_REQUEST_USER={...u,account_name:u.name,name:row.operator_name||u.name,operation_context:publicContext(row)};
 return row;
}
const conflict=()=>Response.json({ok:false,operator_context_changed:true,error:'操作姓名尚未确认或已切换，请重新加载并确认；未提交内容请先保留 / 작업자 이름을 다시 확인하세요. 저장하지 않은 내용은 먼저 보관하세요.'},{status:409,headers:{'Cache-Control':'no-store'}});
export async function confirmOfficeOperator(body,env,request){
 if(body.action!=='sop_operator_confirm')return null;
 if(!env.SOP_OPERATION_CONTEXT)return Response.json({ok:false,error:'仅办公室登录会话可确认操作姓名 / 사무실 로그인 후 확인하세요'},{status:403});
 const name=typeof body.name==='string'?body.name.trim().normalize('NFC'):'';
 if(!name||[...name].length>40||/[\p{Cc}\p{Cf}]/u.test(name))return Response.json({ok:false,error:'请输入1至40个字符的操作姓名，不能包含控制字符 / 작업자 이름은 1–40자로 입력하세요'},{status:400});
 const expected=request.headers.get('X-CK-Operation-Context');
 if(expected!==env.SOP_OPERATION_CONTEXT.context_id)return conflict();
 const row=await q(env,'UPDATE ck_office_operation_context SET operator_name=?,context_id=?,confirmed_at=? WHERE session_hash=? AND context_id=? RETURNING *',name,crypto.randomUUID(),new Date().toISOString(),env.SOP_OPERATION_SESSION,expected).first();
 if(!row)return conflict();
 return Response.json({ok:true,user:{...env.SOP_REQUEST_USER,name,operation_context:publicContext(row)}},{headers:{'Cache-Control':'no-store'}});
}
// Only coordination/business writes require the label. Attendance registration,
// account administration and consumables keep their existing policies.
const reads=new Set('sop_login sop_identity sop_logout sop_operator_confirm sop_get sop_list sop_linked sop_updates sop_source_detail sop_need_groups sop_batch_work_materials sop_work_materials sop_work_need_search sop_dispatch_list sop_field_resolve sop_check_for_batch sop_courier_config sop_courier_list sop_courier_detail sop_crew_availability sop_crew_status'.split(' '));
export function officeOperationGuard(body,env,request){
 if(!env.SOP_OPERATION_CONTEXT)return null;
 const action=String(body.action||'').trim();
 const business=/^v2_(?:inbound|outbound|ops|issue|verify|pick|bulk|unload|unplanned|import_delivery|attachment|correction)_/.test(action)||/^sop_(?:need|task|issue|check|native|courier|work_material|batch|demo|crew_return)/.test(action);
 const read=reads.has(action)||/_(?:list|detail|list_upcoming|ops_candidates|find_by_code|resolve_code|resolve|export|diag_date_mismatch|lookup|active_list|docs_list|my_active_job|get)$/.test(action);
 if(business&&!read&&(!env.SOP_OPERATION_CONTEXT.operator_name||request.headers.get('X-CK-Operation-Context')!==env.SOP_OPERATION_CONTEXT.context_id))return conflict();
 if(env.SOP_OPERATION_CONTEXT.operator_name){
  const name=env.SOP_OPERATION_CONTEXT.operator_name;
  for(const target of [body,body.payload].filter(x=>x&&typeof x==='object'))for(const field of ['created_by','by','actor','modified_by','operator','operator_name','requested_by','uploaded_by'])target[field]=name;
 }
 return null;
}
export function planAuditStatements(env,type,id,kind,time){
 const context=env.SOP_OPERATION_CONTEXT;
 if(!context?.operator_name)return [];
 const table=type==='inbound'?'v2_inbound_plans':'v2_outbound_orders';
 return [q(env,`INSERT INTO ck_plan_operation_audit SELECT ?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM ${table} WHERE id=?)`,crypto.randomUUID(),type,id,kind,env.SOP_REQUEST_USER.id,context.operator_name,time,id)];
}
export async function planOperationAudit(env,type,id){
 if(!accessEnabled(env))return [];
 await ensureOfficeAudit(env);
 return (await q(env,'SELECT kind,actor_id,actor_name,created_at FROM ck_plan_operation_audit WHERE doc_type=? AND doc_id=? ORDER BY created_at DESC,rowid DESC LIMIT 100',type,id).all()).results;
}
