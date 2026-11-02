import {attendanceSource,attendanceDaySQL,attendanceCurrentSQL} from './attendance-context.js';
import {ensureSchema} from './schema-ready.js';
import {kstDay} from './attendance-time.js';
import {dispatchTransferTarget} from './daily-dispatch-permissions.js';
// Pairwise dispatch collaboration. Tables are created only while the flag is on,
// and no existing order, owner or labor row is rewritten.
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const fail=(message,status=409)=>{throw Object.assign(Error(message),{status});};
export const shareEnabled=e=>e?.SOP_DISPATCH_SHARE_ENABLED==='true';
const day=()=>kstDay(new Date().toISOString());
const pairOf=(a,b)=>a<b?[a,b]:[b,a];
const empty={ok:true,enabled:false,incoming:[],outgoing:[],partners:[]};
export async function ensureDispatchShare(e){
 if(!shareEnabled(e))return;
 await ensureSchema(e.DB,'dispatch-share-v1',()=>e.DB.batch([
  q(e,`CREATE TABLE IF NOT EXISTS ck_dispatch_share_pairs(
    id TEXT PRIMARY KEY,
    person_lo TEXT NOT NULL,
    person_hi TEXT NOT NULL,
    invited_by TEXT NOT NULL,
    invitee_id TEXT NOT NULL,
    status TEXT NOT NULL,
    day TEXT NOT NULL,
    created_at TEXT NOT NULL,
    accepted_at TEXT NOT NULL DEFAULT '',
    ended_at TEXT NOT NULL DEFAULT '',
    ended_by TEXT NOT NULL DEFAULT ''
  )`),
  q(e,`CREATE UNIQUE INDEX IF NOT EXISTS ck_dispatch_share_open_pair ON ck_dispatch_share_pairs(person_lo,person_hi) WHERE status IN ('pending','active')`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_dispatch_share_one_partner_insert BEFORE INSERT ON ck_dispatch_share_pairs WHEN NEW.status IN ('pending','active') AND EXISTS(SELECT 1 FROM ck_dispatch_share_pairs WHERE status IN ('pending','active') AND (person_lo IN (NEW.person_lo,NEW.person_hi) OR person_hi IN (NEW.person_lo,NEW.person_hi))) BEGIN SELECT RAISE(ABORT,'ck_dispatch_share_one_partner'); END`),
  q(e,`CREATE TRIGGER IF NOT EXISTS ck_dispatch_share_one_partner_update BEFORE UPDATE OF status ON ck_dispatch_share_pairs WHEN NEW.status IN ('pending','active') AND EXISTS(SELECT 1 FROM ck_dispatch_share_pairs WHERE id<>NEW.id AND status IN ('pending','active') AND (person_lo IN (NEW.person_lo,NEW.person_hi) OR person_hi IN (NEW.person_lo,NEW.person_hi))) BEGIN SELECT RAISE(ABORT,'ck_dispatch_share_one_partner'); END`),
  q(e,`CREATE TABLE IF NOT EXISTS ck_dispatch_share_events(
    request_id TEXT PRIMARY KEY,
    pair_id TEXT NOT NULL,
    action TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    signature TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  )`),
  q(e,`CREATE TABLE IF NOT EXISTS ck_dispatch_share_checks(id TEXT PRIMARY KEY, allowed INTEGER NOT NULL CONSTRAINT ck_dispatch_share_checks CHECK(allowed=1))`)
 ]));
}
async function expire(e){
 await q(e,"UPDATE ck_dispatch_share_pairs SET status='expired',ended_at=?,ended_by='expired' WHERE status IN ('pending','active') AND day<>?",new Date().toISOString(),day()).run();
}
function caller(e){
 const u=e.SOP_REQUEST_USER;
 if(!u||u.scope!=='field'||u.role!=='dispatcher')fail('请用派审员工牌登录后再协作');
 const departments=u.daily_dispatch?u.dispatch_departments||[]:u.departments||[];
 if(!departments.includes('bulk')||u.daily_dispatch&&!u.can_dispatch)fail('仅当天在岗的大货派审员可以协作');
 return u;
}
async function onDuty(e,u){
 if(!u.attendance_id)fail('请先完成当天签到');
 const row=await q(e,`SELECT 1 FROM ${attendanceSource(e)} d WHERE d.id=? AND ${attendanceDaySQL(e,'d','?')} AND ${attendanceCurrentSQL(e,'d')} AND d.signed_out='' AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at='')`,u.attendance_id,day()).first();
 if(!row)fail('派审员须已签到且不在休息中');
}
const pub=p=>({id:p.id,person_lo:p.person_lo,person_hi:p.person_hi,invited_by:p.invited_by,invitee_id:p.invitee_id,status:p.status,day:p.day,created_at:p.created_at,accepted_at:p.accepted_at,ended_at:p.ended_at,ended_by:p.ended_by});
async function replay(e,u,action,request,signature){
 const old=await q(e,'SELECT * FROM ck_dispatch_share_events WHERE request_id=?',request).first();
 if(!old)return null;
 if(old.actor_id!==u.id||old.action!==action||old.signature!==signature)fail('请求编号已用于其他协作操作');
 return JSON.parse(old.response_json);
}
async function people(e,ids){
 if(!ids.length)return new Map();
 const list=await rows(e,'SELECT id,name,badge_id FROM ck_attendance_people WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(ids));
 return new Map(list.map(p=>[p.id,p]));
}
function named(map,id){const p=map.get(id);return {id,name:p?.name||'',badge:p?.badge_id||''};}
export async function shareStatus(e){
 if(!shareEnabled(e))return empty;
 await ensureDispatchShare(e);await expire(e);
 const u=e.SOP_REQUEST_USER;if(!u?.id)return {...empty,enabled:true};
 const list=await rows(e,"SELECT * FROM ck_dispatch_share_pairs WHERE status IN ('pending','active') AND (person_lo=? OR person_hi=?)",u.id,u.id);
 const names=await people(e,[...new Set(list.flatMap(p=>[p.person_lo,p.person_hi,p.invited_by]))]);
 const view=p=>({...pub(p),inviter:named(names,p.invited_by),invitee:named(names,p.invitee_id),partner:named(names,p.person_lo===u.id?p.person_hi:p.person_lo)});
 return {ok:true,enabled:true,incoming:list.filter(p=>p.status==='pending'&&p.invitee_id===u.id).map(view),outgoing:list.filter(p=>p.status==='pending'&&p.invited_by===u.id).map(view),partners:list.filter(p=>p.status==='active').map(view)};
}
async function occupied(e,personId){
 return q(e,"SELECT id FROM ck_dispatch_share_pairs WHERE status IN ('pending','active') AND (person_lo=? OR person_hi=?)",personId,personId).first();
}
async function openPair(e,a,b){
 const [lo,hi]=pairOf(a,b);
 return q(e,"SELECT * FROM ck_dispatch_share_pairs WHERE person_lo=? AND person_hi=? AND status IN ('pending','active')",lo,hi).first();
}
export async function activePartnerIds(e,personId){
 if(!shareEnabled(e)||!personId)return [];
 await ensureDispatchShare(e);await expire(e);
 const list=await rows(e,"SELECT person_lo,person_hi FROM ck_dispatch_share_pairs WHERE status='active' AND day=? AND (person_lo=? OR person_hi=?)",day(),personId,personId);
 return list.map(p=>p.person_lo===personId?p.person_hi:p.person_lo);
}
export async function isActivePartner(e,actorId,ownerId){
 if(!shareEnabled(e)||!actorId||!ownerId||actorId===ownerId)return false;
 return (await activePartnerIds(e,actorId)).includes(ownerId);
}
// The first authorization records why it passed. A later lookup must not drop
// the in-transaction check just because the pair ended in between.
const shareGrants=new WeakMap();
export function noteShareGrant(subject,viaShare){
 if(!subject||typeof subject!=='object')return;
 if(shareGrants.get(subject)===true)return;
 shareGrants.set(subject,!!viaShare);
}
export function notedShareGrant(subject){return !!(subject&&typeof subject==='object'&&shareGrants.get(subject));}
export function isBulkShareRecord(row,data=row?.data){
 if(!row||!data)return false;
 if(row.kind==='bulk_batch')return true;
 return data.job_type==='bulk_op'&&['task','dispatch'].includes(row.kind)&&(row.department==='bulk'||data.labor_department==='bulk');
}
// viaShare is the authorization recorded at the first check. When it is true,
// the pair is rechecked inside the write transaction even if it has already
// ended. Owner, manager, reviewer, delegate and off-duty claim pass false and
// keep their own rules. The check row is deleted in the same transaction.
export const shareConflict=/ck_dispatch_share_checks/;
export function shareGuardStatements(e,actor,recordId,revision){
 const id=crypto.randomUUID();
 return [q(e,`INSERT INTO ck_dispatch_share_checks(id,allowed)
  SELECT ?, CASE WHEN (SELECT revision FROM sop_records WHERE id=?)=?
   AND EXISTS(SELECT 1 FROM ck_dispatch_share_pairs WHERE status='active' AND day=? AND (
     (person_lo=? AND person_hi=json_extract((SELECT state FROM sop_records WHERE id=?),'$.owner_id'))
     OR (person_hi=? AND person_lo=json_extract((SELECT state FROM sop_records WHERE id=?),'$.owner_id'))
   )) THEN 1 ELSE 0 END`,id,recordId,revision,day(),actor.id,recordId,actor.id,recordId),
  q(e,'DELETE FROM ck_dispatch_share_checks WHERE id=?',id)];
}
export async function partnerShareGuard(e,record,actor=e.SOP_REQUEST_USER,viaShare=false){
 if(!viaShare||!shareEnabled(e)||!record)return [];
 const u=actor;if(!u?.id)return [];
 let data=record.data;if(!data){try{data=JSON.parse(record.state||'{}');}catch{return [];}}
 const row={id:record.id,kind:record.kind,department:record.department,data};
 if(!isBulkShareRecord(row,data))return [];
 if(u.role==='manager'||u.role==='reviewer'||data.owner_id===u.id)return [];
 if((data.delegates||[]).includes(u.id)&&!(data.owner_transfers||[]).some(x=>x.from_id===u.id))return [];
 await ensureDispatchShare(e);
 return shareGuardStatements(e,u,record.id,record.revision);
}
export async function handleDispatchShare(b,e){
 if(!shareEnabled(e))return b.action==='sop_dispatch_share_status'?empty:{ok:false,enabled:false,error:'派审协作未开启'};
 await ensureDispatchShare(e);await expire(e);
 if(b.action==='sop_dispatch_share_status')return shareStatus(e);
 const leaving=b.action==='sop_dispatch_share_leave';
 const u=leaving?e.SOP_REQUEST_USER:caller(e);
 if(!u?.id)fail('请先登录');
 if(!leaving)await onDuty(e,u);
 const request=String(b.client_req_id||'').trim();if(!request||request.length>120)fail('缺少请求编号');
 if(b.action==='sop_dispatch_share_invite'){
  const target=await dispatchTransferTarget(e,b.target_badge,'bulk');
  if(target.id===u.id)fail('不能邀请自己');
  const signature=JSON.stringify({target:target.id});
  const prior=await replay(e,u,b.action,request,signature);if(prior)return prior;
  if(await openPair(e,u.id,target.id))fail('已有待确认或进行中的协作');
  if(await occupied(e,u.id))fail('不能同时配对两组，请先结束当前协作');
  if(await occupied(e,target.id))fail('对方已有协作，不能同时配对两组');
  const [lo,hi]=pairOf(u.id,target.id),id='DS-'+crypto.randomUUID(),t=new Date().toISOString();
  const response={ok:true,enabled:true,pair:{id,status:'pending',day:day(),invited_by:u.id,invitee_id:target.id,partner:{id:target.id,name:target.name}}};
  try{await e.DB.batch([
   q(e,'INSERT INTO ck_dispatch_share_pairs(id,person_lo,person_hi,invited_by,invitee_id,status,day,created_at) VALUES(?,?,?,?,?,?,?,?)',id,lo,hi,u.id,target.id,'pending',day(),t),
   q(e,'INSERT INTO ck_dispatch_share_events(request_id,pair_id,action,actor_id,actor_name,signature,response_json,created_at) VALUES(?,?,?,?,?,?,?,?)',request,id,b.action,u.id,u.name,signature,JSON.stringify(response),t)
  ]);}catch(error){if(/ck_dispatch_share_one_partner/i.test(error.message||''))fail('不能同时配对两组，请先结束当前协作');if(/UNIQUE/i.test(error.message||''))fail('已有待确认或进行中的协作');throw error;}
  return response;
 }
 const pair=await q(e,'SELECT * FROM ck_dispatch_share_pairs WHERE id=?',String(b.pair_id||'')).first();
 if(!pair)fail('协作邀请不存在');
 const signature=JSON.stringify({pair:pair.id});
 const prior=await replay(e,u,b.action,request,signature);if(prior)return prior;
 const member=pair.person_lo===u.id||pair.person_hi===u.id;if(!member)fail('这不是你的协作');
 const t=new Date().toISOString();
 let next=pair.status,endedBy='';
 if(b.action==='sop_dispatch_share_accept'){
  if(pair.invitee_id!==u.id)fail('请由被邀请的派审员确认');
  if(pair.status!=='pending')fail('邀请已结束，请刷新');
  if(pair.day!==day())fail('邀请已过期，请重新发起');
  if(await q(e,"SELECT id FROM ck_dispatch_share_pairs WHERE id<>? AND status IN ('pending','active') AND (person_lo IN (?,?) OR person_hi IN (?,?))",pair.id,pair.person_lo,pair.person_hi,pair.person_lo,pair.person_hi).first())fail('不能同时配对两组，请先结束当前协作');
  next='active';
 }else if(b.action==='sop_dispatch_share_cancel'){
  if(pair.invited_by!==u.id||pair.status!=='pending')fail('只能取消自己发出且尚未确认的邀请');
  next='cancelled';endedBy=u.id;
 }else if(b.action==='sop_dispatch_share_leave'){
  if(pair.status!=='active')fail('当前没有进行中的协作');
  next='ended';endedBy=u.id;
 }else fail('未知协作操作');
 const response={ok:true,enabled:true,pair:{...pub(pair),status:next,accepted_at:next==='active'?t:pair.accepted_at,ended_at:next==='active'?'':t,ended_by:endedBy}};
 const checkId=crypto.randomUUID();
 try{await e.DB.batch([
  q(e,"UPDATE ck_dispatch_share_pairs SET status=?,accepted_at=CASE WHEN ?='active' THEN ? ELSE accepted_at END,ended_at=CASE WHEN ?='active' THEN '' ELSE ? END,ended_by=? WHERE id=? AND status=?",next,next,t,next,t,endedBy,pair.id,pair.status),
  q(e,`INSERT INTO ck_dispatch_share_checks(id,allowed) SELECT ?, CASE WHEN changes()=1 AND (SELECT status FROM ck_dispatch_share_pairs WHERE id=?)=? THEN 1 ELSE 0 END`,checkId,pair.id,next),
  q(e,'DELETE FROM ck_dispatch_share_checks WHERE id=?',checkId),
  q(e,'INSERT INTO ck_dispatch_share_events(request_id,pair_id,action,actor_id,actor_name,signature,response_json,created_at) VALUES(?,?,?,?,?,?,?,?)',request,pair.id,b.action,u.id,u.name,signature,JSON.stringify(response),t)
 ]);}catch(error){const saved=await replay(e,u,b.action,request,signature);if(saved)return saved;if(/ck_dispatch_share_one_partner/i.test(error.message||''))fail('不能同时配对两组，请先结束当前协作');if(shareConflict.test(error.message||''))fail('邀请已确认或已结束，请刷新');throw error;}
 return response;
}
