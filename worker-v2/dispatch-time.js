// Automatic dispatch labor has its own ledger: it is never an operation job or output.
// Internal server API only. Business-route integration is a separate rollout stage.
import {ensureSchema} from './schema-ready.js';
import {kstDay} from './attendance-time.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const actions=new Set(['dispatch','transfer_in','assignment_end','transfer_out','operation_start','operation_end','rest_start','rest_end','checkout','qualification_change','refresh','cutoff']);
const resumptions=new Set(['dispatch','transfer_in','operation_end','rest_end','qualification_change']);
export const dispatchTimeEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_AUTO_DISPATCH_TIME_ENABLED==='true';
export const DISPATCH_TIME_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_sessions(id TEXT PRIMARY KEY,person_id TEXT NOT NULL,attendance_id TEXT NOT NULL,day TEXT NOT NULL,version INTEGER NOT NULL,state TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(person_id,attendance_id))`,
 `CREATE TABLE IF NOT EXISTS ck_dispatch_time_events(request_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,expected_version INTEGER NOT NULL,action TEXT NOT NULL,actor_id TEXT NOT NULL,at TEXT NOT NULL,signature TEXT NOT NULL,before_json TEXT NOT NULL,after_json TEXT NOT NULL)`,
 `CREATE UNIQUE INDEX IF NOT EXISTS ck_dispatch_time_one_person ON ck_dispatch_time_sessions(person_id) WHERE json_extract(state,'$.status')='dispatching'`,
 `CREATE INDEX IF NOT EXISTS ck_dispatch_time_day ON ck_dispatch_time_sessions(day,person_id)`,
 `CREATE TRIGGER IF NOT EXISTS ck_dispatch_time_version BEFORE INSERT ON ck_dispatch_time_events BEGIN SELECT CASE WHEN COALESCE((SELECT version FROM ck_dispatch_time_sessions WHERE id=NEW.session_id),0)!=NEW.expected_version THEN RAISE(ABORT,'dispatch_time_version_conflict') END; END`
];
export async function ensureDispatchTime(e){
 if(!dispatchTimeEnabled(e))return;
 await ensureSchema(e.DB,'dispatch-time-ledger-v1',()=>e.DB.batch(DISPATCH_TIME_SCHEMA.map(s=>e.DB.prepare(s))));
}
function stamp(value){const t=Date.parse(value);if(!Number.isFinite(t))throw Error('派审事件时间无效');return t;}
function boundary(at,day){
 const midnight=Date.parse(day+'T00:00:00+09:00')+86400000;
 let cutoff=Date.parse(day+'T04:00:00+09:00');if(cutoff<=at)cutoff+=86400000;
 return Math.min(midnight,cutoff);
}
const iso=t=>new Date(t).toISOString();
function close(state,at,reason){
 const segment=state.segments.find(s=>!s.end);if(!segment)return;
 segment.end=iso(Math.max(stamp(segment.start),at));segment.endReason=reason;
}
function expire(state,at){
 const segment=state.segments.find(s=>!s.end);if(!segment)return;
 const last=stamp(state.lastConfirmedAt),limit=Math.min(last+state.leaseMs,boundary(stamp(segment.start),state.day));
 if(at<=limit&&kstDay(iso(at))===state.day)return;
 const end=Math.min(last,boundary(stamp(segment.start),state.day));
 close(state,end,'unconfirmed');state.suspendedForVerification=true;
 state.gaps.push({from:iso(end),observedAt:iso(at),reason:kstDay(iso(at))!==state.day?'cross_day':'confirmation_expired'});
}
/** Context comes from authoritative post-transition attendance/ownership facts, never request JSON.
 * leaseMs is an explicit rollout policy; no timeout is silently enabled by this module.
 */
export function transitionDispatchTime(previous,event,context){
 if(!actions.has(event.action))throw Error('无效派审计时事件');
 const at=stamp(event.at),leaseMs=Number(context.leaseMs);
 if(!Number.isSafeInteger(leaseMs)||leaseMs<=0||leaseMs>3600000)throw Error('请明确派审确认有效期');
 for(const key of ['personId','attendanceId','day'])if(!context[key])throw Error('派审身份或签到归属缺失');
 if(!/^\d{4}-\d{2}-\d{2}$/.test(context.day)||kstDay(context.day+'T12:00:00+09:00')!==context.day)throw Error('派审日期无效');
 const state=previous?structuredClone(previous):{personId:context.personId,attendanceId:context.attendanceId,day:context.day,version:0,status:'waiting_dispatch',activatedAt:'',lastConfirmedAt:'',lastEventAt:'',leaseMs,responsibilities:[],segments:[],gaps:[],suspendedForVerification:false};
 if(state.personId!==context.personId||state.attendanceId!==context.attendanceId||state.day!==context.day)throw Error('不得把派审时间并入另一身份或签到');
 if(state.lastEventAt&&at<stamp(state.lastEventAt))throw Error('派审事件早于已确认状态');
 expire(state,at);state.leaseMs=leaseMs;
 // Only actual successful dispatch or an explicit owner handoff can create responsibility.
 if(['dispatch','transfer_in'].includes(event.action)){
  if(!event.jobId)throw Error('缺少真实大货作业单');
  if(kstDay(event.at)!==context.day)throw Error('不能以旧班次记录当天派审');
  if(!state.responsibilities.includes(event.jobId))state.responsibilities.push(event.jobId);
  state.activatedAt ||= event.at;
 }
 if(['assignment_end','transfer_out'].includes(event.action))state.responsibilities=state.responsibilities.filter(id=>id!==event.jobId);
 // A heartbeat may confirm an existing responsibility; it cannot invent an assignment.
 if(context.unfinishedJobIds)state.responsibilities=state.responsibilities.filter(id=>context.unfinishedJobIds.includes(id));
 const currentDay=kstDay(event.at)===context.day;
 const present=context.attendanceValid===true&&currentDay&&(!context.inAt||at>=stamp(context.inAt));
 const canDispatch=present&&context.qualified===true&&context.identityUnique===true&&!context.resting&&!context.operating&&!context.pendingLoan;
 const interrupted=event.action==='checkout'||event.action==='cutoff';
 if(interrupted||!canDispatch||!state.responsibilities.length)close(state,at,event.action);
 if(interrupted||!present)state.status=event.action==='cutoff'||!currentDay?'pending_review':'out';
 else if(!context.identityUnique)state.status='pending_review';
 else if(context.operating)state.status='operating';
 else if(context.resting)state.status='rest';
 else if(!context.qualified||context.pendingLoan)state.status='blocked';
 else if(!state.responsibilities.length)state.status='waiting_dispatch';
 else if(state.suspendedForVerification&&!resumptions.has(event.action))state.status='pending_review';
 else{
  state.status='dispatching';state.suspendedForVerification=false;
  if(!state.segments.some(s=>!s.end))state.segments.push({start:event.at,end:'',startReason:event.action,endReason:'',actorId:event.actorId||'',requestId:event.requestId});
  state.lastConfirmedAt=event.at;
 }
 state.lastEventAt=event.at;state.version++;
 return state;
}
export function dispatchTimeReport(state,asOf){
 if(!state)return {seconds:0,status:'waiting_dispatch',segments:[],gaps:[]};
 const copy=structuredClone(state),at=stamp(asOf);expire(copy,at);
 const segments=copy.segments.map(s=>({...s,seconds:Math.max(0,(Math.min(stamp(s.end||asOf),at)-stamp(s.start))/1000)})).filter(s=>s.seconds>0);
 return {seconds:segments.reduce((n,s)=>n+s.seconds,0),status:copy.suspendedForVerification&&copy.status==='dispatching'?'pending_review':copy.status,segments,gaps:copy.gaps,personId:copy.personId,attendanceId:copy.attendanceId,day:copy.day};
}
/** Append the ledger change to the SAME batch as the real business transition.
 * Failed dispatches and CAS conflicts roll both writes back; retries replay no business SQL.
 * No route currently invokes this foundation until lifecycle hooks are verified.
 */
export async function commitDispatchTime(e,event,context,businessStatements=[]){
 if(!dispatchTimeEnabled(e))throw Error('自动派审工时尚未启用');
 await ensureDispatchTime(e);
 if(!event.requestId||!event.actorId)throw Error('缺少服务端派审事件身份');
 const id=context.personId+'|'+context.attendanceId;
 const signature=JSON.stringify({event,context});
 const replay=async()=>{const old=await q(e,'SELECT * FROM ck_dispatch_time_events WHERE request_id=?',event.requestId).first();if(!old)return null;if(old.signature!==signature||old.actor_id!==event.actorId)throw Error('派审请求编号内容冲突');return JSON.parse(old.after_json);};
 const cached=await replay();if(cached)return cached;
 const row=await q(e,'SELECT state FROM ck_dispatch_time_sessions WHERE id=?',id).first(),before=row?JSON.parse(row.state):null;
 const after=transitionDispatchTime(before,event,context);
 const sql=[...businessStatements,q(e,'INSERT INTO ck_dispatch_time_events VALUES(?,?,?,?,?,?,?,?,?)',event.requestId,id,before?.version||0,event.action,event.actorId,event.at,signature,JSON.stringify(before),JSON.stringify(after)),q(e,'INSERT INTO ck_dispatch_time_sessions VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version,state=excluded.state,updated_at=excluded.updated_at',id,context.personId,context.attendanceId,context.day,after.version,JSON.stringify(after),event.at)];
 try{await e.DB.batch(sql);}catch(error){const saved=await replay();if(saved)return saved;throw error;}
 return after;
}
