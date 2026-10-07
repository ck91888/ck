import {ensureSchema} from './schema-ready.js';
import {ensureAttendance,guardAttendance} from './attendance.js';
import {ensureDispatchStartGuard} from './atomic-native-start.js';
import {staffedDispatchStatements} from './crew-borrow.js';
import {nativeOwner} from './sop-dispatch.js';
import {nativePeople} from './native-lifecycle.js';
import {dispatchAccess} from './dispatch-access.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const fail=m=>{throw Error(m);};
export const COURIER_BATCH_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_courier_batches(id TEXT PRIMARY KEY,day TEXT NOT NULL,sequence INTEGER NOT NULL,batch_no TEXT NOT NULL UNIQUE,created_at TEXT NOT NULL,UNIQUE(day,sequence))`,
 `CREATE TABLE IF NOT EXISTS ck_courier_batch_items(receipt_id TEXT PRIMARY KEY,batch_id TEXT NOT NULL REFERENCES ck_courier_batches(id))`,
 `CREATE INDEX IF NOT EXISTS ck_courier_batch_receipts ON ck_courier_batch_items(batch_id,receipt_id)`,
 `CREATE TABLE IF NOT EXISTS ck_courier_batch_requests(request_id TEXT PRIMARY KEY,actor_id TEXT NOT NULL,signature TEXT NOT NULL,batch_id TEXT NOT NULL,kind TEXT NOT NULL,created_at TEXT NOT NULL)`,
 // A stale scanner cannot commit after the batch has completed. Check inside D1's transaction.
 `CREATE TRIGGER IF NOT EXISTS ck_courier_batch_receipt_guard BEFORE INSERT ON ck_courier_batch_items BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_courier_batches b JOIN v2_ops_jobs j ON j.id=b.id WHERE b.id=NEW.batch_id AND j.status='working') THEN RAISE(ABORT,'快递批次已完成 / 택배 차수 종료') END; END`,
 `CREATE TRIGGER IF NOT EXISTS ck_courier_finish_guard BEFORE INSERT ON ck_courier_batch_requests WHEN NEW.kind='finish' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE j.id=NEW.batch_id AND j.status IN ('working','awaiting_close') AND s.revision=json_extract(NEW.signature,'$.revision')) THEN RAISE(ABORT,'批次或人员已变化，请刷新 / 차수 정보 변경') END; END`,
 `CREATE TRIGGER IF NOT EXISTS ck_courier_batch_immutable BEFORE UPDATE ON ck_courier_batch_items BEGIN SELECT RAISE(ABORT,'receipt_batch_is_immutable'); END`
];
export async function ensureCourierBatches(e){await ensureSchema(e.DB,'courier-batches-v1',()=>e.DB.batch(COURIER_BATCH_SCHEMA.map(s=>q(e,s))));}
export async function courierBatchDetail(e,id){
 const batch=await q(e,`SELECT b.*,j.status,j.finished_at,s.revision,s.state FROM ck_courier_batches b JOIN v2_ops_jobs j ON j.id=b.id JOIN sop_records s ON s.id=b.id WHERE b.id=?`,String(id||'')).first();if(!batch)fail('快递批次不存在 / 택배 차수가 없습니다');
 const state=JSON.parse(batch.state);delete batch.state;
 const [counts,workers]=await Promise.all([rows(e,`SELECT r.owner,COUNT(*) AS count FROM ck_courier_batch_items i JOIN ck_courier_receipts r ON r.id=i.receipt_id WHERE i.batch_id=? GROUP BY r.owner`,id),rows(e,"SELECT worker_id AS id,worker_name AS name,joined_at,left_at FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at,id",id)]);
 return {...batch,owner:state.owner,lead_id:state.lead_id,workers:workers.filter(w=>!w.left_at),segments:workers,counts:Object.fromEntries(counts.map(x=>[x.owner,x.count])),total:counts.reduce((n,x)=>n+x.count,0),can_manage:await nativeOwner({job_id:id},e)};
}
export async function courierBatchList(e,b={}){
 const args=[],where=['1=1'];
 if(b.active){const a=dispatchAccess(e.SOP_REQUEST_USER);where.push("j.status IN ('working','awaiting_close') AND "+a.sql);args.push(...a.args);}
 if(b.from){where.push('b.day>=?');args.push(b.from);}if(b.to){where.push('b.day<=?');args.push(b.to);}
 const keyword=String(b.keyword||'').trim().toUpperCase().replace(/[ -]/g,'');
 const itemFilters=[],itemArgs=[];
 if(keyword){itemFilters.push("replace(r.tracking_no,'-','') LIKE ? ESCAPE '\\'");itemArgs.push('%'+keyword.replace(/[\\%_]/g,'\\$&')+'%');}
 if(b.owner){itemFilters.push('r.owner=?');itemArgs.push(b.owner);}
 if(b.status){itemFilters.push('r.status=?');itemArgs.push(b.status);}
 if(itemFilters.length){where.push(`EXISTS(SELECT 1 FROM ck_courier_batch_items i JOIN ck_courier_receipts r ON r.id=i.receipt_id WHERE i.batch_id=b.id AND ${itemFilters.join(' AND ')})`);args.push(...itemArgs);}
 const limit=Math.min(100,Math.max(1,parseInt(b.limit)||50)),offset=Math.max(0,parseInt(b.offset)||0),condition=where.join(' AND ');
 const reads=await e.DB.batch([q(e,`SELECT COUNT(*) AS total FROM ck_courier_batches b JOIN v2_ops_jobs j ON j.id=b.id JOIN sop_records s ON s.id=b.id WHERE ${condition}`,...args),q(e,`SELECT b.*,j.status,j.finished_at,json_extract(s.state,'$.owner') AS owner,(SELECT COUNT(*) FROM ck_courier_batch_items i WHERE i.batch_id=b.id) AS total FROM ck_courier_batches b JOIN v2_ops_jobs j ON j.id=b.id JOIN sop_records s ON s.id=b.id WHERE ${condition} ORDER BY b.created_at DESC,b.sequence DESC LIMIT ? OFFSET ?`,...args,limit,offset)]);
 return {ok:true,items:reads[1].results,total:reads[0].results[0].total,limit,offset};
}
// Canonical attendance names, rather than names supplied by a scanner or client.
async function staff(e,b,allowEmpty=false){
 if(!Array.isArray(b.workers)||b.workers.length>50||(!allowEmpty&&!b.workers.length)||new Set(b.workers.map(w=>w.id)).size!==b.workers.length)fail('请扫描操作人员工牌 / 작업자 명찰을 스캔하세요');
 await ensureAttendance(e);
 const t=new Date().toISOString(),day=new Date(Date.parse(t)+9*3600000).toISOString().slice(0,10),out=[];
 const names=await rows(e,`SELECT d.worker_id,d.name FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.worker_id IN (SELECT value FROM json_each(?)) AND p.enabled=1 AND d.signed_out='' AND d.signed_in<=? AND (d.day=? OR EXISTS(SELECT 1 FROM v2_ops_job_workers s WHERE s.job_id=? AND s.worker_id=d.worker_id AND s.left_at='' AND date(s.joined_at,'+9 hours')=d.day)) AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='') ORDER BY d.day DESC`,JSON.stringify(b.workers.map(w=>w.id)),t,day,b.batch_id||'');
 for(const w of b.workers){const person=names.find(x=>x.worker_id===w.id);if(!person||person.name!==w.name)fail('工牌无效、未签到、已签退或休息中 / 명찰·출근 상태를 확인하세요');out.push({id:w.id,name:person.name});}
 if(out.length&&!out.some(w=>w.id===b.lead_id))fail('请选择主操作员 / 주 작업자를 선택하세요');
 return out;
}
export async function courierBatchOperation(e,b){
 const u=e.SOP_REQUEST_USER;if(!['manager','dispatcher'].includes(u?.role))fail('请由派审员操作 / 배정 담당자가 처리하세요');
 const operation=b.operation,request=String(b.client_req_id||'');if(!request||request.length>100)fail('缺少请求编号');
 const signature=JSON.stringify({operation,batch_id:b.batch_id||'',workers:b.workers,lead_id:b.lead_id,revision:b.revision});
 const replay=async()=>{const old=await q(e,'SELECT * FROM ck_courier_batch_requests WHERE request_id=?',request).first();if(!old)return null;if(old.actor_id!==u.id||old.signature!==signature)fail('同一请求的人员或批次已变化 / 요청 내용이 변경되었습니다');return {ok:true,batch:await courierBatchDetail(e,old.batch_id),replayed:true};};
 const previous=await replay();if(previous)return previous;
 if(operation==='people'){const prior=await q(e,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(prior){const after=JSON.parse(prior.after_json);if(prior.action!=='sop_native_people'||prior.actor_id!==u.id||prior.record_id!==b.batch_id||Number(b.revision)!==prior.expected_revision||JSON.stringify(after.workers)!==JSON.stringify(b.workers)||after.lead_id!==(b.workers.length?b.lead_id:after.lead_id))fail('同一请求的人员或批次已变化');return {ok:true,replayed:true,batch:await courierBatchDetail(e,b.batch_id)};}}
 if(operation==='start'){
  const workers=await staff(e,b),blocked=await guardAttendance({action:'sop_task_dispatch',workers},e);if(blocked)fail(blocked);
  const id='CB-'+crypto.randomUUID(),t=new Date().toISOString(),day=new Date(Date.parse(t)+9*3600000).toISOString().slice(0,10),result={worker_seg_id:'WS-'+crypto.randomUUID()},team={workers,lead_id:b.lead_id,department:'direct_ship',estimated_minutes:0};
  await ensureDispatchStartGuard(e);
  try{await e.DB.batch([
   q(e,`INSERT INTO ck_courier_batches(id,day,sequence,batch_no,created_at) SELECT ?,?,COALESCE(MAX(sequence),0)+1,?||'-'||printf('%02d',COALESCE(MAX(sequence),0)+1),? FROM ck_courier_batches WHERE day=?`,id,day,day,t,day),
   q(e,`INSERT INTO v2_ops_jobs(id,flow_stage,biz_class,job_type,related_doc_type,related_doc_id,status,created_by,created_at,updated_at,active_worker_count,display_no) SELECT ?,'inbound','direct_ship','courier_receiving','courier_batch',?,'working',?,?,?,?,batch_no FROM ck_courier_batches WHERE id=?`,id,id,u.id,t,t,workers.length,id),
   ...staffedDispatchStatements(e,{job_type:'courier_receiving',source_type:'courier_batch',source_id:id,client_req_id:request},team,id,t,result),
   q(e,"UPDATE sop_records SET state=json_set(state,'$.title','快递收货 / 택배 수령') WHERE id=?",id),
   q(e,'INSERT INTO ck_courier_batch_requests VALUES(?,?,?,?,?,?)',request,u.id,signature,id,operation,t)
  ]);}catch(error){const old=await replay();if(old)return old;throw error;}
  return {ok:true,batch:await courierBatchDetail(e,id)};
 }
 if(!await nativeOwner({job_id:b.batch_id},e))fail('请由本批派审员操作 / 이 차수의 담당자가 처리하세요');
 const batch=await courierBatchDetail(e,b.batch_id);
 if(operation==='people'){
  const workers=await staff(e,b,true),newWorkers=workers.filter(w=>!batch.workers.some(p=>p.id===w.id)),blocked=await guardAttendance({action:'sop_native_people',job_id:b.batch_id,workers:newWorkers},e);if(blocked)fail(blocked);
  await nativePeople({...b,action:'sop_native_people',job_id:b.batch_id,workers},e);
  return {ok:true,batch:await courierBatchDetail(e,b.batch_id)};
 }
 if(operation==='finish'){
  if(batch.status==='completed')return {ok:true,batch,already_completed:true};
  if(batch.revision!==Number(b.revision))fail('人员已更新，请刷新 / 인원 정보 변경, 새로고침하세요');
  const t=new Date().toISOString(),gate='EXISTS(SELECT 1 FROM ck_courier_batch_requests WHERE request_id=?)';
  try{await e.DB.batch([
   q(e,'INSERT INTO ck_courier_batch_requests VALUES(?,?,?,?,?,?)',request,u.id,signature,b.batch_id,operation,t),
   q(e,`UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='dispatcher_finish' WHERE job_id=? AND left_at='' AND ${gate}`,t,t,b.batch_id,request),
   q(e,`UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,finished_at=?,updated_at=? WHERE id=? AND ${gate}`,t,t,b.batch_id,request),
   q(e,`UPDATE sop_records SET state=json_set(state,'$.status','completed','$.finished_at',?),revision=revision+1,updated_at=? WHERE id=? AND ${gate}`,t,t,b.batch_id,request)
  ]);}catch(error){const old=await replay();if(old)return old;throw error;}
  if(!await replay())fail('批次或人员已变化，请刷新 / 차수 정보가 변경되었습니다');
  return {ok:true,batch:await courierBatchDetail(e,b.batch_id)};
 }
 fail('未知批次操作');
}
// This expression is used in the INSERT, so completion, checkout and crew changes
// racing with a scan are evaluated at commit, not at an earlier read.
export const courierScanGuard=`EXISTS(SELECT 1 FROM ck_courier_batches b JOIN v2_ops_jobs j ON j.id=b.id JOIN v2_ops_job_workers w ON w.job_id=j.id JOIN ck_attendance_days d ON d.worker_id=w.worker_id AND d.day=date(w.joined_at,'+9 hours') JOIN ck_attendance_people p ON p.id=d.person_id WHERE b.id=? AND j.status='working' AND w.worker_id=? AND w.left_at='' AND d.signed_out='' AND p.enabled=1 AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks br WHERE br.attendance_id=d.id AND br.ended_at=''))`;

export function courierCommitAccess(e,id){const a=dispatchAccess(e.SOP_REQUEST_USER);return {sql:`EXISTS(SELECT 1 FROM sop_records s WHERE s.id=? AND s.kind='dispatch' AND ${a.sql})`,args:[id,...a.args]};}
