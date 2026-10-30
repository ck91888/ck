import {ensureSchema} from './schema-ready.js';
import {nativeOwner} from './sop-dispatch.js';
import {dispatchAccess} from './dispatch-access.js';
import {workerBadgeMatch} from './daily-badge-identity.js';
import {guardAttendance} from './attendance.js';
import {isAttendanceBadge} from '../shared/badge-identity.js';
import {activeAttendanceSQL} from './attendance-management.js';

const q=(e,s,...a)=>e.DB.prepare(s).bind(...a);
const rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const required=(value,label)=>{const s=String(value??'').trim();if(!s||s.length>256)throw Error(label+'无效 / 입력값을 확인하세요');return s;};
export const checkWorkbenchEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_UPGRADE_ENABLED==='true';
async function schema(e){
 await ensureSchema(e.DB,'check-workbench-v1',()=>e.DB.batch([
  q(e,`CREATE TABLE IF NOT EXISTS ck_verify_requests(request_id TEXT PRIMARY KEY,signature TEXT NOT NULL,response_json TEXT NOT NULL,created_at TEXT NOT NULL,allowed INTEGER NOT NULL CHECK(allowed=1))`)
 ]));
}
async function replay(e,key,signature){
 const old=await q(e,'SELECT * FROM ck_verify_requests WHERE request_id=?',key).first();
 if(!old)return null;
 if(old.signature!==signature)throw Error('请求编号已用于其他扫码或人员，请核实 / 요청번호가 다른 작업에 사용되었습니다');
 return JSON.parse(old.response_json);
}
const summary=d=>{const active=d.items.filter(x=>!x.removed),planned=active.reduce((n,x)=>n+x.qty,0),scanned=active.reduce((n,x)=>n+x.scanned,0);return {planned_total_box_count:planned,scanned_ok_total_count:scanned,abnormal_count:d.exceptions.filter(x=>!x.resolved).length,diff:planned-scanned};};

// The request receipt, scan log, and authoritative date checklist are committed
// together. Revision/count guards serialize concurrent devices; a lost response
// can be replayed using the same request ID without scanning the box twice.
export async function scanCheck(b,e){
 await schema(e);
 const u=e.SOP_REQUEST_USER,key=required(b.client_req_id,'请求编号'),jobId=required(b.job_id,'任务'),batchId=required(b.batch_id,'批次'),barcode=required(b.barcode,'条码'),pallet=required(b.pallet_no,'托盘号'),workerId=required(b.worker_id,'操作员工牌');
 const signature=JSON.stringify({action:b.action,actor:u?.id,scope:u?.scope,jobId,batchId,barcode,pallet,workerId});
 const cached=await replay(e,key,signature);if(cached)return cached;
 for(let attempt=0;attempt<8;attempt++){
  if(!await nativeOwner({job_id:jobId},e))throw Error('仅本任务派审员或在岗人员可扫码 / 담당 작업자만 스캔할 수 있습니다');
  const job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',jobId).first();
  if(!job||job.job_type!=='verify_scan'||job.related_doc_type!=='verify_batch'||job.related_doc_id!==batchId)throw Error('任务与核对批次不一致 / 작업과 배치가 일치하지 않습니다');
  if(job.status!=='working')throw Error('任务未作业或已暂停，请先恢复 / 작업을 먼저 재개하세요');
  const worker=await q(e,`SELECT worker_id,worker_name FROM v2_ops_job_workers WHERE job_id=? AND ${workerBadgeMatch('worker_id','?')} AND left_at=''`,jobId,workerId).first();
  if(!worker)throw Error('扫码人员不在岗，请先结束休息或调整人员 / 작업 인원을 확인하세요');
  const attendanceError=await guardAttendance({action:'sop_native_resume',job_id:jobId,workers:[{id:worker.worker_id,name:worker.worker_name}]},e);if(attendanceError)throw Error(attendanceError);
  const batch=await q(e,'SELECT * FROM v2_verify_batches WHERE id=?',batchId).first();
  if(!batch||!['pending','verifying'].includes(batch.status))throw Error('核对批次已关闭或取消 / 배치가 종료되었습니다');
  const check=await q(e,"SELECT * FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?",batchId).first();
  const counts=await q(e,"SELECT COUNT(*) AS total,SUM(scan_result='ok') AS ok,SUM(scan_result!='ok') AS abnormal,SUM(barcode=? AND scan_result='ok') AS barcode_ok FROM v2_verify_scan_logs WHERE batch_id=?",barcode,batchId).first();
  const data=check?JSON.parse(check.state):null;
  if(data&&data.status!=='open')throw Error('日期清单已关闭，请负责人核实 / 목록이 종료되었습니다');
  const item=data?data.items.find(x=>!x.removed&&x.barcode===barcode):await q(e,'SELECT * FROM v2_verify_batch_items WHERE batch_id=? AND barcode=?',batchId,barcode).first();
  const planned=item?Number(data?item.qty:item.planned_box_count||item.planned_qty||1):0,before=item?Number(data?item.scanned:counts.barcode_ok||0):0;
  const scanResult=!item?'not_found':before>=planned?'overflow':'ok',t=new Date().toISOString(),logId='SCAN-'+crypto.randomUUID();
  const message={ok:'核对成功 / 확인 완료',overflow:'超出计划箱数，已记异常 / 계획 수량 초과, 이상 기록',not_found:'条码不在当前有效清单，已记异常 / 유효 목록에 없는 바코드'}[scanResult];
  if(data){if(scanResult==='ok'){item.scanned++;item.pallets.push(pallet);item.recheck=item.scanned!==item.qty;}else data.exceptions.push({id:'EX-'+crypto.randomUUID(),barcode,pallet,type:scanResult,at:t,by:worker.worker_name,worker_id:worker.worker_id,job_id:jobId,resolved:false});}
  const result={ok:true,scan_result:scanResult,message,log_id:logId,barcode_info:{barcode,customer_name:item?(data?item.customer:item.customer_name)||'':'',planned_box_count:planned,scanned_ok_count:before+(scanResult==='ok'?1:0)},summary:data?summary(data):{planned_total_box_count:batch.planned_qty,scanned_ok_total_count:Number(counts.ok||0)+(scanResult==='ok'?1:0),abnormal_count:Number(counts.abnormal||0)+(scanResult==='ok'?0:1),diff:batch.planned_qty-Number(counts.ok||0)-(scanResult==='ok'?1:0)},log:{id:logId,barcode,pallet_no:pallet,worker_id:worker.worker_id,worker_name:worker.worker_name,scan_result:scanResult,message,scanned_at:t},...(check?{check_id:check.id,revision:check.revision+1}:{})};
  const access=dispatchAccess(u),attendance=e.SOP_ATTENDANCE_ENABLED==='true'&&isAttendanceBadge(worker.worker_id);
  const sql=[q(e,`INSERT INTO ck_verify_requests VALUES(?,?,?,?,CASE WHEN
   EXISTS(SELECT 1 FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE j.id=? AND j.status='working' AND s.kind='dispatch' AND ${access.sql})
   AND EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at='')
   ${attendance?`AND EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE ${workerBadgeMatch('d.worker_id','?')} AND d.day=date(?,'+9 hours') AND d.signed_out='' AND p.enabled=1 AND ${activeAttendanceSQL('d')} AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at=''))`:''}
   AND EXISTS(SELECT 1 FROM v2_verify_batches WHERE id=? AND status IN ('pending','verifying') AND updated_at=?)
   AND (SELECT COUNT(*) FROM v2_verify_scan_logs WHERE batch_id=?)=?
   AND (SELECT COUNT(*) FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?)=? THEN 1 ELSE 0 END)`,key,signature,JSON.stringify(result),t,jobId,...access.args,jobId,worker.worker_id,...(attendance?[worker.worker_id,t]:[]),batchId,batch.updated_at,batchId,counts.total,batchId,check?1:0)];
  if(check)sql.push(q(e,'INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)','VERIFY-'+key,check.id,check.revision,'sop_check_scan',u.id,u.name,check.state,JSON.stringify(data),JSON.stringify(result),t),q(e,'UPDATE sop_records SET state=?,revision=revision+1,updated_at=? WHERE id=?',JSON.stringify(data),t,check.id));
  sql.push(q(e,'INSERT INTO v2_verify_scan_logs(id,batch_id,job_id,worker_id,worker_name,pallet_no,barcode,scan_result,message,scanned_at) VALUES(?,?,?,?,?,?,?,?,?,?)',logId,batchId,jobId,worker.worker_id,worker.worker_name,pallet,barcode,scanResult,message,t),q(e,"UPDATE v2_verify_batches SET status='verifying',updated_at=? WHERE id=?",t,batchId));
  try{await e.DB.batch(sql);return result;}catch(error){const saved=await replay(e,key,signature);if(saved)return saved;if(!/revision_conflict|CHECK constraint failed: allowed/.test(error.message))throw error;}
 }
 throw Error('批次正在被其他人员更新，本条尚未提交，请重试 / 다른 작업과 충돌했습니다. 다시 시도하세요');
}

export async function finishCheck(b,e){
 await schema(e);
 const u=e.SOP_REQUEST_USER,id=required(b.job_id,'任务'),key=required(b.client_req_id,'请求编号');
 const signature=JSON.stringify({action:b.action,actor:u?.id,scope:u?.scope,id,complete_batch:!!b.complete_batch,remark:String(b.remark||'')});
 const cached=await replay(e,key,signature);if(cached)return cached;
 if(!await nativeOwner({job_id:id},e))throw Error('请由本任务派审员收尾 / 담당자가 종료하세요');
 const job=await q(e,'SELECT * FROM v2_ops_jobs WHERE id=?',id).first();
 if(job?.job_type!=='verify_scan')throw Error('任务类型不符');
 if(job.status==='completed')return {ok:true,already_completed:true,job_id:id};
 if(!['working','awaiting_close'].includes(job.status)||b.complete_job===false||b.leave_only)throw Error('请先恢复任务；人员离开请使用调整人员 / 작업을 재개한 뒤 종료하세요');
 const check=await q(e,"SELECT id FROM sop_records WHERE kind='check' AND json_extract(state,'$.legacy_id')=?",job.related_doc_id).first();
 if(check&&b.complete_batch)throw Error('日期清单须由经理或复核人员最终交接；可先结束本次作业 / 최종 인계는 승인 담당자가 처리하세요');
 const note=String(b.remark||'').trim();if(note.length>2000)throw Error('交接说明过长');
 const t=new Date().toISOString(),result={ok:true,job_id:id,completed:true,batch_completed:!!b.complete_batch},access=dispatchAccess(u);
 const sql=[q(e,`INSERT INTO ck_verify_requests VALUES(?,?,?,?,CASE WHEN EXISTS(SELECT 1 FROM v2_ops_jobs j JOIN sop_records s ON s.id=j.id WHERE j.id=? AND j.status IN ('working','awaiting_close') AND ${access.sql}) THEN 1 ELSE 0 END)`,key,signature,JSON.stringify(result),t,id,...access.args),
  q(e,`INSERT INTO v2_ops_job_results(id,job_id,box_count,remark,result_json,created_by,created_at) SELECT ?,?,0,?,json_object('kind','check_workbench','scope','whole_task','batch_id',?,'task_scanned_ok_count',COALESCE(SUM(scan_result='ok'),0),'task_abnormal_count',COALESCE(SUM(scan_result!='ok'),0),'handover_note',?),?,? FROM v2_verify_scan_logs WHERE job_id=?`,'RES-VERIFY-'+id,id,note,job.related_doc_id,note,u.id,t,id),
  q(e,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*14400)/10.0),leave_reason='dispatcher_finish' WHERE job_id=? AND left_at=''",t,t,id),
  q(e,"UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,finished_at=?,updated_at=? WHERE id=?",t,t,id)];
 if(b.complete_batch)sql.push(q(e,"UPDATE v2_verify_batches SET status='completed',completed_at=?,completed_by=?,updated_at=? WHERE id=?",t,u.name,t,job.related_doc_id));
 try{await e.DB.batch(sql);return result;}catch(error){const saved=await replay(e,key,signature);if(saved)return saved;throw error;}
}
