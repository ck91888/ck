// Person-day projection of immutable punches and child-scoped labor. No source writes.
import {attendanceDaySelect,attendanceVoidBlockers} from './attendance-management.js';
import {attendanceReturnReport,attendanceSpanWindows} from './attendance-return-time.js';
import {kstDay} from './attendance-time.js';
import {shiftPolicyEnabled} from './attendance-shift-policy.js';
import {dispatchTimeEnabled} from './dispatch-time.js';
import {dispatchTimeAttendance} from './dispatch-time-runtime.js';
import {employeeWorkdayAccess,employeeWorkdays} from './employee-workday.js';
import {jobNumbers,jobDisplayMetadata} from './document-numbers.js';
import {laborDepartment} from '../shared/labor-department.js';
import {sameWorkerBadge} from '../shared/badge-identity.js';
import {publicAttendanceSpan} from './attendance-return.js';
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),rows=async(e,s,...a)=>(await q(e,s,...a).all()).results||[];
const parse=s=>{try{return JSON.parse(s||'{}');}catch{return {};}};
export async function attendanceReturnOverview(e,date,at,scope,detailPersonId,{publicDay,agencies}){
 if(scope==='employee')employeeWorkdayAccess(e);const start=new Date(date+'T00:00:00+09:00').toISOString(),end=new Date(Date.parse(start)+86400000).toISOString();
 const parents=await rows(e,attendanceDaySelect+` WHERE d.day=? OR EXISTS(SELECT 1 FROM ck_attendance_spans s WHERE s.attendance_id=d.id AND julianday(s.started_at)<julianday(?) AND (s.ended_at='' OR julianday(s.ended_at)>julianday(?))) ORDER BY d.signed_in,d.id`,date,end,start);
 const parentIds=JSON.stringify(parents.map(d=>d.id));const [spans,breaks,events,spanEvents]=await Promise.all([
 rows(e,'SELECT * FROM ck_attendance_spans WHERE attendance_id IN (SELECT value FROM json_each(?)) ORDER BY started_at,ordinal,id',parentIds),
 rows(e,'SELECT b.*,link.span_id FROM ck_attendance_breaks b LEFT JOIN ck_attendance_span_breaks link ON link.break_id=b.id WHERE b.attendance_id IN (SELECT value FROM json_each(?))',parentIds),
 rows(e,'SELECT * FROM ck_attendance_events WHERE record_id IN (SELECT value FROM json_each(?)) ORDER BY at,version',parentIds),
 rows(e,'SELECT * FROM ck_attendance_span_events WHERE attendance_id IN (SELECT value FROM json_each(?)) ORDER BY at,request_id',parentIds)
 ]);
 const people=await rows(e,'SELECT * FROM ck_attendance_people WHERE id IN (SELECT person_id FROM ck_attendance_spans WHERE attendance_id IN (SELECT value FROM json_each(?))) OR id IN (SELECT source_person_id FROM ck_attendance_spans WHERE attendance_id IN (SELECT value FROM json_each(?)))',parentIds,parentIds),byPerson=new Map(people.map(p=>[p.id,p]));
 const duties=await dispatchTimeAttendance(e,date,at),blockers=await attendanceVoidBlockers(e,date),grouped=new Map();
 for(const s of spans){const p=byPerson.get(s.person_id),employee=p?.badge_id?.startsWith('EMP-');if(scope==='employee'&&!employee||scope!=='employee'&&scope!=='all'&&employee)continue;if(!grouped.has(s.person_id))grouped.set(s.person_id,[]);grouped.get(s.person_id).push(s);}
 const tables=new Set((await rows(e,"SELECT name FROM sqlite_master WHERE type='table'")).map(x=>x.name)),hasRecords=tables.has('sop_records');
 let segments=await rows(e,`SELECT w.*,link.span_id,j.biz_class,j.job_type,j.status AS job_status,j.display_no,${hasRecords?"s.state AS assignment_state,s.department AS assigned_department,s.kind AS assignment_kind":"'{}' AS assignment_state,'' AS assigned_department,'' AS assignment_kind"} FROM v2_ops_job_workers w LEFT JOIN ck_attendance_span_workers link ON link.segment_id=w.id LEFT JOIN v2_ops_jobs j ON j.id=w.job_id ${hasRecords?"LEFT JOIN sop_records s ON s.id=j.id AND s.kind IN ('task','dispatch')":''} WHERE julianday(w.joined_at)<julianday(?) AND (w.left_at='' OR julianday(w.left_at)>julianday(?))`,end,start);
 if(tables.has('sop_document_numbers'))segments=await jobNumbers(e,segments,'job_id');for(const s of segments)s.labor_department=parse(s.assignment_state).labor_department||'';
 const items=[],deletedItems=[],validSegments=new Map();
 for(const [personId,allSpans] of grouped){
  const ids=new Set(allSpans.map(s=>s.attendance_id)),source=parents.filter(d=>ids.has(d.id)),validParents=source.filter(d=>!d.voided_at),validIds=new Set(validParents.map(d=>d.id)),p=byPerson.get(personId),selected=validParents.length?allSpans.filter(s=>validIds.has(s.attendance_id)):allSpans,last=selected.at(-1),anchor=source.find(d=>d.id===last.attendance_id)||source[0];if(!anchor)continue;
  const record={...publicDay(anchor),personId,stablePersonId:personId,badgeId:p?.badge_id||anchor.worker_id,day:date,attendanceIds:[...(validParents.length?validIds:ids)]},badIdentity=selected.some(s=>!byPerson.get(s.source_person_id)||!sameWorkerBadge(byPerson.get(s.source_person_id).badge_id,s.badge_id));
  const br=breaks.filter(b=>record.attendanceIds.includes(b.attendance_id)).map(b=>({id:b.id,start:b.started_at,end:b.ended_at,attendanceSpanId:b.span_id||''}));record.breaks=br;
  const own=segments.filter(s=>selected.some(w=>sameWorkerBadge(w.badge_id,s.worker_id))).map(s=>({id:s.id,jobId:s.job_id,jobNo:s.display_business_no||s.job_label,...jobDisplayMetadata(s),jobType:s.job_type,jobStatus:s.job_status,badgeId:s.worker_id,sourceWorkerId:s.worker_id,department:laborDepartment(s),start:s.joined_at,end:s.left_at,reason:s.leave_reason,attendanceSpanId:s.span_id||''}));
  const ownDuty=source.map(d=>duties.get(d.id)).filter(Boolean),dispatchDuty=dispatchTimeEnabled(e)&&ownDuty.length?{...ownDuty.at(-1),segments:ownDuty.flatMap(d=>d.segments),seconds:ownDuty.reduce((n,d)=>n+d.seconds,0),pendingSeconds:ownDuty.reduce((n,d)=>n+d.pendingSeconds,0)}:null;
  const totals=attendanceReturnReport(record,badIdentity?[]:own,at,date,{attendanceSpans:selected,dispatchSegments:(dispatchDuty?.segments||[]).map(s=>({...s,badgeId:record.badgeId})),shiftPolicy:shiftPolicyEnabled(e,date),needsWorkVerification:badIdentity});
  if(!totals.presence&&!totals.attendanceSpans.some(s=>kstDay(s.start)===date)&&!source.some(d=>d.day===date))continue;
  if(badIdentity){totals.bulk=totals.direct_ship=totals.import=totals.other=totals.unassigned=totals.dispatch=0;totals.conflict=totals.presence-totals.rest;totals.flags.push('出勤子段与工牌身份不一致，工时待核实');}
  const windows=attendanceSpanWindows(record,selected,at).windows,current=windows.find(w=>!w.rawEnd&&!w.pending&&w.start<=Date.parse(at)&&w.end>=Date.parse(at)),live=badIdentity?[]:own.filter(s=>!s.end&&current&&(s.attendanceSpanId?s.attendanceSpanId===current.id:Date.parse(s.start)>=current.start)&&['working','pending','awaiting_close'].includes(s.jobStatus));
  const occupancyWarnings=date===kstDay(at)?own.filter(s=>!s.end&&!live.includes(s)).map(s=>({...s,jobState:s.jobStatus,startedAtLabel:s.start,reason:'作业不属于当前有效出勤段，请核实原记录'})):[];
  const activeSpan=current?selected.find(s=>s.id===current.id):null,resting=!!current&&br.some(b=>!b.end&&Date.parse(b.start)>=current.start);
  record.inAt=selected[0].started_at;record.outAt=activeSpan?'':last.ended_at;record.attendanceSpans=selected;record.originalRecords=source.map(publicDay);record.activeSpan=activeSpan?publicAttendanceSpan(activeSpan):null;
  const status=badIdentity?'identity_conflict':activeSpan?resting?'rest':occupancyWarnings.length?'occupancy_conflict':live.length?'working':dispatchDuty?.status==='dispatching'?'dispatching':'waiting_dispatch':last.ended_at?'out':'dispatch_pending';
  validSegments.set(personId,badIdentity?[]:own);
  const returned=selected.some(s=>s.kind!=='normal'||s.ordinal>1),item={record,totals,attendanceSpans:selected.map(s=>({...publicAttendanceSpan(s),totals:totals.spanTotals.find(v=>v.id===s.id)||null})),spanEvents:spanEvents.filter(ev=>ids.has(ev.attendance_id)).map(ev=>({...ev,before:parse(ev.before_json),after:parse(ev.after_json)})),segments:own,breaks:br,status,identityConflict:badIdentity,occupancyWarnings,currentJobs:live.map(s=>({id:s.jobId,no:s.jobNo,...jobDisplayMetadata({...s,job_type:s.jobType,job_department:s.department}),department:s.department})),canVoid:!badIdentity&&!returned&&!dispatchDuty&&!p?.badge_id?.startsWith('EMP-')&&!blockers.has(anchor.id),voidBlockedReason:returned?'已有返岗或更正续作事实，不能删除签到':dispatchDuty?'已有派审劳动记录，不能删除签到':blockers.get(anchor.id)||'',events:events.filter(ev=>ids.has(ev.record_id)).map(ev=>({action:ev.action,actor:ev.actor,actorName:parse(ev.after_json).audit_actor_name||ev.actor,at:ev.at,before:parse(ev.before_json),after:parse(ev.after_json)})),...(dispatchDuty?{dispatchDuty}:{})};
  (validParents.length?items:deletedItems).push(item);
 }
 if(scope==='employee'){const details=await employeeWorkdays(e,{date,asOf:at,items:detailPersonId?items.filter(x=>x.record.personId===detailPersonId):items,validSegments,detailPersonId});if(detailPersonId){const detail=details.get(detailPersonId);if(!detail)throw Error('没有该职员当天出勤');const item=items.find(x=>x.record.personId===detailPersonId);return {...detail,attendanceSpans:item.attendanceSpans,spanEvents:item.spanEvents};}}
 return {ok:true,date,asOf:at,agencies,items,deletedItems:e.SOP_REQUEST_USER?.role==='manager'?deletedItems:[],stale:[],identityWarnings:[],attendanceReturnsEnabled:true,...(dispatchTimeEnabled(e)?{dispatchTimeEnabled:true}:{}),...(shiftPolicyEnabled(e,date)?{attendancePolicy:{version:'attendance-spans-v1',timeZone:'Asia/Seoul',returnRule:'actual_rest_only',normalQuota:'once_per_original_shift'}}:{})};
}
