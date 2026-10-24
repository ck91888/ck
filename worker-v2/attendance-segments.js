// Decide whether an open clock belongs to this attendance record without changing
// its original start. Effective attendance minutes are clipped by attendanceReport.
import {kstDay} from './attendance-time.js';
import {sameWorkerBadge} from '../shared/badge-identity.js';
const parsed=value=>{try{return typeof value==='string'?JSON.parse(value):value||{};}catch{return {};}};
const fields=record=>({id:record.id,worker:record.worker_id??record.badgeId,day:record.day,inAt:record.signed_in??record.inAt});

export function attendanceOpenSegment(record,segment,history,asOf,currentCorrection=null){
 const r=fields(record),start=Date.parse(segment.joined_at??segment.start),arrival=Date.parse(r.inAt),now=Date.parse(asOf);
 if(!sameWorkerBadge(segment.worker_id??segment.badgeId,r.worker)||segment.left_at||segment.end||![start,arrival,now].every(Number.isFinite)||start>now)return {eligible:false,reason:'invalid'};
 const events=(history||[]).filter(e=>e.record_id===r.id&&['sop_attendance_correct','sop_attendance_checkout'].includes(e.action)).map(e=>({...e,before:parsed(e.before_json),after:parsed(e.after_json)}));
 const identity=e=>e.after.id===r.id&&sameWorkerBadge(e.after.worker_id,r.worker)&&e.after.day===r.day;
 // Clearing a previously confirmed departure must not revive a leftover old clock.
 if(events.some(e=>identity(e)&&e.after.signed_out&&Number.isFinite(Date.parse(e.after.signed_out))&&Date.parse(e.at)>=start&&Date.parse(e.at)<=now))return {eligible:false,reason:'closed_shift'};
 if(start>=arrival)return {eligible:true,corrected:false};
 if(kstDay(start)!==r.day)return {eligible:false,reason:'previous_shift'};
 const candidates=currentCorrection?[...events,{...currentCorrection,before:parsed(currentCorrection.before_json),after:parsed(currentCorrection.after_json)}]:events;
 const proof=candidates.some(e=>e.action==='sop_attendance_correct'&&e.record_id===r.id&&identity(e)&&e.before.id===r.id&&sameWorkerBadge(e.before.worker_id,r.worker)&&e.before.day===r.day&&!e.before.signed_out&&Date.parse(e.at)>=start&&Date.parse(e.at)<=now&&Date.parse(e.before.signed_in)<=start&&Date.parse(e.after.signed_in)>start&&kstDay(e.before.signed_in)===r.day&&kstDay(e.after.signed_in)===r.day);
 return proof?{eligible:true,corrected:true}:{eligible:false,reason:'unverified_before_checkin'};
}
