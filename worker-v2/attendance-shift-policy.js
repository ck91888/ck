// Derived attendance accounting only. Never rewrite punches, work clocks or real breaks.
import {attendanceReport,kstDay} from './attendance-time.js';
const MINUTE=60000,DAY=86400000;
const windows=[{from:510,to:560,start:'09:00'},{from:570,to:620,start:'10:00'},{from:750,to:800,start:'13:00'}];
const validDay=value=>/^\d{4}-\d{2}-\d{2}$/.test(value||'')&&Number.isFinite(Date.parse(value+'T00:00:00+09:00'))&&new Date(value+'T00:00:00Z').toISOString().slice(0,10)===value;
export function shiftPolicyEnabled(env,day){return env.SOP_ENVIRONMENT==='staging'&&env.SOP_ATTENDANCE_SHIFT_ACCOUNTING==='true'&&validDay(day);}
export function attendanceShift(record){
 const raw=record.inAt,stamp=Date.parse(raw),day=record.day;
 if(!Number.isFinite(stamp)||!validDay(day)||kstDay(raw)!==day)return {version:'shift-accounting-v1',rawInAt:raw,effectiveInAt:raw,shift:null,restTargetMinutes:null,matched:false,valid:false};
 const local=new Date(stamp+9*3600000),minute=local.getUTCHours()*60+local.getUTCMinutes(),match=windows.find(w=>minute>=w.from&&minute<=w.to);
 return {version:'shift-accounting-v1',rawInAt:raw,effectiveInAt:match?new Date(day+'T'+match.start+':00+09:00').toISOString():raw,shift:match?.start||null,restTargetMinutes:minute<720?90:40,restBand:minute<720?'morning':'afternoon',matched:!!match,valid:true};
}
function occupiedOutside(record,segments,asOf,day,from,to){
 const rawFrom=Math.max(Date.parse(day+'T00:00:00+09:00'),Date.parse(record.inAt)),rawTo=Math.min(Date.parse(day+'T00:00:00+09:00')+DAY,Date.parse(asOf),record.outAt?Date.parse(record.outAt):Infinity),intervals=[],bounds=new Set([rawFrom,rawTo]);
 if(!Number.isFinite(rawFrom)||!Number.isFinite(rawTo)||rawTo<=rawFrom)return 0;
 const add=(start,end)=>{const s=Date.parse(start),z=end?Date.parse(end):Date.parse(asOf);if(!Number.isFinite(s)||!Number.isFinite(z)||z<s)return;const a=Math.max(rawFrom,s),b=Math.min(rawTo,z);if(b>a){intervals.push({a,b});bounds.add(a);bounds.add(b);}};
 for(const s of segments)if(s.badgeId===record.badgeId)add(s.start,s.end);
 for(const b of record.breaks||[])add(b.start,b.end);
 const sorted=[...bounds].sort((a,b)=>a-b);let outside=0;
 for(let i=1;i<sorted.length;i++){const a=sorted[i-1],b=sorted[i];if(!intervals.some(x=>x.a<b&&x.b>a))continue;const overlap=Math.max(0,Math.min(b,to)-Math.max(a,from));outside+=(b-a-overlap)/MINUTE;}
 return outside;
}
export function shiftAttendanceReport(record,segments,asOf,day=record.day,{needsWorkVerification=false}={}){
 const raw=attendanceReport(record,segments,asOf,day),policy=attendanceShift(record),accounting={...policy,rawPresenceMinutes:raw.presence,registeredRestMinutes:raw.rest,automaticRestMinutes:0,unassignedBeforeRest:raw.unassigned,restShortfallMinutes:null,retainedOutsideShiftMinutes:0,status:'pending_checkout',applied:false};
 const totals={...raw,flags:[...raw.flags],accounting},now=Date.parse(asOf),departure=record.outAt?Date.parse(record.outAt):now;
 if(!policy.valid||!Number.isFinite(now)||!Number.isFinite(departure)||departure<Date.parse(record.inAt)||departure>now){accounting.status='invalid_time';return totals;}
 if(day!==record.day||record.outAt&&kstDay(record.outAt)!==record.day||!record.outAt&&kstDay(asOf)>record.day){accounting.status='cross_day_pending';totals.flags.push('跨日班次待核实，暂不自动调整上岗或补休息 / 자정 경과 근무 확인 필요');return totals;}
 const from=Math.max(Date.parse(day+'T00:00:00+09:00'),Date.parse(policy.effectiveInAt)),to=Math.min(Date.parse(day+'T00:00:00+09:00')+DAY,departure,now),scheduled=Math.max(0,(to-from)/MINUTE),outside=occupiedOutside(record,segments,asOf,day,from,to),occupied=raw.bulk+raw.direct_ship+raw.import+raw.other+raw.rest+raw.conflict;
 accounting.scheduledPresenceMinutes=scheduled;accounting.retainedOutsideShiftMinutes=outside;accounting.applied=true;
 totals.presence=Math.max(scheduled+outside,occupied);totals.unassigned=Math.max(0,totals.presence-occupied);accounting.unassignedBeforeRest=totals.unassigned;
 if(outside>0)totals.flags.push('有效上岗前有已记录作业或休息，原时段保留并单列待核实 / 근무 시작 전 실제 기록 보존·확인 필요');
 if(!record.outAt)return totals;
 if(needsWorkVerification||raw.conflict>0){accounting.status='work_verification_pending';totals.flags.push('作业或休息归属待核实，暂不自动补休息 / 작업·휴식 확인 후 자동 보충');return totals;}
 const missing=Math.max(0,policy.restTargetMinutes-raw.rest),converted=Math.min(totals.unassigned,missing);
 totals.rest=raw.rest+converted;totals.unassigned=Math.max(0,totals.unassigned-converted);accounting.automaticRestMinutes=converted;accounting.restShortfallMinutes=Math.max(0,missing-converted);accounting.status=accounting.restShortfallMinutes>0?'insufficient_unassigned':'settled';
 if(accounting.restShortfallMinutes>0)totals.flags.push('未归属时间不足，仅补入现有时间；未扣作业 / 미분류 시간 범위만 휴식 전환·작업 차감 없음');
 return totals;
}
