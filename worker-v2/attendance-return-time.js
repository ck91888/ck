// Derived multi-span accounting. Raw punches, operation clocks and approvals are never rewritten.
import {attendanceReport,kstDay} from './attendance-time.js';
import {attendanceShift} from './attendance-shift-policy.js';
import {sameWorkerBadge} from '../shared/badge-identity.js';
const MINUTE=60000,DAY=86400000,iso=n=>new Date(n).toISOString();
const fields=['presence','bulk','direct_ship','import','other','rest','unassigned','conflict','dispatch','systemCutoffMinutes','dispatchPendingMinutes','attendancePendingMinutes'];
const empty=()=>Object.fromEntries(fields.map(k=>[k,0]));
export const nextAttendanceCutoff=start=>Math.floor((Date.parse(start)+5*3600000)/DAY)*DAY-5*3600000+DAY;
const first=(s,a,b)=>s[a]??s[b];
export function attendanceSpanWindows(record,spans,asOf){
 const now=Date.parse(asOf);if(!Number.isFinite(now))throw Error('出勤统计时间无效');
 const source=spans===undefined?[{id:'legacy:'+record.id,attendance_id:record.id,person_id:record.stablePersonId||record.personId,badge_id:record.badgeId,shift_id:record.id,kind:'normal',started_at:record.inAt,ended_at:record.outAt,status:record.outAt?'closed':'open',overtime_state:'none'}]:spans;
 const parents=new Set(record.attendanceIds||[record.id]),person=record.stablePersonId||record.personId,flags=[],windows=[];
 for(const s of source){
  if(s.attendance_id&&!parents.has(s.attendance_id)||s.person_id&&person&&s.person_id!==person)continue;
  const start=Date.parse(first(s,'started_at','start')),rawEnd=first(s,'ended_at','end')||'',end=rawEnd?Date.parse(rawEnd):Math.min(now,nextAttendanceCutoff(first(s,'started_at','start')));
  if(!Number.isFinite(start)||!Number.isFinite(end)||end<start){flags.push('出勤子段时间异常，待核实');continue;}
  const pending=s.status==='pending_review'||!rawEnd&&now>=nextAttendanceCutoff(first(s,'started_at','start'));
  const provisional=Date.parse(s.provisional_end_at||'');
  const until=Math.min(end,now,pending&&Number.isFinite(provisional)?provisional:Infinity);
  if(until<=start)continue;
  windows.push({id:s.id,attendanceId:s.attendance_id||record.id,personId:person,badgeId:s.badge_id||record.badgeId,shiftId:s.shift_id||s.attendance_id||record.id,kind:s.kind||'normal',overtimeState:s.overtime_state||'none',start,end:until,rawEnd,closed:!!rawEnd&&end<=now,pending});
 }
 return {windows:windows.sort((a,b)=>a.start-b.start||a.end-b.end),flags};
}
function clipped(rows,a,b,badgeId){return rows.flatMap(s=>{const start=Math.max(a,Date.parse(s.start)),end=Math.min(b,s.end?Date.parse(s.end):b);return Number.isFinite(start)&&Number.isFinite(end)&&end>start?[{...s,badgeId,start:iso(start),end:iso(end)}]:[];});}
function belongs(s,record,windows){
 if(s.personId&&s.personId!==(record.stablePersonId||record.personId))return false;
 return sameWorkerBadge(s.badgeId,record.badgeId)||windows.some(w=>sameWorkerBadge(s.badgeId,w.badgeId));
}
// One normal shift quota; return spans use only real registered breaks. Every
// report day is clipped separately, never first-in to last-out across an absence.
export function attendanceReturnReport(record,operations,asOf,day=record.day,{attendanceSpans,dispatchSegments=[],shiftPolicy=false,needsWorkVerification=false}={}){
 const {windows,flags}=attendanceSpanWindows(record,attendanceSpans,asOf),now=Date.parse(asOf),totals=empty(),bounds=new Set(),groups=new Map(),cells=[];
 const ops=operations.filter(s=>belongs(s,record,windows)),duties=dispatchSegments.filter(s=>belongs(s,record,windows)),breaks=(record.breaks||[]).map(b=>({...b,badgeId:record.badgeId}));
 for(const w of windows){bounds.add(w.start);bounds.add(w.end);if(w.kind!=='return'){if(!groups.has(w.shiftId))groups.set(w.shiftId,{id:w.shiftId,windows:[],policy:attendanceShift({day:kstDay(w.start),inAt:iso(w.start)}),status:'pending_checkout',automaticRestMinutes:0});groups.get(w.shiftId).windows.push(w);}}
 for(const s of [...ops,...duties,...breaks]){const a=Date.parse(s.start),b=s.end?Date.parse(s.end):now;if(Number.isFinite(a))bounds.add(a);if(Number.isFinite(b))bounds.add(b);}
 for(const g of groups.values())if(shiftPolicy)bounds.add(Date.parse(g.policy.effectiveInAt));
 if(windows.length){const lo=windows[0].start,hi=Math.max(...windows.map(w=>w.end));for(let midnight=Date.parse(kstDay(lo)+'T00:00:00+09:00')+DAY;midnight<hi;midnight+=DAY)bounds.add(midnight);}
 const sorted=[...bounds].filter(Number.isFinite).sort((a,b)=>a-b);
 for(let i=1;i<sorted.length;i++){
  const a=sorted[i-1],b=sorted[i],active=windows.filter(w=>w.start<b&&w.end>a);if(!active.length||b<=a)continue;
  const pending=active.some(w=>w.pending)||active.length>1,normal=active.filter(w=>w.kind!=='return'),groupIds=new Set(normal.map(w=>w.shiftId)),groupId=groupIds.size===1?[...groupIds][0]:'';
  const base=attendanceReport({...record,day:kstDay(a),inAt:iso(a),outAt:iso(b),breaks:clipped(breaks,a,b,record.badgeId)},clipped(ops,a,b,record.badgeId),iso(b),kstDay(a),clipped(duties,a,b,record.badgeId));
  const values={...empty(),...base},cellFlags=[...base.flags];
  if(active.length>1)cellFlags.push('出勤子段重叠，按个人合并并标待核实');
  if(pending){values.attendancePendingMinutes=values.unassigned;values.conflict+=values.unassigned;values.unassigned=0;cellFlags.push('出勤截止或身份待核实，不补入劳动或休息');}
  cells.push({start:a,end:b,day:kstDay(a),groupId,returnOnly:normal.length===0,pending,values,registeredRestMinutes:values.rest,automaticRestMinutes:0,flags:cellFlags,virtual:false});
 }
 const rawPresence=cells.filter(c=>c.day===day).reduce((n,c)=>n+c.values.presence,0);
 if(shiftPolicy)for(const g of groups.values()){
  const own=cells.filter(c=>c.groupId===g.id),firstSpan=g.windows[0],p=g.policy,effective=Date.parse(p.effectiveInAt),crossDay=g.windows.some(w=>kstDay(w.start)!==kstDay(firstSpan.start)||kstDay(w.end)!==kstDay(firstSpan.start));
  if(!p.valid){g.status='invalid_time';continue;}
  if(crossDay){g.status='cross_day_pending';for(const c of own)c.flags.push('跨日原班次待核实，暂不自动补休息');continue;}
  if(effective<firstSpan.start){const b=Math.min(firstSpan.start,now),earlierAttendance=windows.some(w=>w.id!==firstSpan.id&&w.start<firstSpan.start&&kstDay(w.end)===kstDay(firstSpan.start));if(b>effective&&!earlierAttendance)own.push({start:effective,end:b,day:kstDay(effective),groupId:g.id,returnOnly:false,pending:false,values:{...empty(),presence:(b-effective)/MINUTE,unassigned:(b-effective)/MINUTE},registeredRestMinutes:0,automaticRestMinutes:0,flags:[],virtual:true});}
  else if(effective>firstSpan.start)for(const c of own.filter(c=>c.start<effective)){const removed=c.values.unassigned;c.values.unassigned=0;c.values.presence-=removed;if(c.values.presence>0)c.flags.push('有效上岗前已记录劳动或休息保留');}
  for(const c of own)if(!cells.includes(c))cells.push(c);
  if(g.windows.some(w=>!w.closed)){g.status='pending_checkout';continue;}
  if(needsWorkVerification||own.some(c=>c.pending||c.values.conflict>0)){g.status='work_verification_pending';continue;}
  const registered=own.reduce((n,c)=>n+c.registeredRestMinutes,0);let missing=Math.max(0,p.restTargetMinutes-registered);
  for(const c of [...own].sort((a,b)=>a.start-b.start)){const add=Math.min(missing,c.values.unassigned);c.values.unassigned-=add;c.values.rest+=add;c.automaticRestMinutes+=add;g.automaticRestMinutes+=add;missing-=add;}
  g.status=missing>1e-7?'insufficient_unassigned':'settled';
 }
 const selected=cells.filter(c=>c.day===day);for(const c of selected){for(const key of fields)totals[key]+=c.values[key]||0;flags.push(...c.flags);}
 const selectedRaw=windows.flatMap(w=>{const a=Math.max(w.start,Date.parse(day+'T00:00:00+09:00')),b=Math.min(w.end,Date.parse(day+'T00:00:00+09:00')+DAY);return b>a?[{a,b}]:[];}),extent=selectedRaw.length?(Math.max(...selectedRaw.map(w=>w.b))-Math.min(...selectedRaw.map(w=>w.a)))/MINUTE:0;
 const relevant=[...groups.values()].filter(g=>selected.some(c=>c.groupId===g.id)),automaticRestMinutes=selected.reduce((n,c)=>n+c.automaticRestMinutes,0);
 return {...totals,flags:[...new Set(flags)],attendanceSpans:windows.map(w=>({id:w.id,attendanceId:w.attendanceId,shiftId:w.shiftId,kind:w.kind,start:iso(w.start),end:w.rawEnd||'',provisionalEnd:w.pending?iso(w.end):'',needsVerification:w.pending,overtimeState:w.overtimeState})),offShiftGapMinutes:Math.max(0,extent-rawPresence),accounting:{version:'attendance-spans-v1',rawPresenceMinutes:rawPresence,registeredRestMinutes:selected.reduce((n,c)=>n+c.registeredRestMinutes,0),automaticRestMinutes,returnRule:'actual_rest_only',status:!relevant.length?'return_only':relevant.some(g=>g.status!=='settled')?relevant.find(g=>g.status!=='settled').status:'settled',groups:relevant.map(g=>({shiftId:g.id,restTargetMinutes:g.policy.restTargetMinutes,status:g.status,automaticRestMinutes:g.automaticRestMinutes})),applied:shiftPolicy}};
}
