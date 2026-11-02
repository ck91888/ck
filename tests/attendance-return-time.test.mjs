import test from 'node:test';import assert from 'node:assert/strict';
import {attendanceReturnReport,nextAttendanceCutoff} from '../worker-v2/attendance-return-time.js';
import {shiftAttendanceReport} from '../worker-v2/attendance-shift-policy.js';
import {database} from './d1-adapter.mjs';
import {ensureAttendanceReturnSchema} from '../worker-v2/attendance-return-schema.js';
const day='2026-11-02',at=t=>day+'T'+t+':00+09:00',record={id:'SYNTH-ATT',personId:'SYNTH-PERSON',badgeId:'EMP-SYNTH',day,inAt:at('09:00'),outAt:at('18:00'),breaks:[]};
const span=(id,a,b,kind='normal',extra={})=>({id,attendance_id:record.id,person_id:record.personId,badge_id:record.badgeId,shift_id:'SYNTH-SHIFT',kind,started_at:at(a),ended_at:b?at(b):'',status:b?'closed':'open',overtime_state:kind==='return'?'pending':'none',...extra});
const op=(a,b,department='bulk',extra={})=>({badgeId:record.badgeId,start:at(a),end:b?at(b):'',department,...extra});
const rest=(a,b)=>({start:at(a),end:b?at(b):''});
const conserved=t=>assert.ok(Math.abs(t.presence-t.bulk-t.direct_ship-t.import-t.other-t.rest-t.unassigned-t.conflict-t.dispatch)<1e-7,JSON.stringify(t));
const report=(spans,ops=[],options={},r=record,asOf=at('23:50'),date=day)=>{const t=attendanceReturnReport(r,ops,asOf,date,{attendanceSpans:spans,...options});conserved(t);return t;};
test('normal checkout then return preserves both punches; departure gap is neither attendance nor rest',()=>{
 const spans=[span('ONE','09:00','18:00'),span('TWO','21:00','23:00','return')],before=structuredClone(spans),t=report(spans,[op('09:00','12:00'),op('21:00','22:00')]);assert.equal(t.presence,660);assert.equal(t.bulk,240);assert.equal(t.offShiftGapMinutes,180);assert.equal(t.rest,0);assert.deepEqual(spans,before);assert.equal(t.attendanceSpans[0].end,at('18:00'));assert.equal(t.attendanceSpans[1].overtimeState,'pending');
});
test('normal quota is applied once; return counts only actual breaks and never pays the normal rest deficit',()=>{
 const spans=[span('ONE','09:00','18:00'),span('TWO','21:00','23:00','return')],t=report(spans,[op('09:00','12:00'),op('13:00','17:00'),op('21:00','22:00')],{shiftPolicy:true},{...record,breaks:[rest('12:00','13:00'),rest('22:00','22:15')]});assert.equal(t.accounting.automaticRestMinutes,30);assert.equal(t.rest,105);assert.equal(t.bulk,480);assert.equal(t.unassigned,75);assert.equal(t.accounting.groups.length,1);assert.equal(t.offShiftGapMinutes,180);
});
test('several return spans never each create a 40 or 90 minute rest target',()=>{
 const spans=[span('ONE','09:00','12:00'),span('TWO','15:00','16:00','return'),span('THREE','19:00','20:00','return')],t=report(spans,[op('09:00','12:00'),op('15:00','16:00'),op('19:00','20:00')],{shiftPolicy:true});assert.equal(t.presence,300);assert.equal(t.bulk,300);assert.equal(t.rest,0);assert.equal(t.accounting.automaticRestMinutes,0);assert.equal(t.accounting.groups.length,1);assert.equal(t.accounting.status,'insufficient_unassigned');
});
test('denied or pending overtime attribute never deletes real returned attendance',()=>{
 for(const state of ['pending','approved','denied']){const t=report([span('RETURN','21:00','23:00','return',{overtime_state:state})],[op('21:00','22:00')],{shiftPolicy:true});assert.equal(t.presence,120);assert.equal(t.bulk,60);assert.equal(t.unassigned,60);assert.equal(t.rest,0);assert.equal(t.accounting.status,'return_only');}
});
test('mis-checkout gap is not filled and original closed work is never extended into resumed attendance',()=>{
 const t=report([span('ONE','09:00','10:00'),span('TWO','10:20','12:00','correction_resume')],[op('09:00','10:00'),op('10:30','11:00')]);assert.equal(t.presence,160);assert.equal(t.bulk,90);assert.equal(t.unassigned,70);assert.equal(t.offShiftGapMinutes,20);assert.equal(t.attendanceSpans[0].end,at('10:00'));
});
test('open returned attendance has no invented checkout or automatic rest and does not restart old work',()=>{
 const t=report([span('ONE','09:00','18:00'),span('RETURN','21:00','','return')],[op('09:00','10:00')],{shiftPolicy:true},record,at('22:00'));assert.equal(t.presence,600);assert.equal(t.bulk,60);assert.equal(t.accounting.automaticRestMinutes,90);assert.equal(t.unassigned,450);assert.equal(t.attendanceSpans[1].end,'');assert.equal(t.offShiftGapMinutes,180);
});
test('recorded labor and duty cannot be converted to normal rest or overlap each other',()=>{
 const t=report([span('ONE','09:00','12:00'),span('RETURN','15:00','16:00','return')],[op('09:00','10:00'),op('15:00','16:00')],{shiftPolicy:true,dispatchSegments:[{badgeId:record.badgeId,start:at('10:00'),end:at('12:00')}]});assert.equal(t.bulk,120);assert.equal(t.dispatch,120);assert.equal(t.rest,0);assert.equal(t.accounting.automaticRestMinutes,0);
});
test('any accidental cross-gap work is clipped to attendance, and no absent minute becomes labor',()=>{
 const t=report([span('ONE','09:00','10:00'),span('RETURN','11:00','12:00','return')],[op('09:00','12:00')]);assert.equal(t.presence,120);assert.equal(t.bulk,120);assert.equal(t.offShiftGapMinutes,60);
});
test('overlapping imported attendance is unioned, known work remains once, uncertain unassigned is separated',()=>{
 const t=report([span('ONE','09:00','11:00'),span('TWO','10:00','12:00','return')],[op('09:00','10:30')]);assert.equal(t.presence,180);assert.equal(t.bulk,90);assert.equal(t.attendancePendingMinutes,30);assert.equal(t.conflict,30);assert.equal(t.unassigned,60);assert.match(t.flags.join(' '),/子段重叠/);
});
test('real midnight shift is clipped by Korea calendar day without counting the next-day gap',()=>{
 const end='2026-11-03T01:00:00+09:00',s=span('NIGHT','23:00','','normal',{ended_at:end,status:'closed'}),asOf='2026-11-03T02:00:00+09:00';assert.equal(report([s],[],{},record,asOf,day).presence,60);assert.equal(report([s],[],{},record,asOf,'2026-11-03').presence,60);
});
test('unclosed past shift stops at 04:00 provisionally without inventing actual departure or infinite time',()=>{
 const s=span('NIGHT','23:00',''),asOf='2026-11-04T12:00:00+09:00',t=report([s],[],{},record,asOf,'2026-11-03');assert.equal(t.presence,240);assert.equal(t.conflict,240);assert.equal(t.attendancePendingMinutes,240);assert.equal(t.attendanceSpans[0].end,'');assert.equal(t.attendanceSpans[0].provisionalEnd,'2026-11-02T19:00:00.000Z');assert.equal(nextAttendanceCutoff(at('23:00')),Date.parse('2026-11-03T04:00:00+09:00'));
});
test('a verified departure after 04:00 remains a fact; forced operation minutes remain pending',()=>{
 const end='2026-11-03T05:00:00+09:00',t=report([span('NIGHT','23:00','','normal',{ended_at:end,status:'closed'})],[op('23:00','','bulk',{end:'2026-11-03T04:00:00+09:00',reason:'system_cutoff:unverified'})],{},record,'2026-11-03T06:00:00+09:00','2026-11-03');assert.equal(t.presence,300);assert.equal(t.bulk,0);assert.equal(t.systemCutoffMinutes,240);assert.equal(t.unassigned,60);
});
test('foreign parent or stable person cannot leak attendance into another person-day',()=>{
 const t=report([span('OTHER','09:00','18:00','normal',{attendance_id:'OTHER'}),span('PERSON','09:00','18:00','normal',{person_id:'OTHER'})]);assert.equal(t.presence,0);
});
test('legacy single normal span matches existing shift totals and does not mutate inputs',()=>{
 const r={...record,inAt:at('08:50'),breaks:[rest('12:00','13:00')]},ops=[op('09:00','12:00'),op('13:00','17:00')],before=structuredClone({r,ops}),old=shiftAttendanceReport(r,ops,at('19:00')),now=attendanceReturnReport(r,ops,at('19:00'),day,{shiftPolicy:true});for(const key of ['presence','bulk','rest','unassigned','conflict'])assert.equal(now[key],old[key],key);assert.deepEqual({r,ops},before);conserved(now);
});
test('the actual future departure cannot apply quota to a report before checkout',()=>{
 const t=report([span('ONE','09:00','18:00')],[],{shiftPolicy:true},record,at('10:00'));assert.equal(t.presence,60);assert.equal(t.accounting.automaticRestMinutes,0);assert.equal(t.accounting.status,'pending_checkout');
});
test('new schema retains parent uniqueness, enforces one open child per person and child version conflicts',async t=>{
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_RETURNS_ENABLED:'true'};await ensureAttendanceReturnSchema(env);const insert=(id,parent='P')=>DB.raw.prepare('INSERT INTO ck_attendance_spans(id,attendance_id,person_id,source_person_id,badge_id,shift_id,kind,started_at,status,started_by,created_at,ordinal) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)').run(id,parent,'SYNTH-PERSON','SYNTH-PERSON','EMP-SYNTH','SYNTH-SHIFT','return',at('21:00'),'open','SYNTH-ACTOR',at('21:00'),Number(DB.raw.prepare('SELECT COALESCE(MAX(ordinal),0)+1 n FROM ck_attendance_spans WHERE attendance_id=?').get(parent).n));insert('ONE');assert.throws(()=>insert('TWO','OTHER-PARENT'),/UNIQUE/);assert.throws(()=>DB.raw.prepare("UPDATE ck_attendance_spans SET ended_at=?,status='closed' WHERE id='ONE'").run(at('22:00')),/version_conflict/);DB.raw.prepare("UPDATE ck_attendance_spans SET ended_at=?,status='closed',version=version+1 WHERE id='ONE'").run(at('22:00'));insert('TWO');assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_spans').get().n,2);
});
