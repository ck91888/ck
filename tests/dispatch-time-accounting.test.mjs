import test from 'node:test';import assert from 'node:assert/strict';
import {attendanceReport} from '../worker-v2/attendance-time.js';
import {shiftAttendanceReport} from '../worker-v2/attendance-shift-policy.js';
const at=m=>new Date(Date.parse('2026-11-02T00:00:00Z')+m*60000).toISOString();
const record=(out=360)=>({id:'SYNTH-ATT',personId:'SYNTH-PERSON',badgeId:'DAF-SYNTH',day:'2026-11-02',inAt:at(0),outAt:out===null?'':at(out),breaks:[]});
const op=(a,b,department='bulk')=>({badgeId:'DAF-SYNTH',start:at(a),end:at(b),department});
const duty=(a,b,extra={})=>({badgeId:'DAF-SYNTH',start:at(a),end:b===null?'':at(b),...extra});
const conserved=t=>t.bulk+t.direct_ship+t.import+t.other+t.rest+t.unassigned+t.conflict+(t.dispatch||0);
test('dispatch is single-column labor removed from unassigned while operation and rest remain unchanged',()=>{
 const r={...record(),breaks:[{start:at(300),end:at(330)}]},ops=[op(0,60)],d=[duty(60,300)],old=attendanceReport(r,ops,at(360)),now=attendanceReport(r,ops,at(360),r.day,d);
 assert.equal(now.dispatch,240);assert.equal(now.bulk,old.bulk);assert.equal(now.rest,old.rest);assert.equal(now.unassigned,old.unassigned-240);assert.equal(conserved(now),now.presence);assert.equal(Object.hasOwn(old,'dispatch'),false,'legacy report shape stays compatible');
});
test('checkout rest supplementation uses only remaining unassigned minutes and never subtracts dispatch or operation labor',()=>{
 const r={...record(),breaks:[{start:at(300),end:at(330)}]},t=shiftAttendanceReport(r,[op(0,60)],at(360),r.day,{dispatchSegments:[duty(60,300)]});
 assert.equal(t.dispatch,240);assert.equal(t.bulk,60);assert.equal(t.rest,60);assert.equal(t.unassigned,0);assert.equal(t.accounting.automaticRestMinutes,30);assert.equal(t.accounting.restShortfallMinutes,30);assert.equal(conserved(t),t.presence);
});
test('overlapping dispatch never removes recorded operation or rest and never double-counts either',()=>{
 const r={...record(60),breaks:[{start:at(40),end:at(50)}]},t=attendanceReport(r,[op(10,30)],at(60),r.day,[duty(0,60)]);
 assert.equal(t.dispatch,30);assert.equal(t.bulk,20);assert.equal(t.rest,10);assert.equal(t.conflict,0);assert.equal(conserved(t),60);assert.ok(t.flags.some(x=>x.includes('重叠派审未计入')));
});
test('duplicate duty intervals merge per person rather than per order',()=>{
 const r=record(60),t=attendanceReport(r,[],at(60),r.day,[duty(0,30),duty(10,40)]);assert.equal(t.dispatch,40);assert.equal(t.unassigned,20);assert.equal(conserved(t),60);
});
test('unverified cutoff duty cannot become either confirmed labor or automatic rest',()=>{
 const r=record(180),t=shiftAttendanceReport(r,[],at(180),r.day,{dispatchSegments:[duty(0,60,{needsVerification:true,endReason:'system_cutoff:unverified'})]});
 assert.equal(t.dispatch,0);assert.equal(t.dispatchPendingMinutes,60);assert.equal(t.conflict,60);assert.equal(t.rest,0);assert.equal(t.accounting.automaticRestMinutes,0);assert.equal(conserved(t),180);
});
test('a dispatch interval before rounded shift start remains recorded labor, not discarded preparation or supplementary rest',()=>{
 const r={...record(60),inAt:at(-10)},t=shiftAttendanceReport(r,[],at(60),r.day,{dispatchSegments:[duty(-5,5)]});assert.equal(t.dispatch,10);assert.equal(t.accounting.retainedOutsideShiftMinutes,5);assert.equal(conserved(t),t.presence);
});
test('another badge cannot contribute duty and no historical duty is invented when data is absent',()=>{
 const r=record(60),old=attendanceReport(r,[],at(60)),t=attendanceReport(r,[],at(60),r.day,[{...duty(0,60),badgeId:'DAF-OTHER'}]);assert.equal(t.dispatch,0);assert.equal(t.unassigned,old.unassigned);assert.equal(t.bulk,0);
});
test('screen inactivity does not truncate an ongoing interval inside the valid attendance day',()=>{
 const r=record(null),t=attendanceReport(r,[],at(240),r.day,[duty(0,null)]);assert.equal(t.dispatch,240);assert.equal(t.unassigned,0);assert.equal(conserved(t),240);
});
