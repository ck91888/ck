import test from 'node:test';import assert from 'node:assert/strict';
import {attendanceReturnReport} from '../worker-v2/attendance-return-time.js';
import {financePersonDay,financeWorkbookData,koreanTime,decimalHours} from '../shared/labor-export-data.js';
const day='2026-01-02',at=t=>day+'T'+t+':00+09:00',record={id:'SYNTHETIC-ATT',personId:'SYNTHETIC-P',badgeId:'DAF-SYNTHETIC',name:'合成财务口径',agency:'直招',day,inAt:at('09:00'),outAt:'',breaks:[]};
const span=(id,a,b,kind='normal',extra={})=>({id,attendance_id:record.id,person_id:record.personId,badge_id:record.badgeId,shift_id:'SHIFT-ONE',kind,started_at:at(a),ended_at:b?at(b):'',status:b?'closed':'open',overtime_state:kind==='return'?'pending':'none',...extra});
const op=(a,b,department='bulk',extra={})=>({badgeId:record.badgeId,start:at(a),end:b?at(b):'',department,...extra});
function project(spans,ops=[],{asOf=at('23:50'),date=day,breaks=[],dispatchSegments=[],shiftPolicy=false}={}){
 const r={...record,breaks},t=attendanceReturnReport(r,ops,asOf,date,{attendanceSpans:spans,dispatchSegments,shiftPolicy}),source=spans.map(s=>({id:s.id,ordinal:spans.indexOf(s)+1,kind:s.kind,start:s.started_at,end:s.ended_at,status:s.status,overtimeState:s.overtime_state,provisionalEnd:t.attendanceSpans.find(x=>x.id===s.id)?.provisionalEnd,needsVerification:t.attendanceSpans.find(x=>x.id===s.id)?.needsVerification,totals:t.spanTotals.find(x=>x.id===s.id)})),p=financePersonDay({record:r,totals:t,attendanceSpans:source},date,asOf);
 assert.ok(Math.abs(p.minutes.presence-p.minutes.operation-p.minutes.dispatch-p.minutes.rest-p.minutes.unassigned-p.minutes.conflict)<1e-6);return {p,t,book:financeWorkbookData([p],{start:date,end:date,asOf})};
}
test('actual cross-department personal hours ignore management labeling and remain separate from dispatch',()=>{
 const {p}=project([span('A','09:00','18:00')],[op('09:00','10:00','bulk'),op('10:00','11:30','direct_ship'),op('13:00','15:00','import'),op('15:00','15:30','office')],{dispatchSegments:[op('16:00','17:00')]});
 assert.equal(p.minutes.bulk,60);assert.equal(p.minutes.direct_ship,90);assert.equal(p.minutes.import,120);assert.equal(p.minutes.other,30);assert.equal(p.minutes.operation,300);assert.equal(p.minutes.dispatch,60);assert.equal(p.minutes.presence,540);
});
test('normal shift quota applies once and returns subtract actual breaks only; gap is not attendance or rest',()=>{
 const {p,t}=project([span('A','09:00','18:00'),span('B','21:00','23:00','return')],[op('09:00','12:00'),op('13:00','17:00'),op('21:00','22:00')],{breaks:[{start:at('12:00'),end:at('13:00')},{start:at('22:00'),end:at('22:15')}],shiftPolicy:true});
 assert.equal(p.minutes.rawPresence,660);assert.equal(p.minutes.operation,480);assert.equal(p.minutes.rest,105);assert.equal(p.minutes.registeredRest,75);assert.equal(p.minutes.automaticRest,30);assert.equal(p.minutes.gap,180);assert.equal(t.accounting.groups.length,1);assert.equal(p.intervals.length,2);
});
test('third return and denied overtime preserve real attendance without filling departure gaps',()=>{
 const {p}=project([span('A','09:00','10:00'),span('B','11:00','12:00','return',{overtime_state:'denied'}),span('C','13:00','14:00','return')],[op('11:00','12:00')]);assert.equal(p.minutes.presence,180);assert.equal(p.minutes.operation,60);assert.equal(p.minutes.gap,120);assert.equal(p.intervals[1].approval,'denied');assert.equal(p.intervals[2].start,at('13:00'));
});
test('overlapping jobs in one department count personal time once',()=>{
 const {p}=project([span('A','09:00','12:00')],[op('09:00','11:00'),op('10:00','12:00')]);assert.equal(p.minutes.bulk,180);assert.equal(p.minutes.operation,180);
});
test('simultaneous different departments remain conflict instead of arbitrary allocation',()=>{
 const {p}=project([span('A','09:00','10:00')],[op('09:00','10:00'),op('09:00','10:00','import')]);assert.equal(p.minutes.bulk,0);assert.equal(p.minutes.import,0);assert.equal(p.minutes.conflict,60);assert.equal(p.needsVerification,true);
});
test('registered rest and dispatch never add work on top of personal attendance',()=>{
 const {p}=project([span('A','09:00','12:00')],[],{breaks:[{start:at('10:00'),end:at('11:00')}],dispatchSegments:[op('09:00','10:00'),op('11:00','12:00')]});assert.equal(p.minutes.dispatch,120);assert.equal(p.minutes.rest,60);assert.equal(p.minutes.operation,0);assert.equal(p.minutes.unassigned,0);
});
test('real overnight punches retain dates but each Korean calendar day owns only its overlap',()=>{
 const spans=[span('A','23:00','','normal',{ended_at:'2026-01-03T03:00:00+09:00',status:'closed'})],ops=[{...op('23:00',''),end:'2026-01-03T03:00:00+09:00'}],first=project(spans,ops,{asOf:'2026-01-03T04:00:00+09:00'}).p,second=project(spans,ops,{asOf:'2026-01-03T04:00:00+09:00',date:'2026-01-03'}).p;
 assert.equal(first.minutes.bulk,60);assert.equal(second.minutes.bulk,180);assert.equal(koreanTime(second.inAt),'2026-01-02 23:00:00');assert.equal(koreanTime(second.outAt),'2026-01-03 03:00:00');assert.equal(koreanTime(second.intervals[0].dayStart),'2026-01-03 00:00:00');
});
test('open checkout snapshots never invent a departure; 04 cutoff is visibly pending',()=>{
 const {p}=project([span('A','23:00','')],[],{asOf:'2026-01-03T05:00:00+09:00',date:'2026-01-03'});assert.equal(p.outAt,'');assert.equal(p.checkoutState,'未签退（待核实）');assert.equal(p.minutes.presence,240);assert.equal(p.minutes.attendancePending,240);assert.equal(p.minutes.operation,0);assert.equal(koreanTime(p.intervals[0].dayEnd),'2026-01-03 04:00:00');
});
test('ordinary open checkout says not off work and raw clock remains the true scan',()=>{
 const {p}=project([span('A','09:00','')],[],{asOf:at('10:00')});assert.equal(p.checkoutState,'未下班');assert.equal(p.outAt,'');assert.equal(p.minutes.presence,60);assert.equal(p.needsVerification,false);
});
test('policy-effective presence stays distinct from raw punch presence',()=>{
 const {p,book}=project([span('A','08:37','18:00')],[],{shiftPolicy:true});assert.equal(p.minutes.rawPresence,563);assert.equal(p.minutes.presence,540);assert.equal(book.daily[1][book.daily[0].indexOf('原始在岗小时')],9.38);assert.equal(book.daily[1][book.daily[0].indexOf('规则核定在岗小时')],9);assert.equal(p.intervals[0].minutes.rawPresence,563);
});
test('subminute operation sums original precision before two-decimal hour presentation',()=>{
 assert.equal(decimalHours(0.49+0.49+0.49),0.02);assert.equal(decimalHours(0.49)*3,0.03);assert.equal(decimalHours(90.000001),1.5);assert.equal(decimalHours(60.999999),1.02);
});
test('legacy single-span reports keep real punches and accounting without return feature',()=>{
 const p=financePersonDay({record:{...record,outAt:at('10:00')},totals:{presence:60,bulk:60,flags:[]}},day,at('12:00'));assert.equal(p.intervals.length,1);assert.equal(p.minutes.bulk,60);assert.equal(p.intervals[0].minutes.bulk,60);assert.equal(p.checkoutState,'已下班');
});
test('identity-conflict presentation retains pending facts without fabricating formal department work',()=>{
 const p=financePersonDay({record,identityConflict:true,totals:{presence:60,bulk:0,conflict:60,flags:['工牌身份冲突']}},day,at('10:00'));assert.equal(p.minutes.operation,0);assert.equal(p.needsVerification,true);assert.match(p.flags.join(),/身份待核实/);
});
test('Excel text remains plain values even with formula-like or quoted names and badges',()=>{
 const p=financePersonDay({record:{...record,name:'=合成公式测试',badgeId:'DAF-"<>;'},totals:{}},day,at('10:00')),b=financeWorkbookData([p],{start:day,end:day,asOf:at('10:00')});assert.equal(b.daily[1][1],'=合成公式测试');assert.equal(b.daily[1][2],'DAF-"<>;');assert.equal(typeof b.daily[1][1],'string');
});
test('invalid source-time flags remain visibly pending even if computed minutes are zero',()=>{
 const p=financePersonDay({record,totals:{presence:0,flags:['出勤子段时间异常，待核实']}},day,at('10:00'));assert.equal(p.needsVerification,true);assert.equal(p.checkoutState,'未签退（待核实）');assert.match(p.flags.join(),/时间异常/);
});
