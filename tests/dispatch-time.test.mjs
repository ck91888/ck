import test from 'node:test';import assert from 'node:assert/strict';
import {transitionDispatchTime as change,dispatchTimeReport as report,commitDispatchTime,ensureDispatchTime} from '../worker-v2/dispatch-time.js';
import {database} from './d1-adapter.mjs';
const at=m=>new Date(Date.parse('2026-11-02T00:00:00Z')+m*60000).toISOString();
const base=()=>({personId:'SYNTH-DISPATCHER',attendanceId:'SYNTH-ATTENDANCE',day:'2026-11-02',inAt:at(-30),attendanceValid:true,qualified:true,identityUnique:true,operating:false,resting:false,pendingLoan:false});
const event=(action,m,jobId='A',requestId=crypto.randomUUID())=>({action,at:at(m),jobId,requestId,actorId:'SYNTH-DISPATCHER'});
const step=(state,action,m,context={},jobId='A')=>change(state,event(action,m,jobId),{...base(),...context});
test('sign-in, qualification and report reads before a real assignment never infer preparation or historic labor',()=>{
 const s=step(null,'qualification_change',0,{unfinishedJobIds:['HISTORICAL']});assert.equal(s.status,'waiting_dispatch');assert.equal(report(s,at(20)).seconds,0);assert.equal(s.activatedAt,'');
});
test('multiple concurrent orders count one personal interval; last end pauses and the next dispatch starts now',()=>{
 let s=step(null,'dispatch',0);s=step(s,'dispatch',10,{},'B');s=step(s,'assignment_end',20,{},'A');s=step(s,'assignment_end',30,{},'B');assert.equal(s.status,'waiting_dispatch');assert.equal(report(s,at(40)).seconds,1800);
 s=step(s,'dispatch',45,{},'C');s=step(s,'assignment_end',55,{},'C');assert.equal(report(s,at(60)).seconds,2400);assert.equal(s.segments.length,2);
});
test('a dispatcher personally operating never overlaps dispatch labor and resumes only on actual exit',()=>{
 let s=step(null,'dispatch',0);s=step(s,'operation_start',10,{operating:true});assert.equal(s.status,'operating');assert.equal(report(s,at(20)).seconds,600);
 assert.equal(report(s,at(15)).seconds,600);s=step(s,'operation_end',20);s=step(s,'assignment_end',30);assert.equal(report(s,at(40)).seconds,1200);assert.deepEqual(s.segments.map(x=>[x.start,x.end]),[[at(0),at(10)],[at(20),at(30)]]);
});
for(const [name,context]of [['signed out',{attendanceValid:false}],['resting',{resting:true}],['permission revoked',{qualified:false}],['other operation',{operating:true}],['pending loan',{pendingLoan:true}],['identity conflict',{identityUnique:false}],['no unfinished responsibility',{unfinishedJobIds:[]}]])test('operation exit does not resume when '+name,()=>{
 let s=step(null,'dispatch',0);s=step(s,'operation_start',10,{operating:true});s=step(s,'operation_end',20,context);assert.equal(report(s,at(30)).seconds,600);assert.notEqual(s.status,'dispatching');
});
test('rest, rest return, borrow and checkout preserve exact disjoint dispatch intervals',()=>{
 let s=step(null,'dispatch',0);s=step(s,'rest_start',10,{resting:true});s=step(s,'rest_end',20);s=step(s,'operation_start',30,{operating:true,pendingLoan:true});s=step(s,'operation_end',40,{pendingLoan:true});s=step(s,'operation_end',45);s=step(s,'checkout',50,{attendanceValid:false});assert.equal(report(s,at(60)).seconds,1500);assert.equal(s.status,'out');
 const dispatch=report(s,at(60)).seconds,operation=10*60,rest=10*60,waiting=5*60;assert.equal(dispatch+operation+rest+waiting,50*60);
});
test('explicit transfer ends the old owner and starts the new owner at the transfer instant',()=>{
 let old=step(null,'dispatch',0);old=step(old,'transfer_out',12);const c={...base(),personId:'SYNTH-NEXT',attendanceId:'SYNTH-NEXT-ATT'};const next=change(null,event('transfer_in',12),c);assert.equal(report(old,at(20)).seconds,720);assert.equal(next.segments[0].start,at(12));assert.equal(report(next,at(20)).seconds,480);
});
test('a shared relationship or review cannot invent a responsibility or double business counts',()=>{
 assert.throws(()=>step(null,'share_accept',0),/无效/);assert.throws(()=>step(null,'review',0),/无效/);assert.equal(step(null,'qualification_change',10,{unfinishedJobIds:['PARTNER-ORDER']}).responsibilities.length,0);
});
test('locking the screen, closing a page or having no heartbeat never stops dispatch before a real service event',()=>{
 let s=step(null,'dispatch',0);const before=structuredClone(s);
 assert.equal(report(s,at(240)).seconds,14400);assert.deepEqual(s,before,'read-only reports do not manufacture state changes');
 s=step(s,'rest_start',250,{resting:true});assert.equal(report(s,at(300)).seconds,15000);
 assert.equal(s.segments.length,1);assert.equal(s.segments[0].endReason,'rest_start');
 for(const action of ['heartbeat','screen_lock','disconnect','page_close'])assert.throws(()=>step(s,action,300),/无效/);
});
test('cross-day unclosed intervals remain bounded and unverified without guessing an actual departure or restarting',()=>{
 let s=step(null,'dispatch',0);s=step(s,'assignment_end',10);s=step(s,'dispatch',800);
 const nextDay='2026-11-02T15:10:00.000Z',r=report(s,nextDay);
 assert.equal(r.seconds,600);assert.equal(r.pendingSeconds,6000);assert.equal(r.status,'pending_review');
 assert.equal(r.gaps[0].provisionalEnd,'2026-11-02T15:00:00.000Z');
 assert.equal(report(s,'2026-11-05T15:10:00.000Z').pendingSeconds,6000,'old hanging orders cannot accumulate forever');
 s=change(s,{...event('operation_end',860),at:nextDay},base());assert.equal(s.status,'pending_review');assert.equal(s.segments.filter(x=>!x.end).length,0);
});
test('04:00 uses the existing server cutoff boundary and retains its provisional interval for verification',()=>{
 const beforeFour='2026-11-01T18:30:00.000Z',afterFour='2026-11-01T19:10:00.000Z';
 const s=change(null,{...event('dispatch',0),at:beforeFour},{...base(),inAt:'2026-11-01T18:00:00.000Z'}),r=report(s,afterFour);
 assert.equal(r.seconds,0);assert.equal(r.pendingSeconds,1800);assert.equal(r.status,'pending_review');assert.equal(r.segments[0].endReason,'system_cutoff:unverified');
});
test('another identity, another attendance and stale events are rejected',()=>{
 const s=step(null,'dispatch',10);for(const c of [{personId:'OTHER'},{attendanceId:'OTHER'},{day:'2026-11-03'}])assert.throws(()=>step(s,'qualification_change',20,c));assert.throws(()=>step(s,'qualification_change',9),/早于/);
});
function fixture(t){const DB=database();t.after(()=>DB.raw.close());DB.raw.exec('CREATE TABLE synthetic_dispatch_business(id TEXT PRIMARY KEY)');return {DB,env:{DB,SOP_ENVIRONMENT:'staging',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true'}};}
test('business failure rolls the clock and assignment back, then the same request retries exactly once',async t=>{
 const f=fixture(t),e=event('dispatch',0),c=base();await ensureDispatchTime(f.env);const bad=f.DB.prepare('INSERT INTO synthetic_dispatch_business VALUES(?)').bind('SYNTH-ORDER');
 f.DB.raw.exec("CREATE TRIGGER synthetic_fail BEFORE INSERT ON synthetic_dispatch_business BEGIN SELECT RAISE(ABORT,'synthetic_failure'); END");await assert.rejects(()=>commitDispatchTime(f.env,e,c,[bad]));assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_dispatch_time_sessions').get().n,0);
 f.DB.raw.exec('DROP TRIGGER synthetic_fail');await commitDispatchTime(f.env,e,c,[bad]);await commitDispatchTime(f.env,e,c,[bad]);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM synthetic_dispatch_business').get().n,1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_dispatch_time_events').get().n,1);
 await assert.rejects(()=>commitDispatchTime(f.env,{...e,jobId:'CHANGED'},c),/内容冲突/);
});
test('two devices starting the same personal clock cannot both commit a version; a reread retries without overlapping',async t=>{
 const f=fixture(t),c=base();await ensureDispatchTime(f.env);const results=await Promise.allSettled([commitDispatchTime(f.env,event('dispatch',0,'A'),c),commitDispatchTime(f.env,event('dispatch',0,'B'),c)]);assert.equal(results.filter(x=>x.status==='fulfilled').length,1);assert.equal(results.filter(x=>x.status==='rejected').length,1);
 const winner=results.find(x=>x.status==='fulfilled').value;await commitDispatchTime(f.env,event('dispatch',1,winner.responsibilities.includes('A')?'B':'A'),c);const stored=JSON.parse(f.DB.raw.prepare('SELECT state FROM ck_dispatch_time_sessions').get().state);assert.equal(stored.segments.filter(x=>!x.end).length,1);assert.equal(stored.responsibilities.length,2);
});
test('foundation creates no jobs, worker operation segments, results or business dispatch events',async t=>{
 const f=fixture(t);await commitDispatchTime(f.env,event('dispatch',0),base());for(const table of ['v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','sop_events'])assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM '+table).get().n,0,table);
});
test('one stable person cannot open two dispatch clocks through different attendance identities',async t=>{
 const f=fixture(t);await commitDispatchTime(f.env,event('dispatch',0),base());const c={...base(),attendanceId:'SYNTH-SECOND-BADGE'};await assert.rejects(()=>commitDispatchTime(f.env,event('dispatch',1,'B'),c),/UNIQUE/);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_dispatch_time_sessions').get().n,1);
});
