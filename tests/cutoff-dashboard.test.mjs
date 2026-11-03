import test from 'node:test';import assert from 'node:assert/strict';
import {cutoffDashboardFixture,DAY,NEXT} from './cutoff-dashboard-fixture.mjs';
import {correctSystemCutoff} from '../worker-v2/system-cutoff.js';
import {liveSegmentMinutes,segmentMinutes,dashboardRange} from '../worker-v2/dashboard-time.js';
test('04 cutoff labor is pending rather than fifteen actual person-hours on its first Korean day',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock();await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';
 const board=await f.api('sop_dashboard',{date:DAY}),hours=await f.api('v2_dashboard_workhour_summary',{start_date:DAY,end_date:DAY});
 assert.equal(hours.summary.total_minutes,0);assert.equal(hours.segments[0].anomaly,1);assert.match(hours.segments[0].leave_reason,/^system_cutoff:/);
 for(const [action,extra]of [['v2_dashboard_management_summary',{}],['v2_dashboard_workhour_summary',{date_basis:'actual_work',report_kind:'segments'}],['v2_dashboard_management_summary',{date_basis:'actual_work',report_kind:'tasks'}]])assert.equal((await f.api(action,{start_date:DAY,end_date:DAY,...extra})).summary.total_minutes,0,'the existing report variants retain their verified-labor rule');
 assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,15);assert.equal(board.pending_segment_count,1);assert.match(board.data_quality.find(x=>x.id==='SYN-CUTOFF').reason,/04:00|待核实/);
});
test('04 cutoff labor contributes four pending hours rather than actual work on the next Korean day',async t=>{
 const f=await cutoffDashboardFixture(t);f.job({kind:'task'});f.clock();await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';
 const board=await f.api('sop_dashboard',{date:NEXT}),hours=await f.api('v2_dashboard_workhour_summary',{start_date:NEXT,end_date:NEXT});
 assert.equal(hours.summary.total_minutes,0);assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,4);
});
test('mixed verified, open and provisional clocks retain the live versus closed-report distinction',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock();f.job({id:'SYN-VERIFIED',status:'completed',start:DAY+'T12:00:00+09:00'});f.clock({id:'SYN-VERIFIED-CLOCK',jobId:'SYN-VERIFIED',badge:'SYN-B',start:DAY+'T12:00:00+09:00',end:DAY+'T13:00:00+09:00',reason:'finished',minutes:999});f.job({id:'SYN-LIVE',start:NEXT+'T04:30:00+09:00'});f.clock({id:'SYN-LIVE-CLOCK',jobId:'SYN-LIVE',badge:'SYN-C',start:NEXT+'T04:30:00+09:00'});await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';
 const previous=await f.api('sop_dashboard',{date:DAY}),current=await f.api('sop_dashboard',{date:NEXT}),hours=await f.api('v2_dashboard_workhour_summary',{start_date:DAY,end_date:DAY}),openReport=await f.api('v2_dashboard_workhour_summary',{start_date:NEXT,end_date:NEXT});
 assert.equal(previous.person_hours,1);assert.equal(previous.pending_person_hours,15);assert.equal(hours.summary.total_minutes,60);assert.equal(current.person_hours,.5);assert.equal(current.pending_person_hours,4);assert.equal(current.active_people,1);assert.equal(current.live.length,1);assert.equal(openReport.summary.total_minutes,0,'reports still exclude open clocks; the live board retains current work');
});
test('audited correction restores only verified labor and retains the original cutoff audit',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock();await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';await f.api('sop_dashboard',{date:DAY});const before=await f.rows('SELECT * FROM ck_system_cutoff_audit ORDER BY kind,row_id'),raw=(await f.rows('SELECT * FROM v2_ops_job_workers'))[0];
 const input={segment_id:raw.id,expected_end:raw.left_at,end_at:DAY+'T09:30:00+09:00',reason:'Synthetic supervisor evidence',client_req_id:crypto.randomUUID()},env={...f.env,SOP_REQUEST_USER:{id:'SYN-M',name:'Synthetic supervisor',role:'manager',scope:'office'}};await correctSystemCutoff(input,env);await correctSystemCutoff(input,env);
 const board=await f.api('sop_dashboard',{date:DAY}),next=await f.api('sop_dashboard',{date:NEXT}),hours=await f.api('v2_dashboard_workhour_summary',{start_date:DAY,end_date:DAY});assert.equal(board.person_hours,.5);assert.equal(hours.summary.total_minutes,30);assert.equal(board.pending_person_hours,0);assert.equal(board.pending_segment_count,0);assert.equal(board.data_quality.some(x=>x.system_cutoff_pending),false);assert.equal(next.person_hours,0);assert.deepEqual(await f.rows('SELECT * FROM ck_system_cutoff_audit ORDER BY kind,row_id'),before);assert.equal((await f.rows('SELECT COUNT(*) n FROM ck_system_cutoff_corrections'))[0].n,1);
});
test('multiple workers add person-hours independently while a job warning is not duplicated',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock({id:'SYN-A',badge:'SYN-A'});f.clock({id:'SYN-B',badge:'SYN-B'});await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';const board=await f.api('sop_dashboard',{date:DAY});assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,30);assert.equal(board.pending_minutes,1800);assert.equal(board.pending_segment_count,2);assert.equal(board.data_quality.filter(x=>x.id==='SYN-CUTOFF').length,1);assert.deepEqual(board.outputs,{});assert.deepEqual(board.rankings,[]);
});
test('pending work keeps the same department and authenticated role scope as the original board',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock();f.job({id:'SYN-IMPORT',department:'import',type:'import_delivery'});f.clock({id:'SYN-IMPORT-A',jobId:'SYN-IMPORT',badge:'SYN-I-A'});f.clock({id:'SYN-IMPORT-B',jobId:'SYN-IMPORT',badge:'SYN-I-B'});await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';const manager=await f.api('sop_dashboard',{date:DAY});assert.equal(manager.pending_person_hours,45);
 const users=[{id:'SYN-BULK',name:'Synthetic bulk reviewer',role:'reviewer',departments:['bulk'],key:'synthetic-bulk-key'},{id:'SYN-IMPORT-USER',name:'Synthetic import dispatcher',role:'dispatcher',departments:['import'],key:'synthetic-import-key'}],changes={SOP_PUBLIC_TEST_ACCESS:'false',SOP_USERS_JSON:JSON.stringify(users)};
 for(const [i,u]of users.entries()){const login=await f.request('sop_login',{sop_key:u.key},changes);assert.equal(login.data.ok,true);const cookie=login.response.headers.get('Set-Cookie').split(';')[0],board=await f.api('sop_dashboard',{date:DAY,department:users[1-i].departments[0],role:'manager'},changes,{Cookie:cookie});assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,i?30:15);assert.deepEqual(board.data_quality.map(x=>x.id),[i?'SYN-IMPORT':'SYN-CUTOFF']);}
});
test('persisted provisional markers remain excluded with the cutoff switch off; production availability is unchanged',async t=>{
 const f=await cutoffDashboardFixture(t);f.job({status:'system_closed'});f.clock({end:DAY+'T10:00:00+09:00',reason:'system_cutoff:attendance_pending'});await f.bind();const board=await f.api('sop_dashboard',{date:DAY});assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,1);const disabled=await f.request('sop_dashboard',{date:DAY},{SOP_ENVIRONMENT:'production',SOP_UPGRADE_ENABLED:'false'});assert.equal(disabled.data.disabled,true);
});
test('warm board and report reads preserve cutoff snapshots, tasks, events and stored minutes',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock();await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';await f.api('sop_dashboard',{date:DAY});await f.api('v2_dashboard_workhour_summary',{start_date:DAY,end_date:DAY});const before=await f.snapshot();for(const date of [DAY,NEXT,'2026-11-01','2026-11-04'])await f.api('sop_dashboard',{date});assert.equal(await f.snapshot(),before);const raw=(await f.rows('SELECT * FROM v2_ops_job_workers'))[0];assert.equal(raw.minutes_worked,17.5);assert.equal(raw.leave_reason,'system_cutoff:unverified');assert.equal((await f.rows('SELECT * FROM v2_ops_job_results')).length,0);
});
test('live interval clipping retains verified closed-report minutes and bounds future or invalid evidence',()=>{
 const current=Date.parse(NEXT+'T05:00:00+09:00'),range=dashboardRange(NEXT,NEXT),base={joined_at:DAY+'T23:30:00+09:00',left_at:NEXT+'T00:30:00+09:00',leave_reason:'finished'};
 for(const reason of ['finished','system_cutoff_verified','system_cutoff:unverified','system_cutoff:attendance_pending']){const row={...base,leave_reason:reason},before={...row},value=liveSegmentMinutes(row,range,current);assert.equal(value.minutes,segmentMinutes(row,range,current).minutes);assert.equal(value.minutes+value.pending_minutes,30);assert.deepEqual(row,before);}
 for(const row of [{...base,joined_at:'bad'},{...base,left_at:'bad'},{...base,joined_at:NEXT+'T04:00:00+09:00',left_at:NEXT+'T03:00:00+09:00'}])assert.deepEqual(liveSegmentMinutes(row,range,current),{minutes:0,pending_minutes:0,pending:false,invalid:true});
 assert.equal(liveSegmentMinutes({...base,joined_at:NEXT+'T06:00:00+09:00',left_at:NEXT+'T07:00:00+09:00'},range,current).minutes,0);assert.equal(liveSegmentMinutes({...base,joined_at:NEXT+'T04:30:00+09:00',left_at:''},range,current).minutes,30);assert.equal(liveSegmentMinutes(base,dashboardRange(DAY,DAY),current).minutes,30);
});
