import test from 'node:test';import assert from 'node:assert/strict';
import {cutoffDashboardFixture,DAY,NEXT} from './cutoff-dashboard-fixture.mjs';
test('04 cutoff labor is pending rather than fifteen actual person-hours on its first Korean day',async t=>{
 const f=await cutoffDashboardFixture(t);f.job();f.clock();await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';
 const board=await f.api('sop_dashboard',{date:DAY}),hours=await f.api('v2_dashboard_workhour_summary',{start_date:DAY,end_date:DAY});
 assert.equal(hours.summary.total_minutes,0);assert.equal(hours.segments[0].anomaly,1);assert.match(hours.segments[0].leave_reason,/^system_cutoff:/);
 assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,15);assert.equal(board.pending_segment_count,1);assert.match(board.data_quality.find(x=>x.id==='SYN-CUTOFF').reason,/04:00|待核实/);
});
test('04 cutoff labor contributes four pending hours rather than actual work on the next Korean day',async t=>{
 const f=await cutoffDashboardFixture(t);f.job({kind:'task'});f.clock();await f.bind();f.env.SOP_DAILY_CUTOFF_ENABLED='true';
 const board=await f.api('sop_dashboard',{date:NEXT}),hours=await f.api('v2_dashboard_workhour_summary',{start_date:NEXT,end_date:NEXT});
 assert.equal(hours.summary.total_minutes,0);assert.equal(board.person_hours,0);assert.equal(board.pending_person_hours,4);
});
