// Run explicitly with CODEX_PRIMARY_RUNTIME_NODE_MODULES pointing at miniflare.
import test from 'node:test';
import assert from 'node:assert/strict';
import {reportFixture} from './actual-work-fixture.mjs';
import {reportCustomers} from './actual-work-scale-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';

test('actual work report on real local D1: compound limits, 1003 associated tasks, 4012 daily rows and bounded reads',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());
 const count=1003,ids=Array.from({length:count},(_,i)=>'SYNTH-SCALE-'+String(i).padStart(4,'0'));
 for(const [i,id] of ids.entries()){
  f.job(id,'pack_direct','2026-09-01T00:00:00Z');
  for(const person of ['A','B'])f.segment(id+person,id,id+person,'2026-10-12T14:59:30Z','2026-10-12T15:00:30Z',1);
  f.result('R'+i,id,{packed_box_count:i===0?0:5});
 }
 reportCustomers(f,ids);
 const db=await localD1(f.DB.raw);t.after(()=>db.close());f.env.DB=db;
 const compound=n=>Array.from({length:n},()=> 'SELECT 1 AS n').join(' UNION ALL ');
 assert.equal(f.DB.raw.prepare(compound(500)).all().length,500);
 assert.throws(()=>f.DB.raw.prepare(compound(501)),/too many terms in compound SELECT/);
 assert.equal((await db.prepare(compound(5)).all()).results.length,5);
 await assert.rejects(()=>db.prepare(compound(6)).all(),/D1_ERROR: too many terms in compound SELECT: SQLITE_ERROR/);
 await assert.rejects(()=>db.prepare(compound(501)).all(),/too many terms/);
 await f.ok('sop_identity');db.metrics.length=0;db.metrics.wall_ms=0;
 const started=performance.now(),first=await f.ok('v2_dashboard_order_list'),firstWall=performance.now()-started,firstMetrics=[...db.metrics];
 assert.equal(first.total,count);assert.equal(first.summary.total_seconds,count*120);assert.equal(first.summary.overlap_seconds,0);
 assert.ok(first.rows.every(r=>r.customers.length===7));
 const collect=async(action,report_kind)=>{const all=[];let offset=0;while(true){const p=await f.ok(action,{report_kind,offset,snapshot:first.metadata.snapshot});all.push(...p.rows);if(!p.has_more){assert.equal(all.length,p.total);break;}assert.ok(p.rows.length);offset+=p.rows.length;}return all;};
 const tasks=await collect('v2_dashboard_order_export','tasks');assert.equal(tasks.length,count);assert.equal(new Set(tasks.map(r=>r.job_id)).size,count);assert.equal(tasks.reduce((n,r)=>n+r.total_seconds,0),count*120);assert.ok(tasks.every(r=>r.customers.length===7));
 assert.equal(tasks.reduce((n,r)=>n+r.packed_box_count_sum,0),(count-1)*5);assert.equal(tasks.find(r=>r.job_id===ids[0]).packed_box_count_sum,0);
 const quantities=await collect('v2_dashboard_order_export','quantities');assert.equal(quantities.length,1);assert.equal(quantities[0].task_cumulative_quantity,(count-1)*5);assert.equal(quantities[0].recorded_task_count,count);
 const segments=await collect('v2_dashboard_workhour_summary','segments');assert.equal(segments.length,count*4);assert.equal(segments.reduce((n,s)=>n+s.seconds,0),count*120);assert.equal(new Set(segments.map(s=>s.job_id+'|'+s.worker_id+'|'+s.work_date)).size,count*4);
 const detail=await f.ok('v2_dashboard_order_detail',{job_id:ids[0]});assert.equal(detail.period_report.rows[0].total_seconds,120);assert.equal(detail.period_report.rows[0].customers.length,7);
 const filtered=await f.ok('v2_dashboard_order_list',{doc_no:'SYNTH-REF-2',department:'direct_ship',job_type:'pack_direct',status:'completed'});assert.equal(filtered.total,count);assert.equal(filtered.summary.total_seconds,count*120);
 const empty=await f.ok('v2_dashboard_order_list',{department:'bulk'});assert.equal(empty.total,0);
 // All hot reads respect D1's bind/SQL limits; six fixed association reads,
 // no SELECT per task, no row/quantity loss when relationships share a label.
 assert.equal(firstMetrics.filter(m=>/SELECT job_id,customer FROM/.test(m.sql)).length,6);
 assert.ok(firstMetrics.length<25);assert.ok(db.metrics.every(m=>m.parameters<=100&&m.sql_bytes<100000&&m.argument_bytes<2000000));
 assert.ok(db.metrics.every(m=>m.rows_written===0),'report requests must not change business records');
 assert.ok(db.metrics.every(m=>!/\bUNION\b/i.test(m.sql)));
 console.log(JSON.stringify({local_d1:true,tasks:count,worker_segments:count*2,daily_rows:segments.length,customer_labels:count*7,first_page_wall_ms:Math.round(firstWall),first_page_queries:firstMetrics.length,first_page_rows_read:firstMetrics.reduce((n,m)=>n+m.rows_read,0),first_page_sql_ms:firstMetrics.reduce((n,m)=>n+m.duration,0),max_sql_bytes:Math.max(...db.metrics.map(m=>m.sql_bytes)),max_parameters:Math.max(...db.metrics.map(m=>m.parameters)),max_argument_bytes:Math.max(...db.metrics.map(m=>m.argument_bytes)),all_report_sql_ms:db.metrics.reduce((n,m)=>n+m.duration,0),report_rows_written:db.metrics.reduce((n,m)=>n+m.rows_written,0)}));
});
