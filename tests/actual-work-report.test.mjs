import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {parseOpsResultForExport} from '../worker-v2/report-results.js';
import {reportFixture} from './actual-work-fixture.mjs';
test('KST half-open clipping, old-created work, daily slices, seconds and all summaries reconcile',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());
 f.job('OLD','pack_direct','2026-09-01T00:00:00Z');f.job('NEXT','pack_direct','2026-10-19T00:00:00Z','2026-10-20T00:00:00Z');
 f.segment('LEFT','OLD','A','2026-10-11T14:59:30Z','2026-10-11T15:00:30Z');
 f.segment('MID','OLD','B','2026-10-12T14:59:30Z','2026-10-12T15:00:30Z');
 f.segment('RIGHT','OLD','A','2026-10-18T14:59:30Z','2026-10-18T15:00:30Z');
 f.segment('EXCLUDE','NEXT','C','2026-10-18T15:00:00Z','2026-10-18T16:00:00Z');
 f.result('R','OLD',{packed_box_count:478});
 const r=await f.ok();assert.equal(r.total,1);assert.equal(r.summary.total_seconds,120);assert.equal(r.rows[0].total_minutes,2);assert.equal(r.rows[0].comparable,false);assert.match(r.rows[0].efficiency_reason,/跨期/);
 const seg=await f.ok('v2_dashboard_workhour_summary',{report_kind:'segments'});assert.equal(seg.total,4);assert.equal(seg.segments.reduce((n,s)=>n+s.seconds,0),120);assert.deepEqual(seg.segments.map(s=>s.work_date),['2026-10-12','2026-10-12','2026-10-13','2026-10-18']);
 assert.equal(r.by_department.reduce((n,g)=>n+g.total_seconds,0),120);assert.equal(r.by_job_type.reduce((n,g)=>n+g.total_seconds,0),120);assert.equal(r.quantities[0].completed_in_period_quantity,478);assert.equal(r.quantities[0].qty_per_hour,null);
 const detail=await f.ok('v2_dashboard_order_detail',{job_id:'OLD'});assert.equal(detail.period_report.rows[0].total_seconds,120);assert.equal(detail.parsed.packed_box_count_sum,478);
});
test('pause gaps excluded, multiple workers retained, same-person overlaps visible even through job filter',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('A');f.job('B','qc');
 f.segment('A1','A','P','2026-10-12T00:00:00Z','2026-10-12T01:00:00Z');f.segment('A2','A','P','2026-10-12T02:00:00Z','2026-10-12T03:00:00Z');
 f.segment('A3','A','Q','2026-10-12T00:00:00Z','2026-10-12T01:00:00Z');f.segment('B1','B','P','2026-10-12T00:30:00Z','2026-10-12T01:30:00Z');
 const r=await f.ok();assert.equal(r.summary.total_seconds,14400);assert.equal(r.summary.overlap_seconds,3600);
 const filtered=await f.ok(undefined,{job_type:'pack_direct'});assert.equal(filtered.summary.total_seconds,10800);assert.equal(filtered.summary.overlap_seconds,1800);assert.match(filtered.rows[0].data_gaps.join(','),/重叠/);
});
test('open, invalid, no-segment and unknown department records are gaps, not zero productivity',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('OPEN','inventory','2026-10-12T00:00:00Z','','');f.job('MISSING','inventory','2026-10-12T00:00:00Z','2026-10-13T00:00:00Z','');f.job('INVALID');
 f.segment('O','OPEN','P','2026-10-11T00:00:00Z','');f.segment('I','INVALID','P','','bad');
 const r=await f.ok();assert.equal(r.total,3);assert.equal(r.summary.total_seconds,0);assert.equal(r.summary.excluded_open_count,1);assert.equal(r.summary.missing_quantity_tasks,3);assert.equal(r.rows.find(x=>x.id==='MISSING').department,'other');assert.ok(r.rows.every(x=>!x.comparable));
});
test('same-period complete comparable sample is whole-task, no multiplication by two workers or linked bills',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('PUT','pack_direct');
 for(const id of ['A','B'])f.segment(id,'PUT',id,'2026-10-12T00:00:00Z','2026-10-12T00:30:00Z');f.result('R','PUT',{location_count:17,used_forklift:false,putaway_refs:[{plan_id:'ONE'},{plan_id:'TWO'}]});
 const r=await f.ok();assert.equal(r.rows[0].comparable,true);assert.equal(r.rows[0].location_count_sum,17);assert.equal(r.summary.total_seconds,3600);assert.equal(r.quantities[0].comparable_quantity,17);assert.equal(r.quantities[0].qty_per_hour,17);
});
test('result aliases once, zero versus missing, scan mapping on legacy type, corrections and complete raw JSON',()=>{
 const line={unit_type:'carton',putaway_qty:478},row={id:'R',job_id:'J',result_json:JSON.stringify({result_lines:[line]}),result_lines_json:JSON.stringify([line])};
 let p=parseOpsResultForExport('inbound_direct',[row]);assert.equal(p.putaway_carton_qty_sum,478);assert.equal(p.result_lines_count,1);
 p=parseOpsResultForExport('old_scan',[{result_json:JSON.stringify({scanned_ok_count:392,abnormal_count:32})}]);assert.equal(p.verify_ok_count_sum,392);assert.equal(p.verify_ng_count_sum,32);
 p=parseOpsResultForExport('verify_scan',[{result_json:'{"verify_ok_count":0,"scanned_ok_count":392}'}]);assert.equal(p.verify_ok_count_sum,0);assert.equal(p.verify_ng_count_sum,null);assert.equal(p.quantity_state,'recorded');
 assert.equal(parseOpsResultForExport('pack_direct',[{box_count:0,pallet_count:0}]).quantity_state,'missing');
 const long='合成'.repeat(2000);p=parseOpsResultForExport('inbound_direct',[{...row,result_json:JSON.stringify({note:long})}]);assert.ok(p.raw_result_json_compact.includes(long));assert.doesNotThrow(()=>JSON.parse(p.raw_result_json_compact));
 p=parseOpsResultForExport('pack_direct',[{id:'A',job_id:'J',result_json:'{"packed_box_count":10}'},{id:'B',job_id:'J',source:'manual_correction',previous_result_id:'A',result_json:'{"packed_box_count":0}'}]);assert.equal(p.packed_box_count_sum,0);
});
test('stable pagination beyond 1000 records, all filtered pages match summaries; changing source rejects snapshot',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());
 for(let i=0;i<1003;i++){const id='J'+String(i).padStart(4,'0');f.job(id);f.segment('S'+i,id,'W'+i,'2026-10-12T00:00:00Z','2026-10-12T00:00:01Z',0);}
 let first=await f.ok(undefined,{limit:200}),all=[...first.rows],offset=200;assert.equal(first.summary.total_seconds,1003);assert.equal(first.total,1003);
 while(offset<first.total){const p=await f.ok(undefined,{offset,snapshot:first.metadata.snapshot});all.push(...p.rows);offset+=p.rows.length;}assert.equal(new Set(all.map(x=>x.job_id)).size,1003);assert.equal(all.reduce((n,x)=>n+x.total_seconds,0),1003);
 f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at='2026-10-12T00:00:02Z' WHERE id='S1'").run();const changed=await f.call(undefined,{offset:200,snapshot:first.metadata.snapshot});assert.equal(changed.ok,false);assert.match(changed.error,/变化/);
});
test('date safety errors and exact legacy page count/date output; report reads preserve synthetic rows',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('KST','pack_direct','2026-10-11T15:01:00Z');const before=f.DB.raw.prepare('SELECT * FROM v2_ops_jobs').all();
 for(const d of [{start_date:''},{end_date:'2026-11-30'},{start_date:'2026-02-30'},{start_date:'2026-10-19'}])assert.equal((await f.call(undefined,d)).ok,false);
 const r=await f.ok(undefined,{date_basis:'created_at',limit:1});assert.equal(r.total,1);assert.equal(r.has_more,false);assert.equal(r.rows[0].日期,'2026-10-12');assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_ops_jobs').all(),before);
});

test('internal start snapshots and return counts stay distinct from finish output and from workers',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());
 for(const [id,type,mode,phase,metrics] of [['PRECISE','inventory','precise','start',{inventory_count:17,location_count:3,used_forklift:false}],['BLIND','inventory','blind','finish',{inventory_count:0,location_count:0,used_forklift:false}],['RETURN','inbound_return',null,'finish',{return_order_count:4,return_product_count:18}]]){
  f.job(id,type);const operation={version:1,job_type:type,inventory_type:mode,[phase]:metrics};f.DB.raw.prepare('UPDATE v2_ops_jobs SET shared_result_json=? WHERE id=?').run(JSON.stringify({internal_operation:operation}),id);
  for(const who of ['A','B'])f.segment(id+who,id,id+who,'2026-10-12T00:00:00Z','2026-10-12T00:01:00Z');
 }
 const r=await f.ok();assert.equal(r.rows.find(x=>x.id==='PRECISE').inventory_count,17);assert.equal(r.rows.find(x=>x.id==='BLIND').inventory_count,0);assert.equal(r.rows.find(x=>x.id==='RETURN').return_order_count,4);
 assert.equal(r.quantities.find(x=>x.metric==='internal_inventory_count'&&x.phase==='start').qty_per_hour,null);
 const ret=r.quantities.find(x=>x.metric==='internal_return_order_count');assert.equal(ret.task_cumulative_quantity,4);assert.equal(ret.comparable_seconds,120);
});
test('source disagreement is explicit, quantity aliases do not duplicate the canonical metric, empty mirrors fall back',()=>{
 const line={unit_type:'pallet',putaway_qty:17};
 const p=parseOpsResultForExport('inbound_direct',[{pallet_count:17,result_json:JSON.stringify({pallet:17,result_lines:[line]}),result_lines_json:JSON.stringify([line])}]);
 assert.equal(p.putaway_pallet_qty_sum,17);assert.equal(p.quantity_metrics.filter(x=>x.unit==='托').reduce((n,x)=>n+x.quantity,0),17);
 const q=parseOpsResultForExport('inbound_direct',[{result_json:JSON.stringify({result_lines:[line]}),result_lines_json:'[]'}]);assert.equal(q.putaway_pallet_qty_sum,17);
 const mismatch=parseOpsResultForExport('inbound_direct',[{result_json:JSON.stringify({result_lines:[line]}),result_lines_json:JSON.stringify([{...line,putaway_qty:15}])}]);assert.equal(mismatch.putaway_pallet_qty_sum,15);assert.match(mismatch.quantity_warnings.join(''),/不一致/);
});
test('generated daily badges share identity and cutoff precision is explicit; no named-worker merge',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('A');f.job('B');
 f.segment('A1','A','da-20261012-abcdef01','2026-10-12T00:00:00.250Z','2026-10-12T00:00:01.750Z');
 f.segment('B1','B','DA-20261012-ABCDEF01','2026-10-12T00:00:01.250Z','2026-10-12T00:00:02.750Z');
 const r=await f.ok('v2_dashboard_workhour_summary',{as_of:'2026-10-12T00:00:02Z'});assert.equal(r.summary.total_seconds,2.25);assert.equal(r.summary.overlap_seconds,1);assert.equal(r.summary.worker_count,1);assert.ok(r.segments.some(x=>x.provisional));
});
test('department filters use saved assignment and real client links without quantity fan-out; management matches seconds',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('A','pack_direct');f.segment('S','A','A','2026-10-12T00:00:00Z','2026-10-12T00:00:12.345Z');f.result('R','A',{packed_box_count:0});
 f.DB.raw.prepare("INSERT INTO sop_records(id,kind,revision,department,state,updated_at) VALUES('A','dispatch',1,'bulk',?,'2026-10-13T00:00:00Z')").run(JSON.stringify({labor_department:'bulk',customer:'合成客户'}));
 const r=await f.ok(undefined,{department:'bulk'});assert.equal(r.total,1);assert.equal(r.rows[0].customer,'合成客户');assert.equal(r.rows[0].quantity_state,'recorded');assert.equal(r.rows[0].packed_box_count_sum,0);assert.equal((await f.ok(undefined,{department:'direct_ship'})).total,0);
 const mgmt=await f.ok('v2_dashboard_management_summary');assert.equal(mgmt.summary.total_seconds,r.summary.total_seconds);assert.equal(mgmt.by_department[0].total_seconds,12.345);
});
test('details preserve more than 200 rows and long JSON; failed oversized range returns no partial report',async t=>{
 const f=await reportFixture();t.after(()=>f.DB.raw.close());f.job('DETAIL');
 for(let i=0;i<220;i++)f.result('R'+i,'DETAIL',{packed_box_count:1,note:'合成'.repeat(600)});
 const d=await f.ok('v2_dashboard_order_detail',{job_id:'DETAIL'});assert.equal(d.results.length,220);assert.equal(d.parsed.packed_box_count_sum,220);assert.equal(JSON.parse(d.parsed.raw_result_json_compact).length,220);
 const r=await f.call(undefined,{end_date:'2026-11-30'});assert.equal(r.ok,false);assert.equal(r.summary,undefined);
});
