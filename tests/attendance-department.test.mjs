import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {laborDepartment,jobDefinitions,startJobTypes} from '../shared/labor-department.js';import fs from 'node:fs';
function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={},expectOk=true)=>{const response=await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);const result=await response.json();if(expectOk)assert.equal(result.ok,true,result.error||result.message);return result;};
 return {DB,env,call};
}
test('completed legacy packing with blank department reclassifies existing minutes without changing records',async()=>{
 const {DB,call}=setup();const person=(await call('sop_attendance_checkin',{name:'虚拟代发测试',agency:'가온'})).record;
 // Reproduce the original outbound menu payload, which omits biz_class.
 const start=await call('v2_ops_job_start',{flow_stage:'outbound',job_type:'pack_direct',worker_id:person.badgeId,worker_name:person.name});
 await call('v2_ops_job_finish',{job_id:start.job_id,worker_id:person.badgeId});
 const date='2026-01-10',arrival=date+'T08:00:00+09:00',leave=date+'T08:10:00+09:00',begin=date+'T08:02:00+09:00',end=date+'T08:04:00+09:00';
 DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_in=?,signed_out=? WHERE id=?').run(date,arrival,leave,person.id);
 DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=?,left_at=?,minutes_worked=2 WHERE job_id=?').run(begin,end,start.job_id);
 const before=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').all(start.job_id);
 const job=DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(start.job_id);assert.equal(job.biz_class,'');assert.equal(job.job_type,'pack_direct');assert.equal(job.status,'completed');
 const report=await call('sop_attendance_summary',{date}),item=report.items[0];
 assert.equal(item.record.badgeId,person.badgeId);assert.equal(item.totals.direct_ship,2);assert.equal(item.totals.other,0);assert.equal(item.totals.unassigned,8);assert.equal(item.totals.presence,10);
 assert.equal(item.segments[0].department,'direct_ship');assert.equal(item.segments[0].jobType,'pack_direct');
 assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').all(start.job_id),before);
 assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(start.job_id),job);
});
test('native dispatch persists a missing packing department and both staff retain their full personal minutes',async()=>{
 const {DB,call}=setup();const people=[];for(const name of ['虚拟打包甲','虚拟打包乙'])people.push((await call('sop_attendance_checkin',{name,agency:'가온'})).record);
 const payload={action:'v2_ops_job_start',job_type:'pack_direct',flow_stage:'outbound',client_req_id:'department-dispatch-fixture'};
 const data={payload,workers:people.map(p=>({id:p.badgeId,name:p.name})),lead_id:people[0].badgeId,estimated_minutes:10};
 const started=await call('sop_native_start',data),again=await call('sop_native_start',data);assert.equal(again.job_id,started.job_id);
 assert.equal(DB.raw.prepare('SELECT biz_class FROM v2_ops_jobs WHERE id=?').get(started.job_id).biz_class,'direct_ship');
 assert.equal(DB.raw.prepare('SELECT department FROM sop_records WHERE id=?').get(started.job_id).department,'direct_ship');
 await call('v2_ops_job_finish',{job_id:started.job_id,worker_id:people[0].badgeId,complete_job:true});
 const date='2026-01-11';DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_in=?,signed_out=?').run(date,date+'T08:00:00+09:00',date+'T08:10:00+09:00');
 DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=?,left_at=? WHERE job_id=?').run(date+'T08:02:00+09:00',date+'T08:04:00+09:00',started.job_id);
 const report=await call('sop_attendance_summary',{date});assert.equal(report.items.length,2);
 for(const x of report.items){assert.equal(x.totals.direct_ship,2);assert.equal(x.totals.other,0);}
 assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=?').get(started.job_id).n,2);
});
test('department resolution respects assigned cross-department work and leaves shared unknown work unallocated',()=>{
 assert.equal(laborDepartment({labor_department:'import',biz_class:'direct_ship',job_type:'pack_direct'}),'import');
 assert.equal(laborDepartment({assignment_kind:'task',assigned_department:'import',job_type:'pack_direct'}),'import');
 assert.equal(laborDepartment({biz_class:'direct_ship',job_type:'unload'}),'bulk','unloading belongs to the bulk crew even for B2C cargo');
 assert.equal(laborDepartment({biz_class:'return',job_type:'inbound_return'}),'direct_ship');
 for(const job_type of ['pack_direct','pick_direct','inbound_direct','inbound_return'])assert.equal(laborDepartment({job_type}),'direct_ship');
 for(const job_type of ['bulk_op','inbound_bulk','unload','unplanned_unload','load_outbound','outbound_stock_op'])assert.equal(laborDepartment({job_type}),'bulk');
 for(const job_type of ['load_import','pickup_delivery_import','scan_pallet'])assert.equal(laborDepartment({job_type}),'import');
 for(const job_type of ['inventory','qc','other_internal','unknown','verify_scan','disposal','inbound_change_order'])assert.equal(laborDepartment({job_type}),'other');
 assert.equal(laborDepartment({job_type:'pack_direct',biz_class:'unknown'}),'direct_ship');
});
test('every field job type and native start entrance has an explicit department policy',()=>{
 const app=fs.readFileSync(new URL('../001/app.js',import.meta.url),'utf8');const labels=app.match(/var JOB_TYPE_LABEL = \{([\s\S]*?)\n\};/)[1];
 const types=[...labels.matchAll(/^\s*([a-z_]+):/gm)].map(x=>x[1]);assert.equal(types.length,20);
 for(const type of types)assert.ok(Object.hasOwn(jobDefinitions,type),'Missing department policy: '+type);
 assert.ok(Object.hasOwn(jobDefinitions,'unplanned_unload'));
 const ui=fs.readFileSync(new URL('../shared/sop-dispatch-ui.js',import.meta.url),'utf8');const starts=ui.match(/const startActions=new Set\(\[([^\]]+)\]/)[1];
 for(const [,action] of starts.matchAll(/'([^']+)'/g))assert.ok(Object.hasOwn(startJobTypes,action),'Missing start policy: '+action);
});
test('shared work requires actual department before creating a job and preserves cargo classification',async()=>{
 const {DB,call}=setup(),p=(await call('sop_attendance_checkin',{name:'虚拟盘点甲',agency:'가온'})).record;
 const request={payload:{action:'v2_ops_job_start',job_type:'inventory',flow_stage:'internal',client_req_id:'shared-job-fixture'},workers:[{id:p.badgeId,name:p.name}],lead_id:p.badgeId,estimated_minutes:5};
 const missing=await call('sop_native_start',request,false);assert.equal(missing.ok,false);assert.match(missing.error,/用工部门/);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,0);
 const invalid=await call('sop_native_start',{...request,labor_department:'invented'},false);assert.equal(invalid.ok,false);
 const started=await call('sop_native_start',{...request,labor_department:'import'});const job=DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(started.job_id);assert.equal(job.biz_class,'');
 const assignment=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(started.job_id);assert.equal(assignment.department,'import');assert.equal(JSON.parse(assignment.state).labor_department,'import');
 const result=await call('sop_attendance_summary');assert.equal(result.items[0].segments[0].department,'import');
});
