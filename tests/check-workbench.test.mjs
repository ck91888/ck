import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,b={})=>(await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env)).json();
 const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
 const person=async name=>{const record=(await ok('sop_attendance_checkin',{name,agency:'가온'})).record;return {record,id:record.badgeId,name:record.name};};
 const fresh=async(qty=5)=>{const c=await ok('sop_check_create',{ship_date:'2026-11-01'});await ok('sop_check_items',{id:c.id,revision:1,items:[{barcode:'000001',customer:'合成测试客户',qty}]});return c.id;};
 const start=(batch,people,key=crypto.randomUUID())=>ok('sop_native_start',{payload:{action:'v2_verify_job_start',batch_id:batch,client_req_id:key},workers:people.map(p=>({id:p.id,name:p.name})),lead_id:people[0].id,estimated_minutes:10,labor_department:'bulk'});
 const scan=(batch,job,person,extras={})=>({batch_id:batch,job_id:job.job_id,worker_id:person.id,worker_name:'不能信任的请求姓名',pallet_no:'PAL-01',barcode:'000001',...extras});
 const state=id=>JSON.parse(DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(id).state);
 return {DB,env,call,ok,person,fresh,start,scan,state};
}
test('date checklist starts a staffed job without resetting previous scans; receipt replay is exact',async()=>{
 const f=fixture(),p=await f.person('合成甲'),id=await f.fresh();
 await f.ok('sop_check_scan',{id,revision:2,barcode:'000001',pallet:'OLD'});await f.ok('sop_check_scan',{id,revision:3,barcode:'000001',pallet:'OLD'});
 const j=await f.start(id,[p]);assert.equal(f.state(id).items[0].scanned,2);
 const payload=f.scan(id,j,p,{client_req_id:'scan-once'}),a=await f.ok('v2_verify_scan_submit',payload),b=await f.ok('v2_verify_scan_submit',payload);assert.deepEqual(a,b);assert.equal(a.log.worker_name,p.name);assert.equal(f.state(id).items[0].scanned,3);
 assert.equal((await f.call('v2_verify_scan_submit',{...payload,pallet_no:'OTHER'})).ok,false);
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_verify_scan_logs WHERE job_id=?').get(j.job_id).n,1);
});
test('parallel valid identical barcodes count each box once, overflow and unknown remain exceptions',async()=>{
 const f=fixture(),p=await f.person('并发甲'),id=await f.fresh(3),j=await f.start(id,[p]);
 const r=await Promise.all(Array.from({length:5},(_,i)=>f.ok('v2_verify_scan_submit',f.scan(id,j,p,{client_req_id:'parallel-'+i,pallet_no:'P-'+i}))));
 assert.equal(r.filter(x=>x.scan_result==='ok').length,3);assert.equal(r.filter(x=>x.scan_result==='overflow').length,2);assert.equal(f.state(id).items[0].scanned,3);
 const unknown=await f.ok('v2_verify_scan_submit',f.scan(id,j,p,{barcode:'UNKNOWN'}));assert.equal(unknown.scan_result,'not_found');assert.equal(f.state(id).exceptions.length,3);
 const logs=f.DB.raw.prepare('SELECT * FROM v2_verify_scan_logs WHERE job_id=?').all(j.job_id);assert.equal(logs.length,6);assert.equal(new Set(logs.map(x=>x.pallet_no)).size,6);
});
test('legacy batches use the same atomic continuous-scan rules and retain historical logs',async()=>{
 const f=fixture(),p=await f.person('旧批次甲'),b=await f.ok('v2_verify_batch_upload',{batch_no:'SYNTH-OLD',rows:[{barcode:'000001',customer_name:'合成',planned_box_count:2}]}),j=await f.start(b.id,[p]);
 const r=await Promise.all(Array.from({length:3},(_,i)=>f.ok('v2_verify_scan_submit',f.scan(b.id,j,p,{client_req_id:'legacy-'+i}))));assert.equal(r.filter(x=>x.scan_result==='ok').length,2);assert.equal(r.filter(x=>x.scan_result==='overflow').length,1);
 await f.ok('v2_verify_job_finish',{job_id:j.job_id,complete_batch:true});assert.equal(f.DB.raw.prepare('SELECT status FROM v2_verify_batches WHERE id=?').get(b.id).status,'completed');assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_verify_scan_logs').get().n,3);
});
test('pause, rest, removed crew, wrong batch and duplicate dispatch cannot bypass job admission',async()=>{
 const f=fixture(),a=await f.person('状态甲'),b=await f.person('状态乙'),id=await f.fresh(),j=await f.start(id,[a,b]);
 assert.equal((await f.call('sop_native_start',{payload:{action:'v2_verify_job_start',batch_id:id,client_req_id:'dup'},workers:[{id:a.id,name:a.name}],lead_id:a.id,estimated_minutes:10,labor_department:'bulk'})).ok,false);
 assert.equal((await f.call('v2_verify_scan_submit',f.scan('OTHER',j,a))).ok,false);
 await f.ok('sop_attendance_break_start',{id:a.record.id});assert.equal((await f.call('v2_verify_scan_submit',f.scan(id,j,a))).ok,false);await f.ok('v2_verify_scan_submit',f.scan(id,j,b));
 await f.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'合成暂停'});assert.equal((await f.call('v2_verify_scan_submit',f.scan(id,j,b))).ok,false);
 await f.ok('sop_attendance_break_end',{id:a.record.id});await f.ok('sop_native_resume',{job_id:j.job_id,revision:2,workers:[{id:a.id,name:a.name}],lead_id:a.id});assert.equal((await f.call('v2_verify_scan_submit',f.scan(id,j,b))).ok,false);await f.ok('v2_verify_scan_submit',f.scan(id,j,a));
});
test('scan transaction rechecks racing rest and returns an error without a receipt or partial scan',async()=>{
 const f=fixture(),p=await f.person('竞态甲'),id=await f.fresh(),j=await f.start(id,[p]),original=f.DB.batch;let once=true;
 f.DB.batch=async statements=>{if(once&&statements.some(s=>s.sql.startsWith('INSERT INTO ck_verify_requests VALUES'))){once=false;f.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=? WHERE job_id=?").run(new Date().toISOString(),j.job_id);}return original(statements);};
 const r=await f.call('v2_verify_scan_submit',f.scan(id,j,p,{client_req_id:'racing-rest'}));assert.equal(r.ok,false);assert.equal(f.state(id).items[0].scanned,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_verify_requests').get().n,0);assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM v2_verify_scan_logs').get().n,0);
});
test('completion closes all clocks exactly once and records only this job scans as whole-task facts',async()=>{
 const f=fixture(),a=await f.person('收尾甲'),b=await f.person('收尾乙'),id=await f.fresh(3);
 await f.ok('sop_check_scan',{id,revision:2,barcode:'000001',pallet:'HISTORY'});
 const j=await f.start(id,[a,b]);await f.ok('v2_verify_scan_submit',f.scan(id,j,a));await f.ok('v2_verify_scan_submit',f.scan(id,j,b));
 assert.equal((await f.call('sop_check_close',{id,revision:5,reason:'仍在作业'})).ok,false);
 assert.equal((await f.call('v2_verify_job_finish',{job_id:j.job_id,complete_batch:true})).ok,false);
 const body={job_id:j.job_id,client_req_id:'finish-once',remark:'合成交接保留清单'};await f.ok('v2_verify_job_finish',body);await f.ok('v2_verify_job_finish',body);
 assert.equal(f.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(j.job_id).n,0);
 const results=f.DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=?').all(j.job_id);assert.equal(results.length,1);assert.equal(results[0].box_count,0);assert.equal(JSON.parse(results[0].result_json).task_scanned_ok_count,2);assert.equal(JSON.parse(results[0].result_json).scope,'whole_task');assert.equal(f.state(id).items[0].scanned,3);
 await f.ok('sop_check_close',{id,revision:5,reason:'经理最终交接'});
});
