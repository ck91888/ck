// Isolated review candidates: actual Worker/SQLite plus extracted shuju export functions.
// No browser, online service, business-source changes, credentials, or deployment.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {pathToFileURL} from 'node:url';
const root=process.env.CK_REVIEW_REPO?pathToFileURL(process.env.CK_REVIEW_REPO.replace(/\\/g,'/').replace(/\/$/,'')+'/'):new URL('../',import.meta.url);
const {default:worker}=await import(new URL('worker-v2/index.js',root));
const {database}=await import(new URL('tests/d1-adapter.mjs',root));
const {ATTENDANCE_SCHEMA,ensureAttendance}=await import(new URL('worker-v2/attendance.js',root));
const {ensureSchema}=await import(new URL('worker-v2/schema-ready.js',root));
async function setup(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_REQUEST_USER:{id:'QA-M',name:'QA isolated',role:'manager'}};
 const raw=async(action,data={})=>{const r=await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);return{status:r.status,body:await r.json()};};
 const ok=async(action,data={})=>{const r=await raw(action,data);assert.equal(r.body.ok,true,JSON.stringify(r));return r.body;};
 await ok('sop_identity');
 DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,created_at) VALUES('QA-JOB','inventory','completed','2026-09-30T00:00:00.000Z')").run();
 const segment=(id,badge,name,start,end,minutes)=>DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(id,'QA-JOB',badge,name,start,end,minutes);
 const management=(data={})=>ok('v2_dashboard_management_summary',{start_date:'2026-09-30',end_date:'2026-09-30',...data});
 const wms=rows=>ok('v2_dashboard_wms_import',{import_type:'generic',file_name:'QA synthetic memory only.csv',rows:rows.map(r=>({work_date:'2026-09-30',...r}))});
 return{DB,env,raw,ok,segment,management,wms};
}

test('warm legacy attendance DB installs new enabled triggers without altering old attendance rows',async t=>{
 const s=await setup(t);s.env.SOP_ATTENDANCE_ENABLED='true';
 const old=ATTENDANCE_SCHEMA.filter(sql=>!sql.includes('CREATE TRIGGER IF NOT EXISTS ck_employee_enabled_'));
 await ensureSchema(s.DB,'attendance-v1',()=>s.DB.batch(old.map(sql=>s.DB.prepare(sql))));
 s.DB.raw.prepare("INSERT INTO ck_attendance_people VALUES('QA-P','EMP-QA-OLD','QA old','bulk','permanent',0,'2026-09-30T00:00:00.000Z')").run();
 s.DB.raw.prepare("INSERT INTO ck_employee_profiles VALUES('QA-P','QA-OLD','bulk',1)").run();
 s.DB.raw.prepare("INSERT INTO ck_attendance_days VALUES('QA-D','QA-P','EMP-QA-OLD','QA old','bulk','2026-09-30','EMP-QA-OLD','2026-09-30T00:00:00.000Z','',1)").run();
 await ensureAttendance(s.env);await ensureAttendance(s.env);
 const triggers=s.DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'ck_employee_enabled_%' ORDER BY name").all().map(r=>r.name);
 assert.deepEqual(triggers,['ck_employee_enabled_checkin','ck_employee_enabled_join']);
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n,1);
 assert.throws(()=>s.segment('QA-FORBIDDEN','EMP-QA-OLD','QA old','2026-09-30T01:00:00.000Z','',0),/disabled|unregistered/i);
 assert.throws(()=>s.DB.raw.prepare("INSERT INTO ck_attendance_days VALUES('QA-D2','QA-P','EMP-QA-OLD','QA old','bulk','2026-10-01','EMP-QA-OLD','2026-10-01T00:00:00.000Z','',1)").run(),/disabled|unregistered/i);
});

test('both dashboards reject invalid or reversed date ranges',async t=>{
 const s=await setup(t);
 for(const action of ['v2_dashboard_workhour_summary','v2_dashboard_management_summary'])for(const dates of [{start_date:'2026-02-30'},{start_date:'2026-10-01',end_date:'2026-09-30'},{end_date:'bad-date'}]){
  const r=await s.raw(action,dates);assert.equal(r.body.ok,false,JSON.stringify(r));
 }
});

test('exact Korean midnight boundaries exclude adjacent shifts without double counting',async t=>{
 const s=await setup(t);
 s.segment('QA-PREV','QA-BADGE','QA name','2026-09-29T14:00:00.000Z','2026-09-29T15:00:00.000Z',60);
 s.segment('QA-IN','QA-BADGE','QA name','2026-09-29T15:00:00.000Z','2026-09-30T15:00:00.000Z',1440);
 s.segment('QA-NEXT','QA-BADGE','QA name','2026-09-30T15:00:00.000Z','2026-09-30T16:00:00.000Z',60);
 for(const action of ['v2_dashboard_workhour_summary','v2_dashboard_management_summary']){
  const r=await s.ok(action,{start_date:'2026-09-30',end_date:'2026-09-30'});assert.equal(r.summary.total_minutes,1440);assert.equal(r.summary.worker_count,1);
 }
});

test('a two-day segment clips to one selected Korean day and retains raw duration',async t=>{
 const s=await setup(t);s.segment('QA-MULTIDAY','QA-BADGE','QA name','2026-09-29T14:00:00.000Z','2026-09-30T16:00:00.000Z',1560);
 const r=await s.ok('v2_dashboard_workhour_summary',{start_date:'2026-09-30',end_date:'2026-09-30'});
 assert.equal(r.summary.total_minutes,1440);assert.equal(r.segments[0].raw_minutes,1560);assert.equal(r.segments[0].anomaly,1);
});

test('ambiguous same-name WMS quantity is returned as unmatched and not assigned to either badge',async t=>{
 const s=await setup(t);for(const id of ['A','B'])s.segment('QA-'+id,'QA-BADGE-'+id,'QA same name','2026-09-30T00:00:00.000Z','2026-09-30T01:00:00.000Z',60);
 await s.wms([{worker_name:'QA same name',qty:10,box_count:2}]);const r=await s.management();
 assert.equal(r.summary.total_qty,10);assert.equal(r.by_worker.reduce((n,x)=>n+x.wms_qty,0),0);
 assert.equal(r.unmatched_wms?.length,1,JSON.stringify(r));assert.equal(r.unmatched_wms[0].qty,10);assert.match(r.unmatched_wms[0].reason,/同名|multiple|ambiguous/i);
});

test('renamed worker retains WMS quantities from both unique historical names',async t=>{
 const s=await setup(t);
 s.segment('QA-OLD','QA-SAME-ID','QA old name','2026-09-30T00:00:00.000Z','2026-09-30T01:00:00.000Z',60);
 s.segment('QA-NEW','QA-SAME-ID','QA new name','2026-09-30T01:00:00.000Z','2026-09-30T02:00:00.000Z',60);
 await s.wms([{worker_name:'QA old name',qty:3,box_count:1},{worker_name:'QA new name',qty:7,box_count:2}]);const r=await s.management();
 assert.equal(r.summary.total_qty,10);assert.equal(r.by_worker.length,1);assert.equal(r.by_worker[0].wms_qty,10,JSON.stringify(r));assert.equal(r.by_worker[0].wms_boxes,3);
});

test('WMS-only employee without selected shifts remains visible as unmatched',async t=>{
 const s=await setup(t);await s.wms([{worker_name:'QA no shift',qty:11,box_count:4}]);const r=await s.management();
 assert.equal(r.summary.total_qty,11);assert.equal(r.by_worker.length,0);assert.equal(r.unmatched_wms?.length,1,JSON.stringify(r));assert.equal(r.unmatched_wms[0].qty,11);
});

test('WMS rows without a name are included in the unmatched reconciliation amount',async t=>{
 const s=await setup(t);await s.wms([{worker_name:'',qty:13,box_count:5}]);const r=await s.management();
 assert.equal(r.summary.total_qty,13);assert.equal(r.by_worker.length,0);
 assert.equal((r.unmatched_wms||[]).reduce((n,x)=>n+x.qty,0),13,JSON.stringify(r));
});

test('exact WMS badge identity disambiguates two staff with the same display name',async t=>{
 const s=await setup(t);for(const id of ['A','B'])s.segment('QA-'+id,'QA-BADGE-'+id,'QA same name','2026-09-30T00:00:00.000Z','2026-09-30T01:00:00.000Z',60);
 await s.wms([{worker_name:'QA same name',worker_id:'QA-BADGE-A',qty:10,box_count:2}]);const r=await s.management();
 assert.equal(r.by_worker.find(x=>x.worker_id==='QA-BADGE-A').wms_qty,10,JSON.stringify(r));
 assert.equal(r.by_worker.find(x=>x.worker_id==='QA-BADGE-B').wms_qty,0);
});

const app=fs.readFileSync(new URL('shuju/app.js',root),'utf8');
const exportSource=app.slice(app.indexOf('function exportWorkhoursSegments()'),app.indexOf('// =====================================================',app.indexOf('function exportWorkhoursSegments()')));
function csvContext(segments,fields={whFilterStart:'2026-09-30',whFilterEnd:'2026-09-30'}){
 let output=null;const ctx={_whQuery:{start_date:fields.whFilterStart,end_date:fields.whFilterEnd},_whSummary:{segments},document:{getElementById:id=>({value:fields[id]||''})},alert:()=>{},jobTypeLabel:x=>x,fmtTime:x=>x||'',round1:x=>x,statusLabel:x=>x,exportCsv:(name,rows)=>output={name,rows}};
 vm.createContext(ctx);vm.runInContext(app.slice(app.indexOf("function workhourKstDate("),app.indexOf("function whFilterParams()"))+exportSource,ctx);return{ctx,get output(){return output;}};
}
test('CSV preserves badge and Korean date while including the requested date range in filename',()=>{
 const s=csvContext([{worker_id:'QA-A',worker_name:'QA',joined_at:'2026-09-30T15:30:00.000Z',left_at:'2026-09-30T16:00:00.000Z',minutes:30}],{whFilterStart:'2026-10-01',whFilterEnd:'2026-10-01'});
 s.ctx.exportWorkhoursSegments();assert.equal(s.output.name,'workhour_segments_2026-10-01_2026-10-01.csv');assert.equal(s.output.rows[0]['工牌'],'QA-A');assert.equal(s.output.rows[0]['日期'],'2026-10-01');
});

test('CSV can export invalid legacy timestamps as quality issues instead of crashing',()=>{
 const s=csvContext([{worker_id:'QA-LEGACY',worker_name:'QA',joined_at:'legacy-invalid-time',left_at:'',minutes:0,anomaly:1,anomaly_reason:'时间无效或倒序'}]);
 assert.doesNotThrow(()=>s.ctx.exportWorkhoursSegments());assert.equal(s.output.rows[0]['异常'],1);assert.equal(s.output.rows[0]['日期'],'');
});

test('failed workhour query invalidates the previous summary before another export',async()=>{
 const loadStart=app.indexOf('async function loadWorkhours(btn)');const branchEnd=app.indexOf('  _whSummary = res;',loadStart);
 const prefix=app.slice(loadStart,branchEnd)+'\n}';
 const ctx={_whSummary:{segments:[{worker_id:'QA-OLD'}]},document:{getElementById:()=>({innerHTML:''})},setBtnLoading:()=>{},whFilterParams:()=>({start_date:'2026-10-01'}),api:async()=>({ok:false})};
 vm.createContext(ctx);vm.runInContext(prefix,ctx);await ctx.loadWorkhours(null);assert.equal(ctx._whSummary,null);
});
