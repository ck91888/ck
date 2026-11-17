// Real local Workers runtime and D1 binding. Dependencies may live outside the repo.
// Requires esbuild and Miniflare 4.
// CK_WORKER_DEPS=/tmp/ck-office-worker-deps/node_modules node tests/office-duty-d1.mjs
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import vm from 'node:vm';
const require=createRequire(import.meta.url),deps=process.env.CK_WORKER_DEPS;
const {build}=require(deps?path.join(deps,'esbuild'):'esbuild'),{Miniflare}=require(deps?path.join(deps,'miniflare'):'miniflare');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'ck-office-d1-')),bundle=path.join(dir,'worker.mjs');
await build({entryPoints:['worker-v2/index.js'],bundle:true,format:'esm',platform:'browser',outfile:bundle});
const mf=new Miniflare({workers:[{name:'office-duty-test',rootPath:dir,modulesRoot:dir,modules:true,scriptPath:bundle,compatibilityDate:'2026-03-03',d1Databases:['DB'],bindings:{SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()},outboundService:()=>{throw Error('Local-only fixture blocks outbound network');}}]});
try{
 const DB=await mf.getD1Database('DB'),source=fs.readFileSync('worker-v2/index.js','utf8');
 const migrations=vm.runInNewContext(source.slice(source.indexOf('const MIGRATIONS ='),source.indexOf('// 每次发布迁移变化时'))+'\nMIGRATIONS');
 for(const sql of migrations){try{await DB.prepare(sql).run();}catch(e){if(!sql.trim().startsWith('ALTER'))throw e;}}
 let sql='',trigger=false;
 for(const line of fs.readFileSync('worker-v2/migrations/20260917_sop.sql','utf8').split('\n')){if(line.trim().startsWith('--')||!line.trim())continue;sql+=line+'\n';if(/CREATE TRIGGER/i.test(sql))trigger=true;if(line.trim().endsWith(';')&&(!trigger||line.trim()==='END;')){await DB.prepare(sql).run();sql='';trigger=false;}}
 const options=await mf.getBindings(),origin='http://local.fixture';let cookie='',context='';
 const call=async(action,data={})=>{const r=await mf.dispatchFetch(origin+'/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:cookie,'X-CK-Operation-Context':context},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})});if(r.headers.get('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];const out=await r.json();if(out.user?.operation_context)context=out.user.operation_context.id;assert.equal(out.ok,true,action+': '+JSON.stringify(out));return out;};
 await call('sop_login',{sop_key:options.SOP_ADMIN_PASSWORD});await call('sop_identity');await call('sop_operator_confirm',{name:'合成本地D1验收'});
 const p=(await call('sop_attendance_employee_register',{employeeNo:'SYNTH-D1',name:'合成D1职员',department:'order_processing'})).person;await call('sop_attendance_checkin',{badge:p.badgeId});
 let s=await call('sop_office_duty_start',{badge:p.badgeId}),request=crypto.randomUUID(),before=s;
 s=await call('sop_office_duty_pause',{badge:p.badgeId,job_id:s.job.id,revision:s.job.revision,client_req_id:request});assert.equal(s.job.status,'paused');
 assert.equal((await call('sop_office_duty_pause',{badge:p.badgeId,job_id:before.job.id,revision:before.job.revision,client_req_id:request})).replayed,true);
 s=await call('sop_office_duty_resume',{badge:p.badgeId,job_id:s.job.id,revision:s.job.revision});s=await call('sop_office_duty_finish',{badge:p.badgeId,job_id:s.job.id,revision:s.job.revision});assert.equal(s.job.segments.length,2);
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10),r=await call('v2_dashboard_order_export',{date_basis:'actual_work',start_date:day,end_date:day,department:'order_processing'});assert.equal(r.rows[0].quantity_state,'time_only');assert.equal(r.summary.missing_quantity_tasks,0);assert.equal(r.rows[0].department,'order_processing');
 const detail=await call('sop_attendance_employee_work_detail',{date:day,person_id:p.id});assert.equal(detail.workday.officeDuty.quantity_state,'time_only');
 console.log(JSON.stringify({localWorkersRuntime:true,realLocalD1:true,segments:s.job.segments.length,replay:true,actualDateReport:true,employeeDay:true,remoteWrites:false}));
}finally{await mf.dispose();fs.rmSync(dir,{recursive:true,force:true});}
