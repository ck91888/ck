// Actual built /001, /shuju and /attendance; all people and jobs are isolated fixtures.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),artifacts=path.join(tmpdir(),'ck-daily-badge');fs.mkdirSync(artifacts,{recursive:true});
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('daily-badge-fixture')};
let cookie='';
async function raw(action,body={}){
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 return rememberOperation(await response.json(),cookie);
}
async function call(action,body={}){const result=await raw(action,body);assert.equal(result.ok,true,action+': '+(result.message||result.error));return result;}
await call('sop_login',{sop_key:'daily-badge-fixture'});await confirmOperation(call,'Synthetic badge manager');
const dispatcher=(await call('sop_attendance_employee_register',{employeeNo:'BADGE-QA',name:'Synthetic badge dispatcher',department:'direct_ship'})).person;await call('sop_attendance_checkin',{badge:dispatcher.badgeId});
const first=(await call('sop_attendance_checkin',{name:'Synthetic duplicate name',agency:'가온'})).record;
const second=(await call('sop_attendance_checkin',{name:first.name,agency:'가온',confirm_distinct_person:true,reason:'Synthetic same-name distinct worker'})).record;
const restPerson=(await call('sop_attendance_checkin',{name:'Synthetic legacy rest',agency:'가온'})).record;
const checkoutPerson=(await call('sop_attendance_checkin',{name:'Synthetic legacy checkout',agency:'가온'})).record;
const orphanSeed=(await call('sop_attendance_checkin',{name:'Synthetic orphan fixture seed',agency:'가온'})).record;
assert.notEqual(first.badgeId,second.badgeId);
const now=Date.now(),dayStart=Date.parse(first.day+'T00:00:00+09:00'),unit=Math.min(60000,(now-dayStart)/60);assert.ok(unit>1000,'Run after the first KST minute.');
const arrival=new Date(now-50*unit).toISOString(),joinedAt=new Date(now-30*unit).toISOString();
for(const person of [restPerson,checkoutPerson,orphanSeed])await call('sop_attendance_correct',{id:person.id,version:person.version,inAt:arrival,outAt:'',reason:'Synthetic historical attendance'});
const absentBadge='DA-'+first.day.replaceAll('-','')+'-'+crypto.randomUUID().slice(0,8).toUpperCase();assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_days WHERE worker_id=?').get(absentBadge).n,0);
const count=table=>DB.raw.prepare('SELECT count(*) n FROM '+table).get().n;
const root=path.resolve(process.env.CK_BROWSER_ASSETS||'worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(root,'001/index.html'))&&fs.existsSync(path.join(root,'attendance/index.html')),'Build staging assets before this test; this test never builds or deploys.');
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://127.0.0.1');
 if(url.pathname.endsWith('/api')||url.pathname.endsWith('/api/file')){
  const chunks=[];for await(const chunk of req)chunks.push(chunk);
  const response=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env);
  const headers=Object.fromEntries(response.headers);if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();res.writeHead(response.status,headers);res.end(new Uint8Array(await response.arrayBuffer()));return;
 }
 let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
}catch(error){res.writeHead(500);res.end(error.message);}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const officeCtx=await browser.newContext({viewport:{width:1440,height:1000}}),kioskCtx=await browser.newContext({viewport:{width:1024,height:900}}),fieldCtx=await browser.newContext({viewport:{width:390,height:844}}),office=await officeCtx.newPage(),kiosk=await kioskCtx.newPage(),field=await fieldCtx.newPage(),errors=[],dialogs=[],requests=[],scenarios=[];
const bodyOf=request=>request.headers()['content-type']?.includes('application/json')?request.postDataJSON():null;
for(const page of [office,kiosk,field]){
 page.setDefaultTimeout(20000);page.on('pageerror',error=>errors.push(error.message));page.on('dialog',async dialog=>{dialogs.push(dialog.message());await dialog.accept();});
 await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
}
await officeCtx.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
field.on('request',request=>{const body=bodyOf(request);if(body)requests.push(body);});
const dashboard=office.locator('#ck-labor-dashboard'),row=person=>dashboard.locator('[data-rows] tr').filter({hasText:person.badgeId});
const record=name=>{scenarios.push(name);console.log('PASS '+name);};
async function addBadge(dialog,id,name){await dialog.locator('[data-staff-badge]').fill(id+'|'+name);await dialog.locator('[data-staff-badge]').press('Enter');}
async function startSelected(dialog){const response=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_native_start');await dialog.locator('[type=submit]').click();const result=await(await response).json();await field.waitForFunction(()=>!document.querySelector('dialog[open]'));return result;}
async function refreshDashboard(){const response=office.waitForResponse(r=>bodyOf(r.request())?.action==='sop_attendance_summary');await dashboard.locator('[data-refresh]').click();assert.equal((await(await response).json()).ok,true);await office.waitForFunction(()=>!document.querySelector('#ck-labor-dashboard').hasAttribute('aria-busy'));}
async function legacyJob(person,code,identity={badgeId:person.badgeId,name:person.name}){
 const result=await call('sop_native_start',{payload:{action:'v2_pick_job_start_by_docs',pick_doc_nos:[code],client_req_id:crypto.randomUUID()},workers:[{id:person.badgeId,name:person.name}],lead_id:person.badgeId,estimated_minutes:15,labor_department:'direct_ship'});
 // Simulate already-existing lowercase records locally. Production must retain this evidence.
 const lower=identity.badgeId.toLowerCase(),jobId=result.job_id,dispatch=DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(jobId),state=JSON.parse(dispatch.state);
 state.workers=state.workers.map(w=>({...w,id:lower,name:identity.name}));state.lead_id=lower;if(state.last_lead)state.last_lead={...state.last_lead,id:lower,name:identity.name};
 const orphan=identity.badgeId!==person.badgeId,guard=DB.raw.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='ck_daily_badge_join_update'").get()?.sql;
 // The orphan models a pre-migration row. In this isolated memory database only,
 // seed it and restore the exact new guard synchronously before any API can run.
 DB.raw.exec('BEGIN');
 try{
  if(orphan){assert.ok(guard);DB.raw.exec('DROP TRIGGER ck_daily_badge_join_update');}
  DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(state),jobId);
  DB.raw.prepare('UPDATE v2_ops_job_workers SET worker_id=?,worker_name=?,joined_at=? WHERE job_id=?').run(lower,identity.name,joinedAt,jobId);
  DB.raw.prepare('UPDATE v2_pick_worker_docs SET worker_id=?,worker_name=?,started_at=? WHERE job_id=?').run(lower,identity.name,joinedAt,jobId);
  if(orphan)DB.raw.exec(guard);
  DB.raw.exec('COMMIT');
 }catch(error){DB.raw.exec('ROLLBACK');throw error;}
 assert.equal(DB.raw.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='ck_daily_badge_join_update'").get()?.sql,guard);
 const segment=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').get(jobId),pick=DB.raw.prepare('SELECT * FROM v2_pick_worker_docs WHERE job_id=?').get(jobId);
 return {person:{...person,...identity},jobId,code,lower,segment,pick};
}
function assertClosedOriginal(legacy,reason){
 const rows=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').all(legacy.jobId);assert.equal(rows.length,1);const current=rows[0];
 assert.equal(current.id,legacy.segment.id);assert.equal(current.worker_id,legacy.lower);assert.equal(current.worker_name,legacy.segment.worker_name);assert.equal(current.joined_at,legacy.segment.joined_at);assert.ok(current.left_at);assert.match(current.leave_reason,reason);
 const expected=Math.round((Date.parse(current.left_at)-Date.parse(current.joined_at))/6000)/10;assert.ok(Math.abs(current.minutes_worked-expected)<0.11,JSON.stringify(current));assert.ok(current.minutes_worked>0);
 assert.equal(DB.raw.prepare('SELECT active_worker_count FROM v2_ops_jobs WHERE id=?').get(legacy.jobId).active_worker_count,0);
 const pick=DB.raw.prepare('SELECT * FROM v2_pick_worker_docs WHERE job_id=?').get(legacy.jobId);assert.equal(pick.id,legacy.pick.id);assert.equal(pick.worker_id,legacy.lower);assert.equal(pick.segment_id,legacy.segment.id);assert.equal(pick.started_at,legacy.pick.started_at);assert.equal(pick.status,'completed');assert.equal(pick.finished_at,current.left_at);assert.equal(pick.minutes_worked,current.minutes_worked);
 return {segmentId:current.id,workerId:current.worker_id,joinedAt:current.joined_at,leftAt:current.left_at,minutes:current.minutes_worked};
}
try{
 await field.goto(origin+'/001/');await field.locator('#ck-login-code').fill(dispatcher.badgeId);await field.locator('#ck-auth-gate button[type=submit]').click();await field.locator('#page-home').waitFor({state:'visible'});
 await field.evaluate(()=>goPage('order_op_menu'));await field.locator('[onclick="goPage(\'pick_direct\')"]').click();await field.locator('#pickStartDocInput').fill('SYNTHETIC-CASE-PICK-001');await field.locator('#pickStartDocInput').press('Enter');
 await field.locator('[onclick="submitStartPickByDocs(this)"]').click();let people=field.locator('dialog[open]');await addBadge(people,absentBadge.toLowerCase(),'Synthetic absent worker');const rejected=await startSelected(people);
 assert.equal(rejected.ok,false);assert.match(rejected.error,/签到|출근/);assert.ok(rejected.error.includes(absentBadge));assert.equal(count('v2_ops_jobs'),0);assert.equal(count('v2_ops_job_workers'),0);assert.equal(count('v2_pick_worker_docs'),0);
 record('well-formed lowercase daily badge without attendance is rejected before creating any job or work segment');

 await field.locator('[onclick="submitStartPickByDocs(this)"]').click();people=field.locator('dialog[open]');await addBadge(people,first.badgeId.toLowerCase(),first.name);await addBadge(people,first.badgeId,first.name);
 assert.match(await people.locator('[data-staff-count]').innerText(),/已选 1 人/);assert.equal(await people.locator('[data-staff-people] .badge').count(),1);assert.equal(await people.locator('[data-staff-lead]').inputValue(),first.badgeId);assert.match(await people.locator('[data-staff-status]').innerText(),/已添加.*未重复|이미 추가/);
 assert.equal(await field.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);await field.screenshot({path:path.join(artifacts,'lowercase-duplicate-single-worker-mobile.png'),fullPage:true});
 const started=await startSelected(people);assert.equal(started.ok,true,JSON.stringify(started));const request=requests.filter(r=>r.action==='sop_native_start').at(-1);assert.deepEqual(request.workers,[{id:first.badgeId,name:first.name}]);assert.equal(request.lead_id,first.badgeId);
 const fresh=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').get(started.job_id);assert.equal(fresh.worker_id,first.badgeId);assert.equal(count('v2_ops_job_workers'),1);
 record('lowercase then uppercase scan selects one person and sends canonical worker and lead IDs to the real picking route');

 const rest=await legacyJob(restPerson,'SYNTHETIC-LEGACY-REST'),checkout=await legacyJob(checkoutPerson,'SYNTHETIC-LEGACY-CHECKOUT');
 // Model a pre-fix unauthorized legacy row without ever giving the absent badge attendance.
 const orphan=await legacyJob(orphanSeed,'SYNTHETIC-LEGACY-ORPHAN',{badgeId:absentBadge,name:'Synthetic legacy missing attendance'});
 await office.goto(origin+'/shuju/');await office.getByRole('button',{name:'日当人力 / 일용직',exact:true}).click();await row(first).waitFor();
 assert.equal(await row(first).locator('[data-status]').getAttribute('data-status'),'working');assert.equal(await row(second).locator('[data-status]').getAttribute('data-status'),'unassigned');assert.doesNotMatch(await row(second).innerText(),/SYNTHETIC-CASE-PICK-001/);
 const before=await call('sop_attendance_summary',{date:first.day,scope:'daily'}),other=before.items.find(x=>x.record.badgeId===second.badgeId);assert.equal(other.currentJobs.length,0);assert.equal(other.segments.length,0);
 for(const legacy of [rest,checkout]){
  const item=before.items.find(x=>x.record.badgeId===legacy.person.badgeId);assert.equal(item.status,'working');assert.equal(item.currentJobs[0].id,legacy.jobId);assert.equal(item.segments.length,1);const expected=(Date.parse(before.asOf)-Date.parse(joinedAt))/60000;assert.ok(Math.abs(item.totals.direct_ship-expected)<0.05,JSON.stringify(item.totals));
  assert.equal(await row(legacy.person).locator('[data-status]').getAttribute('data-status'),'working');assert.match(await row(legacy.person).innerText(),new RegExp(legacy.code));assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').get(legacy.jobId),legacy.segment);
 }
 await office.screenshot({path:path.join(artifacts,'canonical-dashboard-legacy-lowercase.png'),fullPage:true});
 record('same-name DA identities remain independent; canonical attendance rows show existing lowercase jobs and their full actual minutes without rewriting segments');

 const warnings=dashboard.locator('[data-identity-warnings]');await warnings.waitFor({state:'visible'});const warning=warnings.locator('[data-identity-warning]').filter({hasText:orphan.lower});assert.equal(await warning.count(),1);assert.match(await warning.innerText(),/SYNTHETIC-LEGACY-ORPHAN/);assert.equal(await warning.locator('button,input,select,textarea').count(),0);
 assert.ok(Array.isArray(before.identityWarnings));assert.ok(JSON.stringify(before.identityWarnings).includes(orphan.lower));assert.equal(before.items.some(x=>x.record.badgeId.toUpperCase()===absentBadge),false);assert.equal(before.items.find(x=>x.record.badgeId===orphanSeed.badgeId).totals.direct_ship,0);assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_days WHERE lower(worker_id)=?').get(orphan.lower).n,0);assert.equal(await dashboard.locator('[data-rows] tr').filter({hasText:absentBadge}).count(),0);
 const orphanSnapshot=()=>({job:DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(orphan.jobId),workers:DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').all(orphan.jobId),dispatch:DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(orphan.jobId),jobs:count('v2_ops_jobs'),segments:count('v2_ops_job_workers'),attendance:count('ck_attendance_days')});const orphanBefore=orphanSnapshot();
 const expanded=await raw('sop_native_people',{job_id:orphan.jobId,revision:orphanBefore.dispatch.revision,workers:[{id:absentBadge,name:orphan.person.name},{id:second.badgeId,name:second.name}],lead_id:absentBadge,reason:'Synthetic attempt to extend invalid legacy crew'});assert.equal(expanded.ok,false,JSON.stringify(expanded));
 const newAttempt=await raw('sop_native_start',{payload:{action:'v2_pick_job_start_by_docs',pick_doc_nos:['SYNTHETIC-ORPHAN-CANNOT-START'],client_req_id:crypto.randomUUID()},workers:[{id:orphan.lower,name:orphan.person.name}],lead_id:orphan.lower,estimated_minutes:15,labor_department:'direct_ship'});assert.equal(newAttempt.ok,false,JSON.stringify(newAttempt));assert.deepEqual(orphanSnapshot(),orphanBefore);
 await office.screenshot({path:path.join(artifacts,'legacy-missing-attendance-warning.png'),fullPage:true});
 record('legacy lowercase work without attendance is shown as a raw-ID exception outside attendance totals; expansion and new dispatch are rejected without writes');

 await row(restPerson).locator('[data-person]').click();await dashboard.locator('[data-rest]').click();const restResponse=office.waitForResponse(r=>bodyOf(r.request())?.action==='sop_attendance_break_start');await dashboard.locator('[data-editor] [type=submit]').click();const restResult=await(await restResponse).json();assert.equal(restResult.ok,true,JSON.stringify(restResult));await dashboard.locator('[data-editor]').waitFor({state:'hidden'});
 const restEvidence=assertClosedOriginal(rest,/attendance:rest|attendance:break/);assert.equal(count('v2_ops_job_workers'),4);await dashboard.locator('[data-detail-dialog] [data-close-detail]').click();assert.equal(await row(restPerson).locator('[data-status]').getAttribute('data-status'),'rest');
 record('real dashboard rest closes the original lowercase segment and picking assignment with its original ID and start time retained');

 await kiosk.goto(origin+'/attendance/');await kiosk.locator('#ck-login-code').fill('daily-badge-fixture');await kiosk.locator('#ck-auth-gate button[type=submit]').click();await kiosk.locator('#tab-out').click();await kiosk.locator('#code').fill(checkoutPerson.badgeId.toLowerCase()+'|'+checkoutPerson.name);const checkoutResponse=kiosk.waitForResponse(r=>bodyOf(r.request())?.action==='sop_attendance_checkout');await kiosk.locator('#terminal-content form .primary').click();const checkoutResult=await(await checkoutResponse).json();assert.equal(checkoutResult.ok,true,JSON.stringify(checkoutResult));assert.equal(checkoutResult.record.badgeId,checkoutPerson.badgeId);await kiosk.locator('.success-panel').waitFor();
 const checkoutEvidence=assertClosedOriginal(checkout,/attendance:checkout/);assert.equal(count('v2_ops_job_workers'),4);await refreshDashboard();assert.equal(await row(checkoutPerson).locator('[data-status]').getAttribute('data-status'),'out');assert.equal(await row(second).locator('[data-status]').getAttribute('data-status'),'unassigned');
 await office.screenshot({path:path.join(artifacts,'legacy-rest-checkout-closed.png'),fullPage:true});
 record('real attendance kiosk accepts lowercase daily scan and checkout closes the original lowercase segment without leaving a live duplicate');

 assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE id=?').get(fresh.id),fresh);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers WHERE worker_id=?').get(second.badgeId).n,0);assert.equal(count('v2_ops_jobs'),4);assert.equal(count('v2_ops_job_results'),0);assert.deepEqual(errors,[]);
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:true,pageErrors:errors,dialogs,scenarios,canonical:{badge:first.badgeId,worker:request.workers[0].id,lead:request.lead_id,sameNameOther:second.badgeId,rejectedWithoutAttendance:absentBadge},legacy:{rest:restEvidence,checkout:checkoutEvidence,orphan:{rawBadge:orphan.lower,jobId:orphan.jobId,attendanceRows:0}},counts:{jobs:4,segments:4,results:0},screenshots:['lowercase-duplicate-single-worker-mobile.png','canonical-dashboard-legacy-lowercase.png','legacy-missing-attendance-warning.png','legacy-rest-checkout-closed.png']},null,2));
 console.log('PASS actual /001 lowercase badge canonicalization and /shuju + /attendance legacy segment accounting and closure. Artifacts: '+artifacts);
}catch(error){
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:false,error:error.stack,pageErrors:errors,dialogs,scenarios},null,2));console.error('ERRORS',errors);console.error('FIELD',await field.locator('body').innerText());console.error('DASHBOARD',await dashboard.innerText().catch(()=>''));console.error('KIOSK',await kiosk.locator('body').innerText());await field.screenshot({path:path.join(artifacts,'failure-field.png'),fullPage:true}).catch(()=>{});await office.screenshot({path:path.join(artifacts,'failure-dashboard.png'),fullPage:true}).catch(()=>{});throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
