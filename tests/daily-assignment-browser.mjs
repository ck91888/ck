// Actual built /001 and /shuju + real Worker, with an isolated synthetic database.
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
const DB=database(),objects=new Map(),artifacts=path.join(tmpdir(),'ck-daily-assignment');fs.mkdirSync(artifacts,{recursive:true});
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('daily-assignment-fixture'),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);}}};
let cookie='';
async function call(action,body={}){
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 const result=rememberOperation(await response.json(),cookie);assert.equal(result.ok,true,action+': '+(result.message||result.error));return result;
}
await call('sop_login',{sop_key:'daily-assignment-fixture'});await confirmOperation(call,'Synthetic assignment manager');
const dispatcher=(await call('sop_attendance_employee_register',{employeeNo:'ASSIGN-QA',name:'Synthetic dispatcher',department:'direct_ship'})).person;await call('sop_attendance_checkin',{badge:dispatcher.badgeId});
const first=(await call('sop_attendance_checkin',{name:'Synthetic same name',agency:'가온'})).record;
const second=(await call('sop_attendance_checkin',{name:first.name,agency:'가온',confirm_distinct_person:true,reason:'Synthetic two distinct people with the same name'})).record;
const helper=(await call('sop_attendance_checkin',{name:'Synthetic continuing helper',agency:'가온'})).record;
assert.notEqual(first.badgeId,second.badgeId);assert.match(first.badgeId,/^DA-/);assert.match(second.badgeId,/^DA-/);
const now=Date.now(),dayStart=Date.parse(first.day+'T00:00:00+09:00'),available=now-dayStart;
assert.ok(available>4*60000,'Run this time-accounting fixture at least four minutes after KST midnight.');
const unit=Math.min(60000,available/60),signedIn=new Date(now-50*unit).toISOString(),joinedAt=new Date(now-30*unit).toISOString(),correctedLocal=new Date(now-10*unit+9*3600000).toISOString().slice(0,16),correctedAt=new Date(correctedLocal+':00+09:00').toISOString();
await call('sop_attendance_correct',{id:first.id,version:first.version,inAt:signedIn,outAt:'',reason:'Synthetic historical attendance setup'});
await call('sop_attendance_correct',{id:helper.id,version:helper.version,inAt:signedIn,outAt:'',reason:'Synthetic historical helper setup'});
const root=path.resolve(process.env.CK_BROWSER_ASSETS||'worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(root,'001/index.html'))&&fs.existsSync(path.join(root,'shuju/index.html')),'Build staging assets before this test; the test never builds or deploys.');
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
const officeCtx=await browser.newContext({viewport:{width:1440,height:1000}}),fieldCtx=await browser.newContext({viewport:{width:390,height:844}}),office=await officeCtx.newPage(),field=await fieldCtx.newPage(),errors=[],dialogs=[],scenarios=[],requests=[];
for(const page of [office,field]){
 page.setDefaultTimeout(20000);page.on('pageerror',error=>errors.push(error.message));page.on('dialog',async dialog=>{dialogs.push(dialog.message());await dialog.accept();});
 await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
}
await officeCtx.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
const bodyOf=request=>request.headers()['content-type']?.includes('application/json')?request.postDataJSON():null;
field.on('request',request=>{const body=bodyOf(request);if(body)requests.push(body);});
const home=field.locator('#page-home'),freshness=field.locator('[data-dispatch-freshness]'),taskList=field.locator('[data-dispatch-tasks]'),dashboard=office.locator('#ck-labor-dashboard');
const attendanceRow=person=>dashboard.locator('[data-rows] tr').filter({hasText:person.badgeId});
const record=name=>{scenarios.push(name);console.log('PASS '+name);};
const frames=page=>page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
let jobId,card;
async function setCrew(people){const row=DB.raw.prepare('SELECT revision FROM sop_records WHERE id=?').get(jobId);return call('sop_native_people',{job_id:jobId,revision:row.revision,workers:people.map(p=>({id:p.badgeId,name:p.name})),lead_id:people[0]?.badgeId||'',reason:'Synthetic crew change'});}
async function manualRefresh(){const response=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_dispatch_list');await field.locator('[data-dispatch-refresh]').click();await response;await field.waitForFunction(()=>document.querySelector('[data-dispatch-freshness]')?.dataset.freshness==='current');await frames(field);}
async function waitCrew(names){await field.waitForFunction(({jobId,names})=>{const card=document.querySelector('[data-dispatch-job="'+jobId+'"]');if(!card)return false;const crew=[...card.querySelectorAll('small')].find(x=>x.textContent.startsWith('作业人员'));return crew&&names.every(n=>crew.textContent.includes(n))&&(names.includes('Synthetic same name')||!crew.textContent.includes('Synthetic same name'));},{jobId,names});}
async function holdList(){
 let entered,release,delivered;const pending=new Promise(r=>entered=r),done=new Promise(r=>delivered=r);let used=false,captured;
 const handler=async route=>{if(!used&&bodyOf(route.request())?.action==='sop_dispatch_list'){used=true;const response=await route.fetch();captured=await response.json();entered();await new Promise(r=>release=r);await route.fulfill({response});delivered();return;}return route.continue();};
 await field.route('**/api',handler);
 return {pending,get captured(){return captured;},async finish(){release();await done;await field.unroute('**/api',handler);await frames(field);}};
}
try{
 await field.goto(origin+'/001/');await field.locator('#ck-login-code').fill(dispatcher.badgeId);await field.locator('#ck-auth-gate button[type=submit]').click();await home.waitFor({state:'visible'});await freshness.waitFor();
 await field.evaluate(()=>goPage('order_op_menu'));await field.locator('[onclick="goPage(\'pick_direct\')"]').click();await field.locator('#pickStartDocInput').fill('SYNTHETIC-DAILY-PICK-001');await field.locator('#pickStartDocInput').press('Enter');
 await field.locator('[onclick="submitStartPickByDocs(this)"]').click();const crew=field.locator('dialog[open]');
 for(const person of [first,helper]){await crew.locator('[data-staff-badge]').fill(person.badgeId+'|'+person.name);await crew.locator('[data-staff-badge]').press('Enter');}
 const started=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_native_start');await crew.locator('[type=submit]').click();const start=await(await started).json();assert.equal(start.ok,true,JSON.stringify(start));jobId=start.job_id;
 const dispatched=requests.find(r=>r.action==='sop_native_start');assert.deepEqual(dispatched.workers.map(w=>w.id),[first.badgeId,helper.badgeId]);assert.equal(dispatched.workers.some(w=>w.id===second.badgeId),false);
 await field.locator('#pickWorkingSection').waitFor({state:'visible'});
 // Age only the isolated fixture's just-created real segments; no second dispatch or copied output.
 DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(joinedAt,jobId);
 DB.raw.prepare('UPDATE v2_pick_worker_docs SET started_at=? WHERE job_id=?').run(joinedAt,jobId);
 const originalSegments=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id').all(jobId);
 await field.locator('.ck-dispatch-controls [data-home]').click();card=field.locator('[data-dispatch-job="'+jobId+'"]');await waitCrew([first.name,helper.name]);
 await office.goto(origin+'/shuju/');await office.getByRole('button',{name:'日当人力 / 일용직',exact:true}).click();await attendanceRow(first).waitFor();
 assert.equal(await attendanceRow(first).locator('[data-status]').getAttribute('data-status'),'working');assert.equal(await attendanceRow(second).locator('[data-status]').getAttribute('data-status'),'unassigned');
 assert.match(await attendanceRow(first).innerText(),/SYNTHETIC-DAILY-PICK-001/);assert.doesNotMatch(await attendanceRow(second).innerText(),/SYNTHETIC-DAILY-PICK-001/);
 record('real field picking assigns exact scanned DA IDs; same-name second person remains unassigned on dashboard');

 await attendanceRow(first).locator('[data-person]').click();await dashboard.locator('[data-correct]').click();const editor=dashboard.locator('[data-editor]');
 await editor.locator('[name=inAt]').fill(correctedLocal);await editor.locator('[name=outAt]').fill('');await editor.locator('[name=reason]').fill('Synthetic same-day verified arrival after original job start');
 const correction=office.waitForResponse(r=>bodyOf(r.request())?.action==='sop_attendance_correct');await editor.locator('[type=submit]').click();assert.equal((await(await correction).json()).ok,true);await editor.waitFor({state:'hidden'});await dashboard.locator('[data-detail-dialog] [data-close-detail]').click();
 const report=await call('sop_attendance_summary',{date:first.day,scope:'daily'}),one=report.items.find(x=>x.record.badgeId===first.badgeId),two=report.items.find(x=>x.record.badgeId===second.badgeId);
 assert.equal(one.record.inAt,correctedAt);assert.equal(one.status,'working');assert.equal(one.currentJobs.length,1);assert.equal(one.currentJobs[0].id,jobId);assert.equal(two.status,'unassigned');assert.equal(two.currentJobs.length,0);assert.equal(two.segments.length,0);
 const expectedMinutes=(Date.parse(report.asOf)-Date.parse(correctedAt))/60000;assert.ok(Math.abs(one.totals.direct_ship-expectedMinutes)<0.05,JSON.stringify(one.totals));assert.equal(one.totals.unassigned,0);assert.ok(one.totals.flags.includes('任务超出签到区间'));assert.equal(one.totals.flags.some(x=>x.includes('上个班次任务未退出')),false);assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id').all(jobId),originalSegments);
 assert.equal(await attendanceRow(first).locator('[data-status]').getAttribute('data-status'),'working');assert.equal(await attendanceRow(second).locator('[data-status]').getAttribute('data-status'),'unassigned');
 await office.screenshot({path:path.join(artifacts,'same-name-corrected-dashboard.png'),fullPage:true});
 record('same-day attendance correction keeps live task and clips minutes to corrected arrival without changing original job segments');

 await field.bringToFront();await manualRefresh();await waitCrew([first.name,helper.name]);const beforeAuto=requests.filter(r=>r.action==='sop_dispatch_list').length;await setCrew([helper]);
 await waitCrew([helper.name]);assert.ok(requests.filter(r=>r.action==='sop_dispatch_list').length>beforeAuto);assert.equal(await freshness.getAttribute('data-freshness'),'current');
 assert.equal(await field.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);await field.screenshot({path:path.join(artifacts,'home-crew-refreshed-mobile.png'),fullPage:true});
 record('visible field homepage refreshes changed real crew automatically within its 15-second interval');

 // Old response has a removed person; return to home must discard it before a fresh read.
 await setCrew([first,helper]);const held=await holdList();await field.locator('[data-dispatch-refresh]').click();await held.pending;assert.ok(held.captured.items.find(x=>x.id===jobId).workers.some(w=>w.id===first.badgeId));await setCrew([helper]);
 await field.evaluate(()=>{window.__dailySnapshots=[];window.__dailyObserver=new MutationObserver(()=>window.__dailySnapshots.push(document.querySelector('[data-dispatch-tasks]').textContent));window.__dailyObserver.observe(document.querySelector('[data-dispatch-tasks]'),{childList:true,subtree:true,characterData:true});goPage('order_op_menu');goPage('home');});
 await held.finish();await field.waitForFunction(()=>document.querySelector('[data-dispatch-freshness]')?.dataset.freshness==='current'&&document.querySelector('[data-dispatch-refresh]')?.disabled===false);await waitCrew([helper.name]);
 assert.equal((await field.evaluate(()=>{window.__dailyObserver.disconnect();return window.__dailySnapshots;})).some(text=>text.includes('Synthetic same name')),false);
 record('late response from a previous homepage visit is discarded and followed by current crew data');

 const beforeFailure=await card.innerText();const failHandler=route=>bodyOf(route.request())?.action==='sop_dispatch_list'?route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'Synthetic dispatch unavailable'})}):route.continue();
 await field.route('**/api',failHandler);await field.locator('[data-dispatch-refresh]').click();await field.locator('[data-dispatch-freshness][data-freshness=failed]').waitFor();assert.equal(await card.innerText(),beforeFailure);assert.match(await freshness.innerText(),/刷新失败.*保留上次数据.*过时/);assert.equal(await field.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);await field.screenshot({path:path.join(artifacts,'home-refresh-failure-retains-card.png'),fullPage:true});
 await field.unroute('**/api',failHandler);await manualRefresh();assert.equal(await card.innerText(),beforeFailure);
 record('failed refresh retains existing task cards, marks failure and stale crew explicitly, and permits retry');

 // Use the production people dialog itself, keeping its scanner input unsaved on home.
 await setCrew([first,helper]);const dialogHold=await holdList();await field.locator('[data-dispatch-refresh]').click();await dialogHold.pending;
 await field.evaluate(person=>{window.__dailyDialog=CKNativePeopleDialog({title:'Synthetic crew draft',description:'Synthetic unsaved crew selection',workers:[{id:person.badgeId,name:person.name}],leadId:person.badgeId,submitLabel:'Synthetic submit',onSubmit:async value=>value});},helper);
 const peopleDialog=field.locator('dialog[open]');await peopleDialog.locator('[data-staff-badge]').fill('SYNTHETIC-UNSAVED-SCAN');const duringDialog=requests.filter(r=>r.action==='sop_dispatch_list').length;await dialogHold.finish();
 assert.equal(await card.innerText(),beforeFailure);assert.match(await freshness.innerText(),/自动更新已暂停|자동 갱신 일시 중지/);
 await field.waitForTimeout(16000);assert.equal(requests.filter(r=>r.action==='sop_dispatch_list').length,duringDialog);assert.equal(await peopleDialog.locator('[data-staff-badge]').inputValue(),'SYNTHETIC-UNSAVED-SCAN');assert.equal(await card.innerText(),beforeFailure);
 const afterDialog=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_dispatch_list');await peopleDialog.locator('[data-cancel]').click();await afterDialog;await waitCrew([first.name,helper.name]);await setCrew([helper]);await manualRefresh();await waitCrew([helper.name]);
 record('open native people dialog preserves unsaved scanner input, pauses automatic reads and defers pending responses until close');

 // A task form must retain both its selected job identity and unsaved fields.
 const editingHold=await holdList();await field.locator('[data-dispatch-refresh]').click();await editingHold.pending;await card.click();await field.locator('#pickWorkingSection').waitFor({state:'visible'});await field.locator('#pickRemark').fill('Synthetic unsaved remark survives');await field.locator('#pickResultNote').fill('Synthetic unsaved result survives');
 const whileEditing=requests.filter(r=>r.action==='sop_dispatch_list').length;await editingHold.finish();await field.evaluate(()=>{window.dispatchEvent(new Event('focus'));document.dispatchEvent(new Event('visibilitychange'));window.dispatchEvent(new Event('pageshow'));});
 await field.waitForTimeout(16000);assert.equal(requests.filter(r=>r.action==='sop_dispatch_list').length,whileEditing);assert.equal(await field.locator('#pickRemark').inputValue(),'Synthetic unsaved remark survives');assert.equal(await field.locator('#pickResultNote').inputValue(),'Synthetic unsaved result survives');assert.equal(await field.evaluate(()=>window._activeJobId),jobId);assert.equal(await field.evaluate(()=>window._currentPage),'pick_direct');assert.equal(await field.locator('#pickWorkingSection').isVisible(),true);
 await field.screenshot({path:path.join(artifacts,'pick-draft-preserved-mobile.png'),fullPage:true});
 record('background interval, focus, visibility and late home response never reinitialize an active picking draft');

 await field.locator('.ck-dispatch-controls [data-home]').click();await waitCrew([helper.name]);await setCrew([first,helper]);
 const focusRead=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_dispatch_list');await field.evaluate(()=>window.dispatchEvent(new Event('focus')));await focusRead;await waitCrew([first.name,helper.name]);
 await setCrew([helper]);const visibilityRead=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_dispatch_list');await field.evaluate(()=>document.dispatchEvent(new Event('visibilitychange')));await visibilityRead;await waitCrew([helper.name]);
 await field.evaluate(()=>{window.dispatchEvent(new Event('pagehide'));});await setCrew([first,helper]);const pageShowRead=field.waitForResponse(r=>bodyOf(r.request())?.action==='sop_dispatch_list');await field.evaluate(()=>window.dispatchEvent(new Event('pageshow')));await pageShowRead;await waitCrew([first.name,helper.name]);
 record('home focus, visibilitychange event and pageshow refresh real current crew promptly');

 const backgroundTab=await fieldCtx.newPage();await backgroundTab.goto('about:blank');await backgroundTab.bringToFront();const nativeTabHidingObserved=await field.evaluate(()=>document.hidden);await backgroundTab.close();await field.bringToFront();
 const limitations=nativeTabHidingObserved?[]:['This headless Chromium keeps both tabs document.hidden=false after bringToFront; native tab hiding is not exercised by this browser run. Separate DOM lifecycle tests cover simulated hidden state; this browser run verifies home navigation generations and visibilitychange event refresh.'];

 assert.deepEqual(errors,[]);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,1);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers WHERE worker_id=?').get(second.badgeId).n,0);
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:true,pageErrors:errors,dialogs,scenarios,nativeTabHidingObserved,limitations,identity:{first:first.badgeId,sameNameOther:second.badgeId,helper:helper.badgeId},correction:{originalJoinedAt:joinedAt,correctedArrival:correctedAt,status:one.status,departmentMinutes:one.totals.direct_ship,expectedMinutes,flags:one.totals.flags},counts:{jobs:1,results:0,sameNameOtherSegments:0},screenshots:['same-name-corrected-dashboard.png','home-crew-refreshed-mobile.png','home-refresh-failure-retains-card.png','pick-draft-preserved-mobile.png']},null,2));
 console.log('PASS actual /001 + /shuju daily identity, same-day correction, refreshed crew, failure retention and isolated task drafts. Artifacts: '+artifacts);
}catch(error){
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:false,error:error.stack,pageErrors:errors,dialogs,scenarios},null,2));console.error('PAGE ERRORS',errors);console.error('FIELD',await field.locator('body').innerText());console.error('DASHBOARD',await dashboard.innerText().catch(()=>''));await field.screenshot({path:path.join(artifacts,'failure-field.png'),fullPage:true}).catch(()=>{});await office.screenshot({path:path.join(artifacts,'failure-dashboard.png'),fullPage:true}).catch(()=>{});throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
