import {pickStartFixture} from './pick-workload-fixture.mjs';
// Actual built /shuju + real Worker. All sessions, people and jobs are synthetic
// and live only in this process's isolated SQLite memory database.
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
const DB=database(),artifacts=path.resolve(process.env.CK_EMPLOYEE_ARTIFACTS||path.join(tmpdir(),'ck-employee-workday'));fs.mkdirSync(artifacts,{recursive:true});
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('employee-workday-fixture')};
const cookies={manager:'',field:'',reviewer:'',viewer:''};
async function raw(scope,action,body={}){
 const response=await worker.fetch(new Request('http://127.0.0.1'+(scope==='field'?'/001/api':'/api'),{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookies[scope],...operationHeaders(cookies[scope])},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 const setCookie=response.headers.getSetCookie().find(value=>value.startsWith(scope==='field'?'ck_field_access=':'ck_office_access='));if(setCookie)cookies[scope]=setCookie.split(';')[0];
 return rememberOperation(await response.json(),cookies[scope]);
}
async function call(scope,action,body={}){const result=await raw(scope,action,body);assert.equal(result.ok,true,action+': '+(result.message||result.error));return result;}
await call('manager','sop_login',{sop_key:'employee-workday-fixture'});
const office=(action,body)=>call('manager',action,body),field=(action,body)=>call('field',action,body);
const employees=[];
for(const [employeeNo,name] of [['EWD-D','Synthetic shared name'],['EWD-W','Synthetic own worker'],['EWD-S','Synthetic shared name']]){
 const person=(await office('sop_attendance_employee_register',{employeeNo,name,department:'direct_ship'})).person;
 const record=(await office('sop_attendance_checkin',{badge:person.badgeId})).record;employees.push({...person,record});
}
const [dispatcher,participant,sameName]=employees;
// A shared office operator can type the dispatcher's name. That label is not
// an employee identity and must never make an office dispatch belong to them.
await confirmOperation(office,dispatcher.name);
const helper=(await office('sop_attendance_checkin',{name:'Synthetic crew helper',agency:'가온'})).record;
const now=Date.now(),day=dispatcher.record.day,dayStart=Date.parse(day+'T00:00:00+09:00'),unit=Math.min(60000,(now-dayStart)/60);
assert.ok(unit>1000,'Run this fixture after the first KST minute.');
const ago=n=>new Date(now-n*unit).toISOString();
for(const person of [...employees.map(p=>p.record),helper])await office('sop_attendance_correct',{id:person.id,version:person.version,inAt:ago(50),outAt:'',reason:'Synthetic browser fixture attendance'});
await call('field','sop_login',{badge:dispatcher.badgeId});
const start=(invoke,code,crew)=>invoke('sop_native_start',{payload:{action:'v2_pick_job_start_by_docs',...pickStartFixture('v2_pick_job_start_by_docs'),pick_doc_nos:[code],client_req_id:crypto.randomUUID()},workers:crew.map(p=>({id:p.badgeId,name:p.name})),lead_id:crew[0].badgeId,estimated_minutes:30,labor_department:'direct_ship'});
const completed=(await start(field,'SYNTHETIC-OWN-COMPLETE',[participant,helper])).job_id;
const setCrew=crew=>field('sop_native_people',{job_id:completed,revision:DB.raw.prepare('SELECT revision FROM sop_records WHERE id=?').get(completed).revision,workers:crew.map(p=>({id:p.badgeId,name:p.name})),lead_id:crew[0].badgeId,reason:'Synthetic rejoin fixture'});
await setCrew([helper]);await setCrew([participant,helper]);
await field('v2_pick_job_finalize',{job_id:completed,worker_id:participant.badgeId,result_note:'Synthetic complete after rejoin'});
const ownClosed=DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? ORDER BY joined_at,id').all(completed,participant.badgeId);assert.equal(ownClosed.length,2);
// Only age the original real fixture segments, preserving their IDs and job
// relationship. The two separated ten-minute periods must count as one task.
for(const [i,segment] of ownClosed.entries()){
 const joined=ago(i===0?40:20),left=ago(i===0?30:10);
 DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=?,left_at=?,minutes_worked=? WHERE id=?').run(joined,left,10*unit/60000,segment.id);
 DB.raw.prepare('UPDATE v2_pick_worker_docs SET started_at=?,finished_at=?,minutes_worked=? WHERE segment_id=?').run(joined,left,10*unit/60000,segment.id);
}
const active=(await start(field,'SYNTHETIC-OWN-ACTIVE',[participant])).job_id;
DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(ago(5),active);
DB.raw.prepare('UPDATE v2_pick_worker_docs SET started_at=? WHERE job_id=?').run(ago(5),active);
const unmapped=(await start(office,'SYNTHETIC-OFFICE-UNMAPPED',[helper])).job_id;

// Production office login currently issues manager sessions only. These two
// isolated fixture sessions exercise the existing read-role/API policy without
// changing the application's authentication configuration or login behavior.
for(const role of ['reviewer','viewer']){
 await call(role,'sop_login',{sop_key:'employee-workday-fixture'});
 await confirmOperation((action,body)=>call(role,action,body),'Synthetic '+role);
 DB.raw.prepare('UPDATE ck_access_sessions SET role=? WHERE token_hash=?').run(role,await digest(cookies[role].slice(cookies[role].indexOf('=')+1)));
 assert.equal((await call(role,'sop_identity')).user.role,role);
}
const assets=path.resolve(process.env.CK_BROWSER_ASSETS||'worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(assets,'shuju/index.html')),'Build staging assets before this test; this test never builds or deploys.');
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://127.0.0.1');
 if(url.pathname.endsWith('/api')||url.pathname.endsWith('/api/file')){
  const chunks=[];for await(const chunk of req)chunks.push(chunk);
  const response=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env);
  const headers=Object.fromEntries(response.headers);if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();res.writeHead(response.status,headers);res.end(new Uint8Array(await response.arrayBuffer()));return;
 }
 let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
}catch(error){res.writeHead(500);res.end(error.message);}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const visualResults=[],pages=[],errors=[],dialogs=[],scenarios=[],screenshots=[],apiFailures=[],legacyResponses=[];
const bodyOf=request=>request.headers()['content-type']?.includes('application/json')?request.postDataJSON():null;
const record=name=>{scenarios.push(name);console.log('PASS '+name);};
const frames=page=>page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
async function pageFor(role,width=1440,providedContext=null){
 const context=providedContext||await browser.newContext({viewport:{width,height:width<=390?844:1000}}),page=await context.newPage();pages.push(page);
 page.setDefaultTimeout(20000);page.on('pageerror',error=>errors.push(error.message));page.on('dialog',async dialog=>{dialogs.push(dialog.message());await dialog.accept();});
 page.on('response',response=>{if(response.status()>=400&&bodyOf(response.request()))apiFailures.push({role,action:bodyOf(response.request()).action,status:response.status()});});
 page.on('response',response=>{const action=bodyOf(response.request())?.action;if(['v2_dashboard_realtime_overview','v2_dashboard_live_docs'].includes(action))legacyResponses.push({role,action,status:response.status()});});
 await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
 await context.addCookies([{name:cookies[role].slice(0,cookies[role].indexOf('=')),value:cookies[role].slice(cookies[role].indexOf('=')+1),url:origin}]);
 await page.goto(origin+'/shuju/?tab=employees');await Promise.race([page.locator('.ck-employee-dashboard [data-table] tbody tr').filter({hasText:dispatcher.employeeNo}).waitFor(),page.getByText('登录已结束，请重新扫码 / 로그인이 종료되었습니다',{exact:true}).waitFor().then(()=>{throw Error(role+' employee entry was covered by session lock: '+JSON.stringify(apiFailures));})]);return page;
}
const dashboard=page=>page.locator('.ck-employee-dashboard');
const row=(page,person)=>dashboard(page).locator('[data-table] tbody tr').filter({hasText:person.employeeNo});
const detail=page=>dashboard(page).locator('[data-work-detail-dialog]');
async function openDetail(page,person){
 const response=page.waitForResponse(r=>bodyOf(r.request())?.action==='sop_attendance_employee_work_detail');
 await row(page,person).locator('[data-work-detail-open]').click();const result=await(await response).json();assert.equal(result.ok,true,JSON.stringify(result));
 await detail(page).locator('[data-workday-timeline]').waitFor();await frames(page);return result;
}
async function closeDetail(page){await detail(page).locator('[data-close-work-detail]').click();await detail(page).waitFor({state:'hidden'});await frames(page);}
async function holdDetail(page){
 let entered,release,delivered,used=false;const pending=new Promise(resolve=>entered=resolve),done=new Promise(resolve=>delivered=resolve);
 const handler=async route=>{if(!used&&bodyOf(route.request())?.action==='sop_attendance_employee_work_detail'){used=true;const response=await route.fetch();entered();await new Promise(resolve=>release=resolve);await route.fulfill({response});delivered();return;}await route.continue();};
 await page.route('**/api',handler);return {pending,async finish(){release();await done;await page.unroute('**/api',handler);await frames(page);}};
}
async function assertModalFits(page){
 const geometry=await detail(page).evaluate(node=>{const r=node.getBoundingClientRect();return {modal:node.matches(':modal'),left:r.left,top:r.top,right:r.right,bottom:r.bottom,width:innerWidth,height:innerHeight,scrollWidth:node.scrollWidth,clientWidth:node.clientWidth};});
 assert.equal(geometry.modal,true);assert.ok(geometry.left>=0&&geometry.top>=0&&geometry.right<=geometry.width+1&&geometry.bottom<=geometry.height+1,JSON.stringify(geometry));assert.ok(geometry.scrollWidth<=geometry.clientWidth+1,JSON.stringify(geometry));
 const close=detail(page).locator('[data-close-work-detail]');await close.scrollIntoViewIfNeeded();const hit=await close.evaluate(node=>{const r=node.getBoundingClientRect();return node.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));});assert.equal(hit,true,'close button must be reachable above page content');
}
async function screenshot(page,name,element=null){
 const cdp=await page.context().newCDPSession(page),full=await page.locator('dialog[open]').count()===0;
 try{
  const metrics=await cdp.send('Page.getLayoutMetrics');let clip;
  if(element){const box=await element.evaluate(n=>{const r=n.getBoundingClientRect();return {x:(r.left+scrollX)*devicePixelRatio,y:(r.top+scrollY)*devicePixelRatio,width:r.width*devicePixelRatio,height:r.height*devicePixelRatio,scale:1};});clip=box;}
  else if(full)clip={...metrics.contentSize,scale:1};
  const result=await cdp.send('Page.captureScreenshot',{format:'png',fromSurface:true,captureBeyondViewport:!!clip,...(clip?{clip}:{})});
  fs.writeFileSync(path.join(artifacts,name),Buffer.from(result.data,'base64'));screenshots.push(name);
 }finally{await cdp.detach();}
}
function snapshot(){return {jobs:DB.raw.prepare('SELECT * FROM v2_ops_jobs ORDER BY id').all(),workers:DB.raw.prepare('SELECT * FROM v2_ops_job_workers ORDER BY id').all(),results:DB.raw.prepare('SELECT * FROM v2_ops_job_results ORDER BY id').all(),events:DB.raw.prepare('SELECT * FROM sop_events ORDER BY request_id').all(),attendance:DB.raw.prepare('SELECT * FROM ck_attendance_days ORDER BY id').all(),attendanceEvents:DB.raw.prepare('SELECT * FROM ck_attendance_events ORDER BY id').all()};}
// Native browser zoom is set through Chromium's settings API in a disposable
// profile. The CSS viewport and devicePixelRatio are asserted; CSS transforms,
// screenshot resizing and pinch zoom are not substitutes for browser zoom.
async function visualMatrix(){
 const longName='合成·跨境订单运营负责人 김하늘가나다라마바사',longDepartment='跨境电商订单与客户服务支持 / 해외 전자상거래 주문·고객 지원';
 const person=(await office('sop_attendance_employee_register',{employeeNo:'EWD-LONG-20261012',name:longName,department:'order_processing'})).person;
 person.record=(await office('sop_attendance_checkin',{badge:person.badgeId})).record;
 const label='SYNTH-LONG-20261012-跨境电商多渠道订单复核与出库交接-해외주문검수출고인계-ABCDEFGHIJKLMNOPQRSTUVWXYZ';
 const job=(await start(field,label,[person])).job_id;
 // Stress a legacy/free-text department only in this in-memory fixture; the
 // normal registration form still requires its existing department choices.
 DB.raw.prepare('UPDATE ck_attendance_days SET agency=? WHERE person_id=?').run(longDepartment,person.id);
 const before=snapshot();
 const cases=[{width:1440,zoom:1},{width:1280,zoom:1},{width:1440,zoom:1.25},{width:1280,zoom:1.25},{width:1440,zoom:1.5},{width:1280,zoom:1.5},{width:390,zoom:1},{width:320,zoom:1}];
 for(const spec of cases){
  const profile=fs.mkdtempSync(path.join(tmpdir(),'ck-employee-visual-'));
  const context=await chromium.launchPersistentContext(profile,{executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,viewport:{width:spec.width,height:spec.width<=390?844:1000},args:['--no-sandbox']});
  try{
   const settings=await context.newPage();await settings.goto('chrome://settings/appearance');await settings.evaluate(zoom=>new Promise(resolve=>chrome.settingsPrivate.setDefaultZoom(zoom,resolve)),spec.zoom);await settings.close();
   const page=await pageFor('manager',spec.width,context),tag=`${spec.width}-${Math.round(spec.zoom*100)}`;
   await row(page,person).waitFor();await frames(page);
   const metrics=await dashboard(page).evaluate(root=>{
    const table=root.querySelector('.ck-employee-attendance-table'),mode=getComputedStyle(table.querySelector('tbody')).display;
    const rectangles=nodes=>[...nodes].map(n=>{const b=n.getBoundingClientRect();return {left:b.left,right:b.right,top:b.top,bottom:b.bottom,width:b.width,height:b.height,text:n.textContent};});
    const buttons=[...root.querySelectorAll('[data-work-detail-open],[data-correct]')].map(n=>({label:n.textContent,width:n.clientWidth,scroll:n.scrollWidth,height:n.getBoundingClientRect().height,lines:[...n.querySelectorAll('[lang]')].map(span=>{const range=document.createRange();range.selectNodeContents(span);return {text:span.textContent,lines:range.getClientRects().length};})}));
    const aligns=mode==='table-row-group'?[...table.querySelectorAll('.ck-employee-stat-head>.ck-employee-bi')].map((h,i)=>{const cell=table.querySelector(`tbody tr .ck-employee-stat:nth-child(${i+1})`);return Math.abs((h.getBoundingClientRect().left+h.getBoundingClientRect().right-cell.getBoundingClientRect().left-cell.getBoundingClientRect().right)/2);}):[];
    const heading=root.querySelector('.ck-section-heading>div:first-child'),last=heading.lastElementChild;const headingBlank=heading.getBoundingClientRect().bottom-last.getBoundingClientRect().bottom;
    return {headingBlank,cssWidth:innerWidth,dpr:devicePixelRatio,pageWidth:document.documentElement.scrollWidth,mode,rows:rectangles(table.querySelectorAll('tbody tr')),buttons,aligns,tableOverflow:root.querySelector('[data-table]').scrollWidth-root.querySelector('[data-table]').clientWidth};
   });
   assert.ok(Math.abs(metrics.cssWidth-spec.width/spec.zoom)<=1,JSON.stringify(metrics));assert.ok(Math.abs(metrics.dpr-spec.zoom)<.01,JSON.stringify(metrics));assert.ok(metrics.pageWidth<=metrics.cssWidth+1,JSON.stringify(metrics));assert.ok(metrics.tableOverflow<=1,JSON.stringify(metrics));
   assert.ok(metrics.headingBlank<=12,'heading must not reserve empty mobile height: '+JSON.stringify(metrics));
   assert.ok(metrics.aligns.every(delta=>delta<=1),'metric headings align with numeric columns');
   for(const button of metrics.buttons){assert.ok(button.scroll<=button.width+1,JSON.stringify(button));assert.ok(button.height>=44,JSON.stringify(button));assert.ok(button.lines.every(x=>x.lines===1),'no button caption wraps: '+JSON.stringify(button));}
   if(metrics.mode==='table-row-group')assert.ok(metrics.rows.slice(0,3).every(row=>row.height<90),'ordinary desktop rows must stay compact');
   await screenshot(page,`visual-list-${tag}.png`);
   await screenshot(page,`visual-rows-${tag}.png`,dashboard(page).locator('[data-table]'));
   const result=await openDetail(page,person);assert.equal(result.sections.own.some(j=>j.job_id===job),true);await assertModalFits(page);assert.match(await detail(page).locator('[data-workday-own]').innerText(),/ABCDEFGHIJKLMNOPQRSTUVWXYZ/);
   await screenshot(page,`visual-detail-${tag}.png`);
   await detail(page).locator('[data-workday-own]').scrollIntoViewIfNeeded();await screenshot(page,`visual-detail-tasks-${tag}.png`);
   await detail(page).locator('.ck-employee-coverage summary').click();assert.match(await detail(page).locator('.ck-employee-coverage').innerText(),/不代表个人产量/);await assertModalFits(page);await closeDetail(page);
   await row(page,person).locator('[data-correct]').click();const editor=dashboard(page).locator('dialog.ck-editor');await editor.waitFor({state:'visible'});assert.equal(await editor.locator('[name=reason]').isEditable(),true);await editor.locator('[data-cancel]').click();await editor.waitFor({state:'hidden'});
   visualResults.push({...spec,...metrics,detailAndCorrectionClicked:true});record(`native visual ${tag}: compact alignment, unbroken bilingual actions, long text, detail and correction`);
  }finally{await context.close();fs.rmSync(profile,{recursive:true,force:true});}
 }
 assert.deepEqual(snapshot(),before);fs.writeFileSync(path.join(artifacts,'visual-matrix.json'),JSON.stringify({ok:true,cases:visualResults,longName,longDepartment,longTaskLabel:label},null,2));
}
const businessBefore=snapshot();
try{
 const summary=await office('sop_attendance_summary',{date:day,scope:'employee'});
 assert.equal(summary.items.length,3);
 const dispatchedItem=summary.items.find(x=>x.record.personId===dispatcher.id),ownItem=summary.items.find(x=>x.record.personId===participant.id),sameItem=summary.items.find(x=>x.record.personId===sameName.id);
 assert.equal(dispatchedItem.workday.dispatched.total,2);assert.equal(dispatchedItem.workday.dispatched.completed,1);assert.equal(dispatchedItem.workday.own.participated,0);assert.equal(dispatchedItem.workday.own.minutes,0);assert.equal(dispatchedItem.status,'unassigned');
 assert.equal(ownItem.workday.own.participated,2);assert.equal(ownItem.workday.own.completed,1);assert.equal(ownItem.workday.dispatched.total,0);assert.ok(Math.abs(ownItem.workday.own.minutes-25*unit/60000)<0.1,JSON.stringify(ownItem.workday));
 assert.equal(sameItem.workday.dispatched.total,0);assert.equal(sameItem.workday.own.participated,0);
 record('real Worker separates exact dispatcher identity from own task counts, unique rejoin periods, and same-name shared-office labels');
 const desktop=await pageFor('manager'),dispatchRow=row(desktop,dispatcher),ownRow=row(desktop,participant);
 assert.equal(await dispatchRow.locator('[data-work-metric="dispatched"] strong').innerText(),'2');assert.equal(await dispatchRow.locator('[data-work-metric="participated"] strong').innerText(),'0');assert.equal(await ownRow.locator('[data-work-metric="participated"] strong').innerText(),'2');assert.equal(await ownRow.locator('[data-work-metric="completed"] strong').innerText(),'1');
 assert.equal(await dispatchRow.locator('[data-status]').getAttribute('data-status'),'unassigned');assert.match(await dispatchRow.locator('.ck-employee-work-status').innerText(),/无现场任务/);assert.match(await dashboard(desktop).locator('[data-workday-scope]').innerText(),/非个人产量/);
 await dashboard(desktop).locator('[data-work-status-filter]').selectOption('unassigned');assert.equal(await row(desktop,dispatcher).count(),1);assert.equal(await row(desktop,participant).count(),0);assert.equal(await row(desktop,sameName).count(),1);await dashboard(desktop).locator('[data-work-status-filter]').selectOption('');
 await screenshot(desktop,'employee-workday-desktop.png');record('actual desktop employee entry displays separate dispatch and own totals and explains the unassigned filter for management-only activity');

 const dispatchedDetail=await openDetail(desktop,dispatcher);assert.equal(dispatchedDetail.sections.dispatched.length,2);assert.equal(dispatchedDetail.sections.own.length,0);
 assert.match(await detail(desktop).locator('[data-workday-dispatched]').innerText(),/SYNTHETIC-OWN-COMPLETE/);assert.match(await detail(desktop).locator('[data-workday-dispatched]').innerText(),/SYNTHETIC-OWN-ACTIVE/);assert.doesNotMatch(await detail(desktop).innerText(),/SYNTHETIC-OFFICE-UNMAPPED/);assert.equal(await detail(desktop).locator('.ck-employee-coverage').getAttribute('open'),null);await detail(desktop).locator('.ck-employee-coverage summary').click();assert.match(await detail(desktop).innerText(),/共用办公室账号/);await detail(desktop).locator('.ck-employee-coverage summary').click();
 await assertModalFits(desktop);await screenshot(desktop,'employee-dispatch-detail-desktop.png');await closeDetail(desktop);
 assert.equal(await desktop.evaluate(()=>document.activeElement?.hasAttribute('data-work-detail-open')),true,'closing restores focus to detail entry');
 record('desktop top-layer work detail shows only reliably attributed dispatched tasks and no false own work for a dispatcher');

 const ownDetail=await openDetail(desktop,participant),ownJobs=ownDetail.sections.own;
 assert.equal(ownJobs.length,2);assert.equal(ownJobs.find(job=>(job.id||job.job_id)===completed).segments.length,2);assert.ok(Math.abs(ownJobs.find(job=>(job.id||job.job_id)===completed).minutes-20*unit/60000)<0.05);
 assert.equal(await detail(desktop).locator('[data-workday-own] .ck-workday-jobs > li').count(),2);assert.equal(await detail(desktop).locator('[data-workday-timeline] li').filter({hasText:'SYNTHETIC-OWN-COMPLETE'}).count(),2);assert.equal(await detail(desktop).locator('[data-workday-timeline] li').filter({hasText:'SYNTHETIC-OWN-ACTIVE'}).count(),1);
 await detail(desktop).locator('.ck-employee-coverage summary').click();assert.match(await detail(desktop).innerText(),/不代表个人产量/);await detail(desktop).locator('.ck-employee-coverage summary').click();await screenshot(desktop,'employee-own-timeline-desktop.png');await closeDetail(desktop);
 await ownRow.locator('[data-correct]').click();const editor=dashboard(desktop).locator('dialog.ck-editor');await editor.waitFor({state:'visible'});assert.equal(await editor.locator('[name=inAt]').isEditable(),true);assert.equal(await editor.locator('[name=reason]').isEditable(),true);await editor.locator('[data-cancel]').click();await editor.waitFor({state:'hidden'});
 record('own detail retains both participation periods in its chronological timeline while counting the rejoined task once; manager correction entry still opens');

 const loading=await holdDetail(desktop);await row(desktop,dispatcher).locator('[data-work-detail-open]').click();await loading.pending;assert.equal(await detail(desktop).locator('[data-work-detail-content]').getAttribute('aria-busy'),'true');assert.match(await detail(desktop).innerText(),/正在读取当日作业记录/);await closeDetail(desktop);
 const later=await openDetail(desktop,sameName);assert.equal(later.workday.dispatched.total,0);assert.equal(later.workday.own.participated,0);await loading.finish();assert.match(await detail(desktop).innerText(),new RegExp(sameName.employeeNo));assert.doesNotMatch(await detail(desktop).innerText(),/SYNTHETIC-OWN-COMPLETE/);await closeDetail(desktop);
 record('detail opens with visible loading state; a response from a closed employee cannot replace a later employee detail');

 let failed=false;const failure=route=>{if(!failed&&bodyOf(route.request())?.action==='sop_attendance_employee_work_detail'){failed=true;return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'Synthetic employee detail unavailable'})});}return route.continue();};
 const beforeFailure=await dispatchRow.innerText();await desktop.route('**/api',failure);await dispatchRow.locator('[data-work-detail-open]').click();await detail(desktop).locator('[data-work-detail-retry]').waitFor();assert.match(await detail(desktop).locator('[role=alert]').innerText(),/Synthetic employee detail unavailable/);assert.equal(await dispatchRow.innerText(),beforeFailure);await desktop.unroute('**/api',failure);
 const retry=desktop.waitForResponse(r=>bodyOf(r.request())?.action==='sop_attendance_employee_work_detail');await detail(desktop).locator('[data-work-detail-retry]').click();assert.equal((await(await retry).json()).ok,true);await detail(desktop).locator('[data-workday-dispatched]').waitFor();await closeDetail(desktop);
 record('detail read failure preserves the attendance row and offers a successful retry inside the same dialog');

 const mobile=await pageFor('manager',390);assert.equal(await mobile.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'390px page must not require horizontal scrolling');
 const mobileRow=row(mobile,dispatcher);await mobileRow.scrollIntoViewIfNeeded();assert.equal(await mobileRow.locator('[data-work-detail-open]').isVisible(),true);assert.equal(await mobileRow.locator('[data-correct]').isVisible(),true);assert.equal(await mobileRow.locator('.ck-employee-workday').isVisible(),true);await screenshot(mobile,'employee-workday-mobile.png');
 await openDetail(mobile,participant);await assertModalFits(mobile);await screenshot(mobile,'employee-own-detail-mobile.png');await closeDetail(mobile);assert.equal(await mobile.evaluate(()=>document.activeElement?.hasAttribute('data-work-detail-open')),true);
 record('390px employee rows show compact totals and visible detail/correction entries; full detail fits the viewport and returns focus on close');

 let viewerPage;
 for(const role of ['reviewer','viewer']){
  const page=await pageFor(role,role==='viewer'?390:1440);assert.equal(await dashboard(page).locator('[data-correct],[data-add],[data-edit],[data-import]').count(),0);const result=await openDetail(page,dispatcher);assert.equal(result.workday.dispatched.total,2);assert.equal(await detail(page).locator('form,input,select,textarea,[type=submit]').count(),0);await assertModalFits(page);await screenshot(page,'employee-'+role+'-readonly.png');await closeDetail(page);
  const correction=await raw(role,'sop_attendance_correct',{id:dispatcher.record.id,version:dispatcher.record.version,inAt:ago(49),outAt:'',reason:'Synthetic read-role denial'});assert.equal(correction.ok,false,role+' must not correct attendance');
  assert.equal(legacyResponses.some(response=>response.role===role),false,role+' must not request manager-only legacy dashboard data');assert.equal(apiFailures.some(response=>response.role===role&&response.status===401),false,role+' must not be locked out while opening employee attendance');
  if(role==='viewer')viewerPage=page;
 }
 assert.ok(legacyResponses.some(response=>response.role==='manager'&&response.action==='v2_dashboard_realtime_overview'&&response.status===200),'manager legacy realtime overview must still load');
 record('reviewer and viewer can read desktop/mobile details without legacy 401 lockout; edits stay unavailable/server-denied while manager realtime still loads');

 // Expiring only this synthetic session must still cause the real API and
 // CKSession fetch wrapper to reject a new read and cover the page with login.
 DB.raw.prepare('UPDATE ck_access_sessions SET expires_at=0 WHERE token_hash=?').run(await digest(cookies.viewer.slice(cookies.viewer.indexOf('=')+1)));
 const expiredRead=viewerPage.waitForResponse(response=>bodyOf(response.request())?.action==='sop_attendance_summary'&&response.status()===401);
 await viewerPage.evaluate(date=>CKAttendance.api('summary',{date,scope:'employee'}).then(()=>{throw Error('Expired employee read unexpectedly succeeded');},error=>error.message),day);
 assert.equal((await(await expiredRead).json()).ok,false);await viewerPage.getByText('登录已结束，请重新扫码 / 로그인이 종료되었습니다',{exact:true}).waitFor();assert.equal(await viewerPage.evaluate(()=>CKSession.user),null);assert.equal(await dashboard(viewerPage).isVisible(),false);await screenshot(viewerPage,'employee-expired-session-lock-mobile.png');
 record('a genuinely expired viewer session receives a real 401 and locks the employee page behind login');
 assert.deepEqual(snapshot(),businessBefore);assert.deepEqual(errors,[]);
 if(process.env.CK_EMPLOYEE_VISUAL==='1')await visualMatrix();
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:true,pageErrors:errors,dialogs,apiFailures,legacyResponses,scenarios,screenshots,visualResults,identity:{dispatcher:dispatcher.id,participant:participant.id,sameName:sameName.id},jobs:{completed,active,unmapped},workday:{dispatcher:dispatchedItem.workday,own:ownItem.workday,sameName:sameItem.workday},limitations:['Read-role sessions are assigned their existing roles only in this isolated fixture database; production office login currently issues manager sessions.']},null,2));
 console.log('PASS employee workday actual /shuju acceptance. Artifacts: '+artifacts);
}catch(error){
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:false,error:error.stack,pageErrors:errors,dialogs,apiFailures,scenarios},null,2));console.error('PAGE ERRORS',errors);console.error('API FAILURES',apiFailures);
 for(const [index,page] of pages.entries()){if(page.isClosed())continue;console.error('PAGE '+index,await page.locator('body').innerText().catch(()=>''));await page.screenshot({path:path.join(artifacts,'failure-'+index+'.png'),fullPage:true}).catch(()=>{});}throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
