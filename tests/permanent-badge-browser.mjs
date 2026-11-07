// Built /shuju and /attendance pages, real staging Worker, isolated in-memory SQLite.
// Run after: node worker-v2/build-sop-staging.mjs
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import entry from '../worker-v2/staging-entry.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

const require=createRequire(import.meta.url);
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const assets=path.resolve('worker-v2/.sop-staging-assets');
const artifacts='/tmp/ck-permanent-badge-browser';
assert.ok(fs.existsSync(path.join(assets,'shuju/index.html')),'Build staging assets before running this test');
fs.mkdirSync(artifacts,{recursive:true});
const DB=database(),password='permanent-browser-fixture-only';
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest(password)};
env.ASSETS={async fetch(request){
 const url=new URL(request.url);let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));
 if(file!==assets&&!file.startsWith(assets+path.sep))return new Response('Invalid path',{status:400});
 try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('Not found',{status:404});}
}};
const server=http.createServer(async(req,res)=>{
 try{
  const chunks=[];for await(const chunk of req)chunks.push(chunk);
  const request=new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(['GET','HEAD'].includes(req.method)?{}:{body:Buffer.concat(chunks)})});
  const response=await entry.fetch(request,env),headers=Object.fromEntries(response.headers);
  if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();
  res.writeHead(response.status,headers);res.end(new Uint8Array(await response.arrayBuffer()));
 }catch(error){res.writeHead(500);res.end(error.message);}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
let cookie='',browser,office,kiosk;
const errors=[],blockedExternal=[];
const call=async(action,data={},expected=true)=>{
 const response=await entry.fetch(new Request(origin+'/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 const result=rememberOperation(await response.json(),cookie);
 assert.equal(result.ok,expected,action+': '+(result.error||'unexpected success'));return result;
};
const person=id=>DB.raw.prepare('SELECT * FROM ck_attendance_people WHERE id=?').get(id);
const day=id=>DB.raw.prepare('SELECT * FROM ck_attendance_days WHERE id=?').get(id);
const events=id=>DB.raw.prepare("SELECT * FROM ck_attendance_events WHERE record_id=? AND action='sop_attendance_permanent_update' ORDER BY version").all(id);
const responseFor=(page,action)=>page.waitForResponse(response=>{
 if(!response.url().endsWith('/api'))return false;
 try{return response.request().postDataJSON()?.action===action;}catch{return false;}
});
const dashboard=()=>office.locator('#ck-labor-dashboard');
const editor=()=>dashboard().locator('[data-editor][open]');
async function submit(action){
 const pending=responseFor(office,'sop_attendance_'+action);
 await editor().locator('[type=submit]').click();
 const result=await(await pending).json();assert.equal(result.ok,true,result.error);
 await dashboard().locator('[data-editor]').waitFor({state:'hidden'});
 await dashboard().locator('[data-feedback]').filter({hasText:'已保存'}).waitFor();return result;
}
async function openManagement(){
 await dashboard().locator('[data-permanent]').click();
 await editor().locator('[data-badge-status]').waitFor();
}
async function closeManagement(){await editor().locator('[data-cancel]').click();await dashboard().locator('[data-editor]').waitFor({state:'hidden'});}
async function assertFits(page,label){
 const dimensions=await page.evaluate(()=>({viewport:innerWidth,page:document.documentElement.scrollWidth,dialogs:[...document.querySelectorAll('dialog[open]')].map(dialog=>({left:dialog.getBoundingClientRect().left,right:dialog.getBoundingClientRect().right}))}));
 assert.ok(dimensions.page<=dimensions.viewport,label+': page overflow '+JSON.stringify(dimensions));
 assert.ok(dimensions.dialogs.every(dialog=>dialog.left>=-1&&dialog.right<=dimensions.viewport+1),label+': dialog outside viewport '+JSON.stringify(dimensions));
}
async function assertDesktopActionsVisible(){
 assert.ok(await editor().locator('[data-badge-list] .ck-table-wrap').evaluate(container=>{
  const visible=container.getBoundingClientRect();
  return container.scrollLeft===0&&[...container.querySelectorAll('button')].every(button=>{
   const rect=button.getBoundingClientRect();return rect.left>=visible.left&&rect.right<=visible.right;
  });
 }),'Desktop management actions are visible without horizontal scrolling');
}
try{
 await call('sop_login',{sop_key:password});await confirmOperation(call,'Synthetic badge manager');
 browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 const officeContext=await browser.newContext({viewport:{width:1440,height:1000}}),kioskContext=await browser.newContext({viewport:{width:390,height:844}});
 await officeContext.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
 office=await officeContext.newPage();kiosk=await kioskContext.newPage();
 for(const page of [office,kiosk]){
  page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',route=>{if(new URL(route.request().url()).origin===origin)return route.continue();blockedExternal.push(new URL(route.request().url()).origin);return route.abort();});
  await page.addInitScript(()=>{window.__badgePrints=0;window.print=()=>{window.__badgePrints++;};});
 }
 await office.goto(origin+'/shuju/?tab=labor');
 await dashboard().locator('[data-register]').waitFor();
 assert.equal(await office.evaluate(()=>CKSession.user.role),'manager');
 const originalName='Synthetic same-name worker',renamed='Synthetic renamed worker',badges=['DAF-QA-BROWSER-A','DAF-QA-BROWSER-B'],people=[];
 for(const badge of badges){
  await dashboard().locator('[data-register]').click();
  await editor().locator('[name=name]').fill(originalName);await editor().locator('[name=agency]').selectOption('가온');await editor().locator('[name=badge]').fill(badge);
  people.push((await submit('register')).person);
  await office.waitForFunction(count=>window.__badgePrints===count,people.length);
 }
 const [first,second]=people,oldQR=first.badgeId+'|'+originalName;
 assert.notEqual(first.id,second.id);assert.notEqual(first.badgeId,second.badgeId);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_days').get().n,0,'Registration must not create attendance');
 await openManagement();await editor().locator('[data-badge-search]').fill(originalName);
 assert.equal(await editor().locator('[data-badge-person]').count(),2,'Same names retain two separately managed badges');
 for(const p of people)assert.match(await editor().locator('[data-badge-person="'+p.id+'"]').innerText(),new RegExp(p.badgeId));
 await office.screenshot({path:path.join(artifacts,'same-name-desktop.png'),fullPage:true});await closeManagement();

 // Make one explicitly synthetic prior-day fixture, using genuine checkin/checkout first.
 const checked=(await call('sop_attendance_checkin',{badge:oldQR,agency:'가온'})).record;
 await call('sop_attendance_checkout',{id:checked.id});
 const today=(await call('sop_attendance_config')).day;
 const yesterday=new Date(Date.parse(today+'T00:00:00Z')-86400000).toISOString().slice(0,10);
 DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_in=?,signed_out=? WHERE id=?').run(yesterday,new Date(yesterday+'T09:00:00+09:00').toISOString(),new Date(yesterday+'T18:00:00+09:00').toISOString(),checked.id);
 const historical={...day(checked.id)};

 await openManagement();await editor().locator('[data-edit-badge="'+first.id+'"]').click();
 await editor().locator('[name=name]').fill(renamed);await editor().locator('[name=agency]').selectOption('포레인');
 assert.equal(await editor().locator('[name=reason]').getAttribute('required'),'');
 await editor().locator('[type=submit]').click();assert.equal(events(first.id).length,0,'Empty audit reason cannot submit');
 await editor().locator('[name=reason]').fill('Synthetic verified name and default agency correction');
 const edited=(await submit('permanent_update')).person;
 assert.equal(edited.id,first.id);assert.equal(edited.badgeId,first.badgeId);assert.equal(edited.name,renamed);assert.equal(edited.agency,'포레인');assert.equal(edited.version,2);
 assert.deepEqual({...day(checked.id)},historical,'Profile edit preserves the complete historical attendance row');
 assert.equal(person(second.id).name,originalName,'Same-name neighbor must not change');
 await dashboard().locator('[data-date]').fill(yesterday);await dashboard().locator('[data-date]').dispatchEvent('change');
 const historyRow=dashboard().locator('[data-rows] tr').filter({hasText:first.badgeId});await historyRow.waitFor();
 assert.equal(await historyRow.locator('td').first().locator('strong').innerText(),originalName);assert.match(await historyRow.innerText(),/가온/);
 await historyRow.locator('[data-person]').click();await dashboard().locator('[data-detail-dialog][open]').waitFor();
 assert.match(await dashboard().locator('[data-detail-dialog] h2').innerText(),new RegExp(originalName));
 await office.screenshot({path:path.join(artifacts,'preserved-history-desktop.png'),fullPage:true});await dashboard().locator('[data-close-detail]').click();
 const lookedUp=await call('sop_attendance_lookup',{badge:oldQR});assert.equal(lookedUp.person.id,first.id);assert.equal(lookedUp.person.name,renamed);assert.equal(lookedUp.record,null);

 await openManagement();await editor().locator('[data-toggle-badge="'+first.id+'"]').click();
 await editor().locator('[name=reason]').fill('Synthetic temporary badge suspension');
 const disabled=(await submit('permanent_update')).person;assert.equal(disabled.enabled,false);assert.equal(disabled.id,first.id);
 await openManagement();await editor().locator('[data-badge-status]').selectOption('disabled');
 assert.equal(await editor().locator('[data-badge-person]').count(),1);assert.equal(await editor().locator('[data-badge-person]').getAttribute('data-badge-person'),first.id);
 assert.match(await editor().locator('[data-badge-person]').innerText(),/停用/);assert.equal(await editor().locator('[data-print-person]').count(),0);
 await assertDesktopActionsVisible();
 await office.screenshot({path:path.join(artifacts,'disabled-desktop.png'),fullPage:true});
 await office.setViewportSize({width:390,height:844});await assertFits(office,'390px disabled management');
 const table=editor().locator('[data-badge-list] .ck-table-wrap');
 assert.ok(await table.evaluate(element=>['auto','scroll'].includes(getComputedStyle(element).overflowX)),'Wide management table scrolls within its container');
 await office.screenshot({path:path.join(artifacts,'disabled-mobile.png'),fullPage:true});
 const scrolled=await table.evaluate(element=>{element.scrollLeft=element.scrollWidth;return element.scrollLeft;});
 assert.ok(scrolled>0,'Mobile management exposes its operation column by scrolling the table');
 assert.ok(await editor().locator('[data-toggle-badge="'+first.id+'"]').evaluate(button=>{const rect=button.getBoundingClientRect();return rect.left>=0&&rect.right<=innerWidth;}),'Restore control is reachable within the mobile viewport');
 await office.screenshot({path:path.join(artifacts,'disabled-mobile-actions.png'),fullPage:true});
 assert.equal((await call('sop_attendance_lookup',{badge:oldQR})).person.enabled,false);
 assert.match((await call('sop_attendance_checkin',{badge:oldQR,agency:'포레인'},false)).error,/停用/);

 // Exercise the actual kiosk with the original QR text while disabled.
 const priorCheckins=DB.raw.prepare("SELECT count(*) n FROM ck_attendance_events WHERE action='sop_attendance_checkin'").get().n;
 await kiosk.goto(origin+'/attendance/');await kiosk.locator('#ck-login-code').fill(password);await kiosk.locator('#ck-auth-gate button[type=submit]').click();
 await kiosk.locator('#terminal-content #fixed').click();await kiosk.locator('#code').fill(oldQR);await kiosk.locator('#terminal-content form button.primary').click();
 await kiosk.locator('#terminal-content h1').filter({hasText:renamed}).waitFor();
 await kiosk.locator('#terminal-content [role=alert]').filter({hasText:'停用'}).waitFor();
 assert.match(await kiosk.locator('#terminal-content .record-meta').innerText(),new RegExp(first.badgeId));
 assert.equal(await kiosk.locator('#terminal-content input[type=radio]').count(),0,'Disabled badge has no checkin agency selection');
 assert.equal(await kiosk.locator('#terminal-content button.primary').count(),0,'Disabled badge has no checkin confirmation button');
 assert.equal(await kiosk.locator('#terminal-content .success-panel').count(),0,'Disabled badge does not report successful checkin');
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_days WHERE person_id=?').get(first.id).n,1,'Disabled QR cannot create a new day');
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM ck_attendance_events WHERE action='sop_attendance_checkin'").get().n,priorCheckins,'Disabled lookup writes no checkin event');
 assert.deepEqual({...day(checked.id)},historical,'Disabled kiosk lookup leaves historical attendance unchanged');
 await assertFits(kiosk,'390px disabled old QR');await kiosk.screenshot({path:path.join(artifacts,'disabled-old-qr-mobile.png'),fullPage:true});

 await editor().locator('[data-toggle-badge="'+first.id+'"]').click();await editor().locator('[name=reason]').fill('Synthetic verified badge restoration');
 const restored=(await submit('permanent_update')).person;
 assert.equal(restored.id,first.id);assert.equal(restored.badgeId,first.badgeId);assert.equal(restored.enabled,true);assert.equal(restored.version,4);
 assert.deepEqual({...day(checked.id)},historical,'Disable and restoration preserve historical attendance');
 await openManagement();await editor().locator('[data-badge-status]').selectOption('enabled');assert.equal(await editor().locator('[data-badge-person]').count(),2);
 await editor().locator('[data-badge-search]').fill(first.badgeId);assert.equal(await editor().locator('[data-badge-person]').count(),1);
 assert.match(await editor().locator('[data-badge-person]').innerText(),new RegExp(renamed));await assertFits(office,'390px restored management');
 await office.screenshot({path:path.join(artifacts,'restored-mobile.png'),fullPage:true});
 await office.setViewportSize({width:1440,height:1000});await assertDesktopActionsVisible();await office.screenshot({path:path.join(artifacts,'restored-desktop.png'),fullPage:true});
 await office.screenshot({path:path.join(artifacts,'restored-desktop-actions.png'),fullPage:true});

 await kiosk.reload();await kiosk.locator('#terminal-content #fixed').click();await kiosk.locator('#code').fill(oldQR);await kiosk.locator('#terminal-content form button.primary').click();
 await kiosk.locator('#terminal-content h1').filter({hasText:renamed}).waitFor();await kiosk.locator('#terminal-content form button.primary').click();
 await kiosk.locator('#terminal-content .success-panel').waitFor();assert.match(await kiosk.locator('#terminal-content .success-panel h1').innerText(),new RegExp(renamed));
 const current=(await call('sop_attendance_lookup',{badge:oldQR})).record;
 assert.equal(current.personId,first.id);assert.equal(current.badgeId,first.badgeId);assert.equal(current.name,renamed);assert.equal(current.agency,'포레인');assert.equal(current.day,today);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_people').get().n,2);
 assert.deepEqual(events(first.id).map(event=>JSON.parse(event.after_json).change_reason),['Synthetic verified name and default agency correction','Synthetic temporary badge suspension','Synthetic verified badge restoration']);
 assert.deepEqual(errors,[]);assert.deepEqual(blockedExternal,[],'Acceptance must not contact external services');
 await assertFits(kiosk,'390px successful old QR');await kiosk.screenshot({path:path.join(artifacts,'restored-old-qr-mobile.png'),fullPage:true});
 console.log('PASS: real /shuju manager registration and same-name separation; reason-required edit; preserved historical attendance; disable/filter/re-enable same identity; old QR blocked while disabled and checks in after restoration; desktop and 390px screenshots. Artifacts: '+artifacts);
}catch(error){
 console.error('Browser errors:',errors);
 for(const [label,page]of [['office',office],['kiosk',kiosk]])if(page&&!page.isClosed()){await page.screenshot({path:path.join(artifacts,'failure-'+label+'.png'),fullPage:true}).catch(()=>{});console.error(label,await page.locator('body').innerText().catch(()=>''));}
 throw error;
}finally{
 await browser?.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();
}
