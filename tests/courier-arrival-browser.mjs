// Actual /002 page and Worker; all plans, uploads and receipts use isolated synthetic storage.
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

const require=createRequire(import.meta.url);
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),objects=new Map();
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('courier-arrival-fixture'),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);}}};
let cookie='';
async function call(action,body={}){
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 const result=rememberOperation(await response.json(),cookie);assert.equal(result.ok,true,action+': '+(result.message||result.error));return result;
}
await call('sop_login',{sop_key:'courier-arrival-fixture'});await confirmOperation(call,'Synthetic service');
const codes=Array.from({length:12},(_,i)=>'301000092'+String(i+1).padStart(3,'0'));
const person=(await call('sop_attendance_checkin',{name:'Synthetic arrival scanner',agency:'가온'})).record;
const batch=(await call('sop_courier_receive',{operation:'start',workers:[{id:person.badgeId,name:person.name}],lead_id:person.badgeId})).batch;
const scan=tracking_no=>call('sop_courier_receive',{batch_id:batch.id,scanner_badge:person.badgeId,tracking_no,owner:'8-1'});
for(const code of codes.slice(0,3))await scan(code);
const originalReceipts=DB.raw.prepare('SELECT * FROM ck_courier_receipts ORDER BY tracking_no').all();
const root=path.resolve('worker-v2/.sop-staging-assets'),artifacts=path.join(tmpdir(),'ck-courier-arrival');fs.mkdirSync(artifacts,{recursive:true});
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
const context=await browser.newContext({viewport:{width:1440,height:1000}}),page=await context.newPage(),errors=[],dialogs=[];
page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));page.on('dialog',async dialog=>{dialogs.push(dialog.message());await dialog.accept(dialog.type()==='prompt'?'Synthetic cancellation':undefined);});
await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
await context.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
const submit=page.locator('button[onclick="submitInbound(this)"]'),input=page.locator('#ibcLinesBody textarea'),preview=page.locator('.courier-arrival-preview');
async function form(customer,numbers,biz='bulk'){
 await page.locator('#mainTabs [data-tab=inbound]').click();await page.locator('#btnNewInbound').click();
 await page.locator('#ibc-customer').fill(customer);await page.locator('.ibc-biz-chk[value='+biz+']').check();
 await page.locator('#ibcLinesBody .ibc-line-type').selectOption('courier');await input.fill(numbers.join('\n'));
 if(biz==='direct_ship')await page.locator('#ck-ibc-external').fill(customer+'-WMS');
}
async function ready(count,received){await page.waitForFunction(({count,received})=>{const x=document.querySelector('.courier-arrival-preview');return x&&!x.hasAttribute('aria-busy')&&x.querySelectorAll('tbody tr').length===count&&x.querySelector('[data-arrival-summary]')?.textContent.includes(received+' / '+count);},{count,received});}
async function save(){const response=page.waitForResponse(r=>r.request().method()==='POST'&&r.request().postData()?.includes('"action":"v2_inbound_plan_create"'));await submit.click();const result=await(await response).json();await page.waitForFunction(()=>!window._actionLocks.submitInbound);return result;}
const state=id=>DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(id);
const quantity=id=>DB.raw.prepare("SELECT actual_qty FROM v2_inbound_plan_lines WHERE plan_id=? AND unit_type='courier'").get(id).actual_qty;
async function detail(customer){await page.locator('#mainTabs [data-tab=inbound]').click();await page.locator('#inboundListBody .list-item').filter({hasText:customer}).click();await page.locator('#inboundDetailBody [data-arrival-summary]').waitFor();}
try{
 await page.goto(origin+'/002/');await form('Synthetic all arrived',codes.slice(0,2),'direct_ship');await ready(2,2);
 assert.match(await preview.innerText(),/已到仓/);assert.match(await preview.innerText(),new RegExp(batch.batch_no));assert.match(await preview.innerText(),/Synthetic arrival scanner/);
 for(const receipt of originalReceipts.slice(0,2))assert.match(await preview.innerText(),new RegExp(new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Seoul',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date(receipt.received_at))));
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,0);await page.screenshot({path:path.join(artifacts,'all-arrived-before-save.png'),fullPage:true});
 const all=await save();assert.equal(all.ok,true,JSON.stringify(all));assert.equal(state(all.id).status,'arrived_pending_putaway');assert.equal(quantity(all.id),2);assert.equal(DB.raw.prepare('SELECT status FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').get(all.id).status,'pending');
 assert.deepEqual(DB.raw.prepare('SELECT * FROM ck_courier_receipts ORDER BY tracking_no').all(),originalReceipts);
 await detail('Synthetic all arrived');assert.match(await page.locator('#inboundDetailBody .courier-progress').innerText(),new RegExp(batch.batch_no));assert.match(await page.locator('#inboundDetailBody [data-arrival-summary]').innerText(),/已到仓.*2 \/ 2/);
 await form('Synthetic partial arrival',codes.slice(2,4));await ready(2,1);assert.match(await preview.innerText(),/部分到仓/);assert.match(await preview.innerText(),/未到仓/);await page.screenshot({path:path.join(artifacts,'partial-before-save.png'),fullPage:true});
 const partial=await save();assert.equal(partial.ok,true,JSON.stringify(partial));assert.equal(state(partial.id).status,'pending');assert.equal(quantity(partial.id),1);
 await detail('Synthetic partial arrival');assert.match(await page.locator('#inboundDetailBody [data-arrival-summary]').innerText(),/部分到仓.*1 \/ 2/);
 await scan(codes[3]);assert.equal(quantity(partial.id),2);assert.equal(state(partial.id).status,'completed'); // Confirmed non-putaway rule: docs/20260922-courier-receiving.md.
 await form('Synthetic plan first',codes.slice(4,6),'direct_ship');await ready(2,0);assert.match(await preview.innerText(),/未到仓/);const first=await save();assert.equal(first.ok,true,JSON.stringify(first));assert.equal(state(first.id).status,'pending');
 await scan(codes[4]);assert.equal(quantity(first.id),1);assert.equal(state(first.id).status,'pending');await scan(codes[5]);assert.equal(quantity(first.id),2);assert.equal(state(first.id).status,'arrived_pending_putaway');
 await detail('Synthetic plan first');assert.match(await page.locator('#inboundDetailBody [data-arrival-summary]').innerText(),/已到仓.*2 \/ 2/);
 // An old lookup cannot overwrite newer input, invalid input, a changed unit or a closed form.
 let held,release,delivered;const intercepted=new Promise(r=>held=r),done=new Promise(r=>delivered=r);
 await page.route('**/api',async route=>{const body=route.request().postDataJSON();if(body?.action==='sop_courier_lookup'&&body.tracking_nos[0]===codes[0]){const response=await route.fetch();held();await new Promise(r=>release=r);await route.fulfill({response});delivered();return;}return route.continue();});
 await form('Synthetic stale lookup',[codes[0]]);await intercepted;await input.fill(codes[6]);await ready(1,0);release();await done;await page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));assert.equal(await preview.locator('tbody tr td').first().innerText(),codes[6]);assert.doesNotMatch(await preview.innerText(),/Synthetic arrival scanner/);await page.unroute('**/api');
 let navHeld,navRelease,navDelivered;const navPending=new Promise(r=>navHeld=r),navDone=new Promise(r=>navDelivered=r);
 await page.route('**/api',async route=>{const body=route.request().postDataJSON();if(body?.action==='sop_courier_lookup'&&body.tracking_nos[0]===codes[8]){const response=await route.fetch();navHeld();await new Promise(r=>navRelease=r);await route.fulfill({response});navDelivered();return;}return route.continue();});
 await input.fill(codes[8]);await navPending;await form('Synthetic new form',[codes[9]]);await ready(1,0);navRelease();await navDone;await page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));assert.equal(await preview.locator('tbody tr td').first().innerText(),codes[9]);await page.unroute('**/api');
 await input.fill('not-a-waybill');await preview.locator('.courier-error').waitFor();assert.equal(await preview.locator('tbody tr').count(),0);assert.equal(await submit.isEnabled(),true);
 // Failure is visible and retryable, never a false "not arrived" conclusion.
 await page.route('**/api',async route=>{if(route.request().postDataJSON()?.action==='sop_courier_lookup')return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'Synthetic lookup unavailable'})});return route.continue();});
 await input.fill(codes[6]);await preview.locator('[data-arrival-retry]').waitFor();assert.match(await preview.innerText(),/尚未确认/);assert.equal(await preview.locator('tbody tr').count(),0);assert.equal(await input.inputValue(),codes[6]);await page.unroute('**/api');await preview.locator('[data-arrival-retry]').click();await ready(1,0);
 // A competing plan created after preview is rejected by the server; inputs remain usable.
 const rival=await call('v2_inbound_plan_create',{customer:'Synthetic rival',biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:[codes[6]]}]});const rejected=await save();assert.equal(rejected.ok,false);assert.equal(await submit.isEnabled(),true);assert.equal(await input.inputValue(),codes[6]);assert.equal(DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(codes[6]).plan_id,rival.id);
 await input.fill(codes[7]);await ready(1,0);await input.fill(codes[6]);await ready(1,0);assert.match(await preview.innerText(),/已关联/);
 await page.setViewportSize({width:390,height:844});await page.evaluate(()=>{setLang('ko');applyLang();});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.ok((await input.boundingBox()).width>240);assert.match(await preview.locator('[data-arrival-summary]').innerText(),/未到仓.*0 \/ 1/);assert.equal(await preview.locator('.courier-claim-conflict').isVisible(),true);await page.screenshot({path:path.join(artifacts,'conflict-mobile-ko.png'),fullPage:true});
 await page.locator('#ibcLinesBody .ibc-line-type').selectOption('carton');await preview.waitFor({state:'hidden'});await page.locator('#ibcLinesBody .ibc-line-type').selectOption('courier');await ready(1,0);await page.locator('#ibcLinesBody tr button').click();await preview.waitFor({state:'hidden'});
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:true,pageErrors:errors,dialogs,scenarios:['receipt-before-plan preview with immutable batch/scanner/time','all vs partial physical-arrival labels','putaway stays pending','documented non-putaway completion','plan-before-receipt equivalence','late lookup input isolation','invalid input clears old preview','lookup failure and retry','stale available claim rejected by server','mobile Korean no page overflow','unit switch and row removal clear preview']},null,2));console.log('PASS actual /002 arrival preview + receipt-before/after plan + immutable receipt metadata + partial/all progress + stale/failure/claim guards. Artifacts: '+artifacts);
}catch(error){console.error('JS errors',errors);console.error('PAGE',await page.locator('body').innerText());await page.screenshot({path:path.join(artifacts,'failure.png'),fullPage:true}).catch(()=>{});throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
