// Actual built field pages and Worker; loopback-only synthetic handover fixtures.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import {createRequire} from 'node:module';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const assets=path.resolve('worker-v2/.sop-staging-assets'),artifacts='/tmp/ck-field-owned-home-evidence';fs.mkdirSync(artifacts,{recursive:true});assert.ok(fs.existsSync(path.join(assets,'001/index.html')),'Build staging assets first.');
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),cases=[],errors=[];
const body=request=>{try{return request.postDataJSON();}catch{return null;}};
const home=page=>page.locator('#page-home'),cards=page=>home(page).locator('[data-dispatch-job]');
function cookie(value,origin){return {name:value.split('=')[0],value:value.slice(value.indexOf('=')+1),url:origin};}
async function frames(page){await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));}
try{
 for(const width of [1440,390]){
  const f=await ownedHomeFixture(),a=await f.task('A','合成A本人任务'),b=await f.task('B','合成B本人任务',f.people.A);
  const server=http.createServer(async(req,res)=>{try{
   const url=new URL(req.url,'http://127.0.0.1');if(url.pathname.endsWith('/api')){const chunks=[];for await(const chunk of req)chunks.push(chunk);const result=await f.worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),f.env),headers=Object.fromEntries(result.headers);if(result.headers.getSetCookie().length)headers['set-cookie']=result.headers.getSetCookie();res.writeHead(result.status,headers);res.end(new Uint8Array(await result.arrayBuffer()));return;}
   let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
  }catch(error){res.writeHead(500);res.end(error.message);}});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port,contexts={},pages={};
  try{
   for(const role of ['A','B']){const context=await browser.newContext({viewport:{width,height:width===390?844:1000}});contexts[role]=context;await context.addCookies([cookie(f.cookies[role],origin)]);const page=await context.newPage();pages[role]=page;page.setDefaultTimeout(30000);page.on('pageerror',error=>errors.push(error.message));await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());await page.goto(origin+'/001/',{waitUntil:'domcontentloaded'});await cards(page).waitFor();assert.equal(await cards(page).count(),1);assert.equal(await cards(page).first().getAttribute('data-dispatch-job'),role==='A'?a.id:b.id);assert.match(await home(page).innerText(),/本人负责的在途任务/);}
   const page=pages.A,recipient=pages.B,clocks=f.clocks();await cards(page).click();await page.getByRole('button',{name:'转移派审员 / 담당자 이관',exact:true}).click();const dialog=page.locator('dialog').filter({has:page.getByRole('heading',{name:'转移派审员 / 배정·검수 담당자 이관',exact:true})});await dialog.waitFor();
   await dialog.locator('[name=target_badge]').fill(f.people.B.badgeId+'|不会作为身份的姓名');await dialog.locator('[name=reason]').fill('合成原负责人扫码交接');await dialog.locator('button[type=submit]').click();await dialog.waitFor({state:'hidden'});await home(page).waitFor({state:'visible'});await page.waitForFunction(()=>document.querySelector('[data-dispatch-freshness]')?.dataset.freshness==='current');assert.equal(await cards(page).count(),0);assert.deepEqual(f.clocks(),clocks);
   await recipient.locator('[data-dispatch-refresh]').click();await recipient.waitForFunction(()=>document.querySelectorAll('[data-dispatch-job]').length===2);assert.deepEqual((await cards(recipient).evaluateAll(nodes=>nodes.map(n=>n.dataset.dispatchJob))).sort(),[a.id,b.id].sort());for(const label of await cards(recipient).locator('.ck-task-dispatcher').allTextContents())assert.match(label,/合成派审B/);
   assert.equal(f.record(a.id).owner_id,f.people.B.id);assert.equal((await f.call('A','sop_task_pause',{id:a.id,revision:f.record(a.id).revision,reason:'合成旧负责人测试'})).ok,false);assert.equal((await f.ok('B','sop_owner_transfer_options',{id:a.id})).can_transfer,true);
   await recipient.screenshot({path:path.join(artifacts,'after-transfer-'+width+'.png'),fullPage:true});cases.push('actual owner badge handover '+width+': source list empty, recipient list owns both tasks, clocks unchanged and old owner denied');console.log('PASS '+cases.at(-1));
   const stale=await f.task('A','合成身份切换缓存任务');await page.locator('[data-dispatch-refresh]').click();await page.locator('[data-dispatch-job="'+stale.id+'"]').waitFor();let entered,release,used=false;const started=new Promise(resolve=>entered=resolve),held=new Promise(resolve=>release=resolve);
   await page.route('**/api',async route=>{if(!used&&body(route.request())?.action==='sop_dispatch_list'){used=true;const response=await route.fetch();entered();await held;await route.fulfill({response});return;}await route.continue();});await page.evaluate(()=>{void window.loadDispatchTasks();});await started;
   await contexts.A.addCookies([cookie(f.cookies.B,origin)]);await page.evaluate(()=>window.dispatchEvent(new Event('focus')));await page.locator('#ck-auth-gate').waitFor();assert.equal(await page.locator('[data-dispatch-job]').count(),0);assert.equal(await page.evaluate(()=>window.CKSession.user),null);release();await frames(page);assert.equal(await page.locator('[data-dispatch-job]').count(),0);
   await page.locator('#ck-login-code').fill(f.people.B.badgeId);await page.locator('#ck-auth-gate button[type=submit]').click();await page.waitForFunction(()=>window.CKSession?.user?.name==='合成派审B'&&document.querySelectorAll('[data-dispatch-job]').length===2);assert.equal(await page.locator('[data-dispatch-job="'+stale.id+'"]').count(),0);assert.match(await page.locator('#headerWorker').innerText(),/合成派审B/);await page.screenshot({path:path.join(artifacts,'identity-switch-'+width+'.png'),fullPage:true});cases.push('identity switch '+width+': old cards cleared behind login gate, late A response discarded, badge B reload shows only B ownership');console.log('PASS '+cases.at(-1));
  }catch(error){for(const [role,page]of Object.entries(pages))await page.screenshot({path:path.join(artifacts,'failure-'+role+'-'+width+'.png'),fullPage:true}).catch(()=>{});throw error;}
  finally{for(const context of Object.values(contexts))await context.close();await new Promise(resolve=>server.close(resolve));f.DB.raw.close();}
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'result.json'),JSON.stringify({ok:true,cases,errors},null,2)+'\n');
}finally{await browser.close();}
