// Existing built authentication client + local Worker on isolated fixture pages.
// Every URL, including the hostile sibling, is fulfilled locally. No live traffic.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {mock} from 'node:test';
import entry from '../worker-v2/staging-entry.js';
import {restrictionFixture} from './test-reset-restriction-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const assets=path.resolve('worker-v2/.sop-staging-assets'),out=process.env.CK_RESET_AUTH_OUT||'/tmp/ck-reset-auth-browser';fs.mkdirSync(out,{recursive:true});
const origin='https://ck-v2-api-sop-staging.ck91888.workers.dev',hostile='https://sibling.ck91888.workers.dev';
const html='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/shared/access-ui.css"><script src="/shared/sop-rollout.js"></script><script src="/shared/sop-session.js"></script></head><body><main id="ready">Local authentication fixture</main><script>CKSession.ready.then(()=>document.getElementById("ready").textContent="Signed in");</script></body></html>';
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),results=[];
try{for(const width of [1440,390])for(const compat of [false,true]){
 const cleanup=[],f=await restrictionFixture({mock,after:fn=>cleanup.push(fn)},{active:true,partial:true}),errors=[],seen=[];let context;
 try{
  if(compat)Object.assign(f.env,{SOP_ACCESS_CONTROL:'false',SOP_PUBLIC_TEST_ACCESS:'false',SOP_USERS_JSON:JSON.stringify([{id:'synthetic-browser-compat',name:'合成兼容登录员',role:'manager',departments:['bulk'],key:'synthetic-browser-compat-key'}])});
  await f.q('CREATE TABLE ck_backup_synthetic_numbers AS SELECT * FROM sop_document_numbers').run();await f.q('DELETE FROM sop_document_numbers').run();const db=f.env.DB;f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};
  const before=await f.snapshot(compat?[]:['ck_access_sessions','ck_access_attempts','ck_office_operation_context']);
  context=await browser.newContext({viewport:{width,height:900}});
  await context.route('**/*',async route=>{
   const r=route.request(),url=new URL(r.url());
   if(url.origin===hostile)return route.fulfill({contentType:'text/html',body:'<!doctype html><a id="logout" href="'+origin+'/api?action=sop_logout">Logout attack</a>'});
   if(url.origin!==origin)return route.abort();
   if(url.pathname==='/local-auth')return route.fulfill({contentType:'text/html',body:html});
   if(url.pathname.startsWith('/shared/')){const file=path.resolve(assets,'.'+url.pathname);if(!file.startsWith(assets+path.sep))return route.abort();return route.fulfill({contentType:file.endsWith('.js')?'text/javascript':'text/css',body:fs.readFileSync(file)});}
   try{const headers=await r.allHeaders(),body=r.postDataBuffer(),response=await entry.fetch(new Request(r.url(),{method:r.method(),headers,...(body?.length?{body}:{})}),f.env);seen.push({method:r.method(),query:url.search,action:r.postDataJSON()?.action||url.searchParams.get('action'),cookie:headers.cookie||'',site:headers['sec-fetch-site'],status:response.status});await route.fulfill({status:response.status,headers:Object.fromEntries(response.headers),body:Buffer.from(await response.arrayBuffer())});}catch(e){errors.push(e.message);await route.abort();}
  });
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+'/local-auth');await page.locator('#ck-login-code').waitFor();
  await page.locator('#ck-login-code').fill('wrong-synthetic-key');const denied=page.waitForResponse(r=>r.url()===origin+'/api'&&r.request().postDataJSON()?.action==='sop_login');await page.locator('.ck-login-submit').click();assert.equal((await(await denied).json()).ok,false);await page.locator('.ck-login-submit:enabled').waitFor();assert.equal((await context.cookies()).filter(c=>/^ck_(office_access|sop_session)$/.test(c.name)).length,0);
  await page.locator('#ck-login-code').fill(compat?'synthetic-browser-compat-key':'synthetic-owned-home-fixture');await page.locator('.ck-login-submit').click();
  if(!compat){await page.locator('#ck-operator-name').fill('合成维护登录员');await page.locator('[data-operator-confirm]').click();}
  await page.waitForFunction(()=>CKSession.user?.role==='manager'&&document.getElementById('ready').textContent==='Signed in');
  const cookie=(await context.cookies()).find(c=>/^ck_(office_access|sop_session)$/.test(c.name));assert.ok(cookie?.httpOnly&&cookie.secure&&cookie.sameSite==='Strict');assert.equal(await f.snapshot(compat?[]:['ck_access_sessions','ck_access_attempts','ck_office_operation_context']),before);
  const sessions=await f.rows('SELECT * FROM ck_access_sessions ORDER BY token_hash'),attacker=await context.newPage();attacker.on('pageerror',e=>errors.push(e.message));
  const getLogin=await attacker.goto(origin+'/api?'+new URLSearchParams({action:'sop_login',sop_key:compat?'synthetic-browser-compat-key':'synthetic-owned-home-fixture'}));assert.equal(getLogin.status(),403);
  await attacker.goto(hostile+'/');const navigation=attacker.waitForNavigation();await attacker.locator('#logout').click();assert.equal((await navigation).status(),403);
  // Local route fulfillment does not expose Chromium's generated Sec-Fetch-Site.
  // Require the actual Strict victim cookie: the navigation must reach the real
  // Worker with the authenticated session, not pass by omitting credentials.
  const attempt=seen.findLast(r=>r.method==='GET'&&r.action==='sop_logout');assert.ok(attempt.cookie.includes(cookie.name+'='+cookie.value),'the hostile same-site navigation must actually carry the victim cookie');assert.deepEqual(await f.rows('SELECT * FROM ck_access_sessions ORDER BY token_hash'),sessions);
  assert.equal((await page.evaluate(()=>CKSession.request('sop_identity'))).user.role,'manager');await page.reload();await page.waitForFunction(()=>CKSession.user?.role==='manager');assert.equal(await f.snapshot(compat?[]:['ck_access_sessions','ck_access_attempts','ck_office_operation_context']),before);assert.deepEqual(await f.rows('SELECT * FROM sop_document_numbers'),[]);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);
  assert.equal(await page.evaluate(async()=>{const r=await fetch('/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'v2_inbound_plan_create',customer:'must not write'})});return r.status;}),503);
  await page.screenshot({path:path.join(out,`auth-${width}-${compat}.png`),fullPage:true});await page.locator('.ck-stage-banner button').click();await page.locator('#ck-login-code').waitFor();assert.equal((await context.cookies()).some(c=>c.name===cookie.name),false);assert.deepEqual(errors,[]);
  results.push({width,compat,login:true,wrongPasswordDenied:true,operatorConfirmation:!compat,getLoginDenied:true,crossSiblingLogoutDenied:true,victimCookieActuallySent:true,coldIdentityDoesNotBackfill:true,normalLogout:true,errors});console.log('PASS local maintenance auth '+width+' compatibility='+compat);
 }catch(e){await context?.pages()[0]?.screenshot({path:path.join(out,`failure-${width}-${compat}.png`),fullPage:true}).catch(()=>{});fs.writeFileSync(path.join(out,`failure-${width}-${compat}.json`),JSON.stringify({error:e.stack,errors,requests:seen.map(({cookie,...r})=>({...r,cookiePresent:!!cookie}))},null,2));throw e;}
 finally{await context?.close();for(const fn of cleanup)await fn();mock.timers.reset();}
}fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({ok:true,localOnly:true,isolatedAuthenticationPages:true,results},null,2));}finally{await browser.close();}
