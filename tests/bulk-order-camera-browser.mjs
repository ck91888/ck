// Actual built office/field pages and real Worker; all data and R2 objects are isolated synthetic fixtures.
import assert from 'node:assert/strict';import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const fixtureAdminCode=crypto.randomUUID();
const DB=database(),objects=new Map(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest(fixtureAdminCode),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);}}},root=path.resolve('worker-v2/.sop-staging-assets');let cookie='';
const call=async(action,b={})=>{const r=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];const v=rememberOperation(await r.json(),cookie);assert.equal(v.ok,true,action+': '+v.error);return v;};
await call('sop_login',{sop_key:fixtureAdminCode});await confirmOperation(call,'资料组隔离客服');const staff=(await call('sop_attendance_employee_register',{employeeNo:'CARGO-QA',name:'虚构派审员',department:'bulk'})).person;await call('sop_attendance_checkin',{badge:staff.badgeId});const loader=(await call('sop_attendance_employee_register',{employeeNo:'CARGO-LOAD',name:'虚构装货员',department:'bulk'})).person;await call('sop_attendance_checkin',{badge:loader.badgeId});
const server=http.createServer(async(req,res)=>{try{const u=new URL(req.url,'http://127.0.0.1');if(u.pathname.endsWith('/api')){const chunks=[];for await(const c of req)chunks.push(c);const r=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env),headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));return;}let file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(!file.startsWith(root+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));}catch(e){res.writeHead(500);res.end(e.message);}});await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const evidence=process.env.CK_SCAN_EVIDENCE||'/tmp/ck-order-camera-browser';fs.mkdirSync(evidence,{recursive:true});
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox','--use-fake-ui-for-media-stream']}),errors=[],results=[];
try{
 const needs=[];for(let i=0;i<2;i++)needs.push(await call('sop_need_create',{source_type:'inventory',department:'bulk',supply_chain_no:'SYNTHETIC-SCAN-'+i,customer:'合成扫码客户',title:'隔离相机选单'+i,instructions:'只用合成单据'}));
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:900},permissions:['camera']}),page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push({width,message:error.message}));
  await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
  await page.addInitScript(()=>{
   window.__scanCode='';window.__scanMode='normal';window.__scanStreams=[];
   navigator.mediaDevices.getUserMedia=async()=>{
    if(window.__scanMode==='denied')throw new DOMException('Synthetic denied','NotAllowedError');
    const canvas=document.createElement('canvas');canvas.width=640;canvas.height=480;const ctx=canvas.getContext('2d');let image;
    if(window.__scanCode&&window.__scanMode!=='code39'){const qr=qrcode(0,'M');qr.addData(window.__scanCode);qr.make();image=new Image();image.src=qr.createDataURL(3,16);await image.decode();}
    const draw=()=>{ctx.fillStyle='white';ctx.fillRect(0,0,640,480);if(image)ctx.drawImage(image,260,180,120,120);if(window.__scanMode==='code39'){const patterns={'*':'nwnnwnwnn','1':'wnnwnnnnw','2':'nnwwnnnnw','3':'wnwwnnnnn','4':'nnnwwnnnw','5':'wnnwwnnnn','6':'nnwwwnnnn'};const bars=[...('*'+window.__scanCode+'*')].map(c=>patterns[c]);let x=140;ctx.fillStyle='black';for(const pattern of bars){for(let i=0;i<pattern.length;i++){const width=pattern[i]==='w'?8:3;if(i%2===0)ctx.fillRect(x,170,width,140);x+=width;}x+=3;}};};draw();const stream=canvas.captureStream(12),timer=setInterval(draw,75);window.__scanStreams.push(stream);for(const track of stream.getTracks()){const stop=track.stop.bind(track);track.stop=()=>{clearInterval(timer);stop();};}return stream;
   };
  });
  const stop=()=>page.waitForFunction(()=>window.__scanStreams.every(s=>s.getTracks().every(t=>t.readyState==='ended')));
  const setCode=(code='',mode='normal')=>page.evaluate(({code,mode})=>{window.__scanCode=code;window.__scanMode=mode;},{code,mode});
  const open=async(external,code)=>{await page.evaluate(external=>goPage('bulk_op',{external}),external);await page.locator('[data-scanform] [data-code]').fill(code);await page.locator('[data-scanform] button[type=submit]').click();await page.locator('[data-bulk-code]').waitFor();};
  const selected=()=>page.locator('[data-bulk-selected] .ck-bulk-selected');
  try{
   await page.goto(origin+'/001/');await page.addScriptTag({url:origin+'/shared/qrcode.min.js'});await page.locator('#ck-login-code').fill(staff.badgeId);await page.locator('#ck-auth-gate button[type=submit]').click();await page.locator('#page-home').waitFor({state:'visible'});
   for(const external of [false,true]){
    await open(external,external?'SYNTHETIC-CAMERA-FIRST':needs[0].id);
    if(external)await page.locator('[data-people] [name=customer]').fill('合成未提交客户');
    const code=external?'SYNTHETIC-CAMERA-SECOND':needs[1].id;await setCode(code);
    const button=page.locator('.ck-order-selection [data-camera-start]');await button.click();
    await page.waitForFunction(()=>document.querySelectorAll('[data-bulk-selected] .ck-bulk-selected').length===2);await stop();assert.equal(await selected().count(),2);assert.match(await page.locator('[data-order-status]').textContent(),/已加入/);
    // Scanning only selects orders. No task, result, time segment, or stock is written.
    assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);
    await button.click();await stop();assert.equal(await selected().count(),2);
    if(external)assert.equal(await page.locator('[data-people] [name=customer]').inputValue(),'合成未提交客户');
    await setCode('', 'denied');await button.click();await page.locator('.ck-order-selection [data-camera-status]').filter({hasText:'权限被拒绝'}).waitFor();assert.equal(await page.locator('[data-bulk-code]').isEnabled(),true);
    if(external){await page.locator('[data-bulk-code]').fill('SYNTHETIC-MANUAL-THIRD');await page.locator('[data-bulk-code]').press('Enter');await page.waitForFunction(()=>document.querySelectorAll('[data-bulk-selected] .ck-bulk-selected').length===3);await setCode('123456','code39');await button.click();await page.waitForFunction(()=>document.querySelectorAll('[data-bulk-selected] .ck-bulk-selected').length===4);await stop();assert.match(await selected().last().textContent(),/123456/);}
    await setCode('');await button.click();await page.locator('.ck-order-selection video').waitFor();await page.locator('.ck-order-selection [data-camera-close]').click();await stop();
    await button.click();await page.locator('.ck-order-selection video').waitFor();await page.evaluate(()=>goPage('home'));await stop();
    results.push({width,external,realQr:true,realCode39:external,duplicate:true,permissionFallback:true,manualEnter:external,cancel:true,navigationStops:true});
   }
   await open(true,'SYNTHETIC-CAMERA-FIRST');await page.locator('[data-people] [name=customer]').fill('合成派工客户');await setCode('SYNTHETIC-CAMERA-SECOND');await page.locator('.ck-order-selection [data-camera-start]').click();await page.waitForFunction(()=>document.querySelectorAll('[data-bulk-selected] .ck-bulk-selected').length===2);await stop();await page.locator('[data-bulk-customer]').fill('合成第二客户');
   await page.screenshot({path:path.join(evidence,'order-camera-'+width+'.png'),fullPage:true});
   assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'no horizontal page overflow');
   if(width===390){
    await page.locator('[data-staff-badge]').fill(loader.badgeId+'|'+loader.name);await page.locator('[data-staff-add]').click();await page.locator('[data-people] button[type=submit]').click();await page.locator('[data-bulk-orders] .ck-bulk-order').first().waitFor();assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_bulk_work_batch_items').get().n,2);assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at='' ").get().n,1);
   }
  }catch(error){console.error('DIAGNOSTIC',JSON.stringify(await page.evaluate(()=>({qr:typeof qrcode,streams:window.__scanStreams.length,status:document.querySelector('.ck-order-selection [data-camera-status]')?.textContent}))),errors);await page.screenshot({path:path.join(evidence,'failure-'+width+'.png'),fullPage:true}).catch(()=>{});throw error;}finally{await context.close();}
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(evidence,'result.json'),JSON.stringify({results,errors},null,2));console.log(JSON.stringify({passed:results.length,widths:[1440,390],realQr:true,dispatchAfterConfirmation:true,errors},null,2));
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
