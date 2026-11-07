// Synthetic local measurements only; never connect this fixture to a remote database.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import {gzipSync} from 'node:zlib';
import {createRequire} from 'node:module';
import entry from '../worker-v2/staging-entry.js';
import {database} from './d1-adapter.mjs';
const require=createRequire(import.meta.url);
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const root=path.resolve(process.env.CK_PERF_ASSET_ROOT||'worker-v2/.sop-staging-assets');
const output=process.env.CK_PERF_OUTPUT||'/tmp/ck-staging-load';fs.mkdirSync(output,{recursive:true});
const delayMs=Number(process.env.CK_PERF_DELAY_MS||150),requests=[],DB=database();
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:'Synthetic-load-only',ASSETS:{async fetch(request){
 let file=path.join(root,new URL(request.url).pathname);if(!fs.existsSync(file))return new Response('Missing',{status:404});
 if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
 const type=file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':file.endsWith('.json')?'application/json':'text/html';
 return new Response(fs.readFileSync(file),{headers:{'Content-Type':type}});
}}};
const server=http.createServer(async(req,res)=>{try{
 const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks);let action='';try{action=JSON.parse(body).action||'';}catch{}
 const started=performance.now(),r=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body}:{})}),env);
 const headers=Object.fromEntries(r.headers),raw=Buffer.from(await r.arrayBuffer()),bytes=gzipSync(raw);headers['content-encoding']='gzip';headers['content-length']=bytes.length;
 const row={path:req.url,action,status:r.status,rawBytes:raw.length,gzipBytes:bytes.length,appMs:performance.now()-started,cache:headers['cache-control']||'',timing:headers['server-timing']||''};requests.push(row);
 // Fixed synthetic response latency makes request scheduling visible. This is not a China RTT measurement.
 if(delayMs)await new Promise(resolve=>setTimeout(resolve,delayMs));
 res.writeHead(r.status,headers);res.end(bytes);
}catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port,browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),errors=[],report={synthetic:true,delayMs,cold:[],modules:[]};
function summary(rows){const assets=rows.filter(x=>/\.(js|css)(\?|$)/.test(x.path));return{assetRequests:assets.length,rawBytes:assets.reduce((n,x)=>n+x.rawBytes,0),gzipBytes:assets.reduce((n,x)=>n+x.gzipBytes,0),apiActions:rows.filter(x=>x.action).map(x=>x.action)};}
try{
 for(let i=0;i<3;i++){
  const context=await browser.newContext({viewport:{width:i===2?390:1440,height:900}}),page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
  const first=requests.length,started=performance.now();await page.goto(origin+'/office-login/?next=/002/');await page.locator('#ck-login-code').waitFor();
  const sample={...summary(requests.slice(first)),gateMs:Math.round(performance.now()-started),navigation:await page.evaluate(()=>({domContentLoaded:performance.getEntriesByType('navigation')[0].domContentLoadedEventEnd,load:performance.getEntriesByType('navigation')[0].loadEventEnd}))};report.cold.push(sample);
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));await page.screenshot({path:path.join(output,'login-'+(i===2?'390':'1440')+'.png')});
  if(i===0){
   await page.locator('#ck-login-code').fill('Synthetic-load-only');await page.locator('.ck-login-submit').click();await page.locator('#ck-operator-name').fill('Synthetic load operator');await page.locator('[data-operator-confirm]').click();await page.locator('#mainTabs').waitFor();await page.waitForFunction(()=>!!window.CKSession?.user);
   await page.waitForTimeout(600);report.modules.push({path:'/002/',...summary(requests.slice(first))});
   for(const target of ['/003/','/shuju/','/002/']){
    const start=requests.length;await page.goto(origin+target);await page.waitForFunction(()=>!!window.CKSession?.user&&!document.documentElement.classList.contains('ck-auth-pending'));await page.waitForTimeout(700);
    const sample={path:target,...summary(requests.slice(start))};report.modules.push(sample);
    if(target==='/shuju/'){
     assert.equal(sample.apiActions.filter(x=>x==='v2_dashboard_realtime_overview').length,1,'first dashboard render performs one overview');
     assert.equal(sample.apiActions.filter(x=>x==='sop_attendance_summary').length,1);
     assert.match(await page.locator('#statActiveWorkers').locator('..').textContent(),/已签到未签退日当/);
     const refresh=requests.length;await page.evaluate(()=>loadRealtime());
     assert.deepEqual(summary(requests.slice(refresh)).apiActions,['v2_dashboard_realtime_overview','sop_attendance_summary'],'later refresh still updates both sources');
    }
   }
   await page.screenshot({path:path.join(output,'office-1440.png')});
   const reload=requests.length;await page.reload();await page.waitForFunction(()=>!!window.CKSession?.user);await page.waitForTimeout(700);report.reload=summary(requests.slice(reload));
   assert.equal(report.reload.assetRequests,2,'Only the existing two unhashed dynamic modules are fetched again');
   const api=requests.filter(x=>x.action);assert.ok(api.every(x=>!x.cache.includes('public')),'Authenticated responses must never be publicly cached');
   await page.evaluate(()=>CKSession.logout());await page.locator('#ck-login-code').waitFor();assert.equal(new URL(page.url()).pathname,'/office-login/');
  }
  await context.close();
 }
 if(process.env.CK_PERF_VISUAL==='1'){
  report.visual=[];
  for(const spec of [{width:1440,zoom:1},{width:1280,zoom:1},{width:1440,zoom:1.25},{width:1280,zoom:1.25},{width:1440,zoom:1.5},{width:1280,zoom:1.5},{width:390,zoom:1},{width:320,zoom:1}]){
   const profile=fs.mkdtempSync('/tmp/ck-load-zoom-'),context=await chromium.launchPersistentContext(profile,{executablePath:'/usr/bin/chromium',headless:true,viewport:{width:spec.width,height:900},args:['--no-sandbox']});
   try{
    const settings=await context.newPage();await settings.goto('chrome://settings/appearance');await settings.evaluate(z=>new Promise(r=>chrome.settingsPrivate.setDefaultZoom(z,r)),spec.zoom);await settings.close();
    const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+'/office-login/?next=/002/');await page.locator('#ck-login-code').waitFor();
    for(const surface of ['login','operator']){
     if(surface==='operator'){await page.locator('#ck-login-code').fill('Synthetic-load-only');await page.locator('.ck-login-submit').click();await page.locator('#ck-operator-name').waitFor();await page.locator('#ck-operator-name').fill('合成验证员 Synthetic operator');}
     const dimensions=await page.evaluate(()=>({width:innerWidth,dpr:devicePixelRatio,page:document.documentElement.scrollWidth}));assert.ok(Math.abs(dimensions.width-spec.width/spec.zoom)<2);assert.ok(Math.abs(dimensions.dpr-spec.zoom)<.01);assert.ok(dimensions.page<=dimensions.width+1);
     const file=path.join(output,surface+'-'+spec.width+'-'+Math.round(spec.zoom*100)+'.png'),cdp=await context.newCDPSession(page);
     const shot=await cdp.send('Page.captureScreenshot',{format:'png',fromSurface:true});await cdp.detach();fs.writeFileSync(file,Buffer.from(shot.data,'base64'));report.visual.push({...spec,surface,...dimensions,file});
    }
   }finally{await context.close();fs.rmSync(profile,{recursive:true,force:true});}
  }
 }
 assert.deepEqual(errors,[]);assert.ok(requests.every(x=>x.status<500));
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM sop_records').get().n,0,'No business records created');
 report.requests=requests;fs.writeFileSync(path.join(output,'report.json'),JSON.stringify(report,null,2));
 console.log(JSON.stringify({...report,requests:undefined},null,2));
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
