// Actual built /001 with cookie authentication, in-memory synthetic DB and native zoom.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
import {handleDailyScan,scanToday} from '../worker-v2/import-daily-scan.js';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};
const artifacts='/tmp/ck-import-daily-scan',assets=path.resolve('worker-v2/.sop-staging-assets');fs.mkdirSync(artifacts,{recursive:true});
let cookie='';
async function office(action,data={}){
 const r=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];
 const result=rememberOperation(await r.json(),cookie);assert.equal(result.ok,true,action+': '+result.error);return result;
}
await office('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await confirmOperation(office,'合成管理员');
const person=(await office('sop_attendance_employee_register',{employeeNo:'SYN-DAILY-SCAN',name:'合成长姓名进口扫描记录员 김가나다라마바사',department:'import'})).person;
await office('sop_attendance_checkin',{badge:person.badgeId});
const seedEnv={...env,SOP_REQUEST_USER:{id:'synthetic-seed',name:'合成长姓名进口扫描记录员 김가나다라마바사',role:'manager',scope:'office'}};
for(const day of ['2026-01-01','2026-01-02','2026-01-03'])await handleDailyScan({action:'sop_import_daily_scan_save',business_date:day,total:123456,version:0,client_req_id:crypto.randomUUID()},seedEnv);
const server=http.createServer(async(req,res)=>{
 try{
  const url=new URL(req.url,'http://127.0.0.1');
  if(url.pathname.endsWith('/api')){
   let data='';for await(const chunk of req)data+=chunk;
   const response=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,body:data}),env);
   const headers=Object.fromEntries(response.headers);if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();
   res.writeHead(response.status,headers);res.end(await response.text());return;
  }
  let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');
  if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
  res.setHeader('Content-Type',({'.js':'text/javascript','.css':'text/css','.html':'text/html; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
 }catch(e){res.writeHead(500);res.end(e.message);}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port,results=[],errors=[];
const specs=[{width:1440,zoom:1},{width:1280,zoom:1},{width:1440,zoom:1.25},{width:1280,zoom:1.25},{width:1440,zoom:1.5},{width:1280,zoom:1.5},{width:390,zoom:1},{width:320,zoom:1}];
try{
 for(const spec of specs){
  const profile=fs.mkdtempSync('/tmp/ck-daily-scan-zoom-'),ctx=await chromium.launchPersistentContext(profile,{executablePath:'/usr/bin/chromium',headless:true,viewport:{width:spec.width,height:1000},timezoneId:'America/Los_Angeles',args:['--no-sandbox']});
  try{
   const settings=await ctx.newPage();await settings.goto('chrome://settings/appearance');await settings.evaluate(z=>chrome.settingsPrivate.setDefaultZoom(z),spec.zoom);await settings.close();
   const p=await ctx.newPage();p.setDefaultTimeout(15000);p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>d.accept());
   await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
   await p.goto(origin+'/001/');await p.locator('#ck-login-code').fill(person.badgeId);await p.locator('#ck-auth-gate button[type=submit]').click();await p.locator('#page-home.active').waitFor();
   await p.locator('#page-home [onclick*="import_menu"]').click();await p.locator('#ck-import-daily-entry').click();await p.locator('#page-import_daily_scan.active [data-save]').waitFor();
   const page=p.locator('#page-import_daily_scan'),date=page.locator('[name=date]'),total=page.locator('[name=total]'),save=page.locator('[data-save]'),refresh=page.locator('[data-refresh]'),status=page.locator('[data-status]');
   const ready=()=>p.waitForFunction(()=>!document.querySelector('#page-import_daily_scan [data-save]').disabled);
   await ready();assert.equal(await date.inputValue(),scanToday());assert.equal(await page.locator('.ck-scan-metric>strong').innerText(),'—');
   const tag=spec.width+'-'+Math.round(spec.zoom*100),selected='2025-12-'+String(specs.indexOf(spec)+1).padStart(2,'0');
   await date.fill(selected);await date.dispatchEvent('change');await ready();assert.equal(await total.inputValue(),'');assert.match(await page.locator('[data-current]').innerText(),/未录入/);
   async function capture(surface){
    const metrics=await p.evaluate(()=>({viewport:innerWidth,dpr:devicePixelRatio,scrollWidth:document.documentElement.scrollWidth}));
    assert.ok(Math.abs(metrics.viewport-spec.width/spec.zoom)<2,JSON.stringify(metrics));assert.ok(Math.abs(metrics.dpr-spec.zoom)<.01);assert.ok(metrics.scrollWidth<=metrics.viewport+1,JSON.stringify(metrics));
    const file=path.join(artifacts,surface+'-'+tag+'.png');const cdp=await ctx.newCDPSession(p);try{const layout=await cdp.send('Page.getLayoutMetrics');const shot=await cdp.send('Page.captureScreenshot',{format:'png',fromSurface:true,captureBeyondViewport:true,clip:{...layout.contentSize,scale:1}});fs.writeFileSync(file,Buffer.from(shot.data,'base64'));}finally{await cdp.detach();}
    const bytes=fs.readFileSync(file);assert.equal(bytes.readUInt32BE(16),spec.width,'native screenshot must retain physical viewport');results.push({...spec,...metrics,surface,file,screenshot_width:bytes.readUInt32BE(16),screenshot_height:bytes.readUInt32BE(20)});
   }
   await capture('empty');
   const jobsBefore=DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n;
   if(spec.width===1440&&spec.zoom===1){
    for(const value of ['', '-1','1.5','1e3','9007199254740992']){
     await total.fill(value);await save.click();assert.match(await status.innerText(),/非负整数/);assert.equal(await total.inputValue(),value);
    }
    // The server commits, then the browser loses its response. Retry reuses the request ID.
    let lost=false;
    const lose=async route=>{const b=route.request().postDataJSON();if(b?.action==='sop_import_daily_scan_save'&&!lost){lost=true;await route.fetch();await route.abort('failed');}else await route.fallback();};
    await p.route('**/001/api',lose);await total.fill('0');await save.click();await ready();assert.match(await status.innerText(),/输入已保留/);assert.equal(await total.inputValue(),'0');await p.unroute('**/001/api',lose);
    await save.click();await ready();assert.match(await status.innerText(),/已保存/);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_import_daily_scan_events WHERE business_date=?').get(selected).n,1);
    // A second operator corrects while this form still holds v1.
    await handleDailyScan({action:'sop_import_daily_scan_save',business_date:selected,total:7,version:1,client_req_id:crypto.randomUUID()},seedEnv);
    await total.fill('12');await save.click();await ready();assert.match(await status.innerText(),/记录已被更正/);assert.equal(await total.inputValue(),'12');
    await refresh.click();await ready();assert.equal(await total.inputValue(),'12');assert.equal(await page.locator('[data-current] strong').innerText(),'7');
    await save.click();await ready();assert.equal(await page.locator('[data-current] strong').innerText(),'12');
    // Failed query leaves input intact and blocks a blind write.
    const failGet=route=>route.request().postDataJSON()?.action==='sop_import_daily_scan_get'?route.abort('failed'):route.fallback();
    await total.fill('19');await p.route('**/001/api',failGet);await refresh.click();await p.waitForFunction(()=>document.querySelector('#page-import_daily_scan [data-status]').textContent.includes('查询失败'));
    assert.equal(await total.inputValue(),'19');assert.equal(await save.isDisabled(),true);await p.unroute('**/001/api',failGet);
    await refresh.click();await ready();assert.equal(await total.inputValue(),'19');await save.click();await ready();
   }else{
    await total.fill('0');await save.click();await ready();assert.equal(await page.locator('[data-current] strong').innerText(),'0');
    await total.fill(spec.width===320?String(Number.MAX_SAFE_INTEGER):'123456789');await save.click();await ready();
   }
   await page.locator('.ck-scan-history summary').click();await capture('saved-history');
   assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_jobs').get().n,jobsBefore);
   const expected=await total.inputValue();
   await page.locator('[data-back]').click();await p.locator('#ck-import-daily-entry').click();await ready();assert.equal(await total.inputValue(),expected);
   // Reopen in a fresh page: stored data must survive, with no in-browser fixture state.
   await p.reload();await p.locator('#page-home.active').waitFor();await p.locator('#page-home [onclick*="import_menu"]').click();await p.locator('#ck-import-daily-entry').click();await ready();
   await date.fill(selected);await date.dispatchEvent('change');await ready();assert.equal(await total.inputValue(),expected);
   await page.locator('[data-back]').click();assert.equal(await p.locator('#page-import_menu button[onclick*="scan_pallet"]').count(),1);await p.locator('#page-import_menu button[onclick*="scan_pallet"]').click();await p.locator('#page-generic_job.active').waitFor();
   if(spec.width===320){
    DB.raw.prepare("UPDATE ck_access_sessions SET departments='[\"bulk\"]' WHERE scope='field'").run();
    await p.reload();await p.locator('#page-home.active').waitFor();await p.locator('#page-home [onclick*="import_menu"]').click();assert.equal(await p.locator('#ck-import-daily-entry').isVisible(),false);
   }
   console.log('PASS import daily result '+tag);
  }finally{await ctx.close();fs.rmSync(profile,{recursive:true,force:true});}
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'result.json'),JSON.stringify({ok:true,results,errors},null,2));console.log(JSON.stringify({ok:true,surfaces:results.length,artifacts}));
}finally{await new Promise(r=>server.close(r));DB.raw.close();}
