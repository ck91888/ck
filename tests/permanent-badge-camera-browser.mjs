// Loopback only: real built UI, vendored QR decoder and Worker; synthetic identities and QR camera video.
// The decoder is not mocked. Its two video.play() sites only swallow AbortError after that video
// is closed/detached; early pagehide below checks this cancellation without hiding other errors.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('permanent-camera-isolated')};
const root=fs.mkdtempSync(path.join(os.tmpdir(),'ck-permanent-camera-assets-')),evidence='/tmp/ck-permanent-camera-browser',requests=[];
fs.cpSync(path.resolve('worker-v2/.sop-staging-assets'),root,{recursive:true});fs.mkdirSync(evidence,{recursive:true});
let cookie='';
const call=async(action,body={})=>{
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 const value=rememberOperation(await response.json(),cookie);assert.equal(value.ok,true,value.error);return value;
};
await call('sop_login',{sop_key:'permanent-camera-isolated'});await confirmOperation(call,'虚构相机验收员');
const fixed=(await call('sop_attendance_register',{badge:'DAF-CAMERA-BROWSER',name:'虚构长期相机甲',agency:'가온'})).person;
const disabled=(await call('sop_attendance_register',{badge:'DAF-CAMERA-DISABLED',name:'虚构停用相机乙',agency:'가온'})).person;
await call('sop_attendance_permanent_update',{id:disabled.id,version:disabled.version,name:disabled.name,agency:disabled.agency,enabled:false,reason:'合成浏览器验收停用'});
const employee=(await call('sop_attendance_employee_register',{employeeNo:'CAMERA-BROWSER',name:'虚构职员相机丙',department:'bulk'})).person;
const count=personId=>DB.raw.prepare('SELECT count(*) n FROM ck_attendance_days WHERE person_id=?').get(personId).n;
const signedOut=personId=>DB.raw.prepare('SELECT signed_out FROM ck_attendance_days WHERE person_id=?').get(personId)?.signed_out;
const server=http.createServer(async(req,res)=>{
 try{
  const url=new URL(req.url,'http://127.0.0.1');
  if(url.pathname.endsWith('/api')){
   const chunks=[];for await(const chunk of req)chunks.push(chunk);const body=Buffer.concat(chunks);requests.push(JSON.parse(body));
   const response=await worker.fetch(new Request('http://'+req.headers.host+url.pathname,{method:'POST',headers:req.headers,body}),env),headers=Object.fromEntries(response.headers);
   if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();res.writeHead(response.status,headers);res.end(await response.text());return;
  }
  let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep))throw Error('Path denied');
  if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
  res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
 }catch(error){res.writeHead(500);res.end(error.message);}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port,browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox','--use-fake-device-for-media-stream','--use-fake-ui-for-media-stream']});
const context=await browser.newContext({viewport:{width:390,height:844},permissions:['camera']}),page=await context.newPage(),errors=[];let stage='startup';
page.on('pageerror',error=>errors.push({stage,message:error.message,stack:error.stack}));
await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
await page.addInitScript(()=>{
 window.__cameraStreams=[];window.__cameraMode='normal';window.__cameraQR='';
 navigator.mediaDevices.getUserMedia=async()=>{
  if(window.__cameraMode==='denied')throw new DOMException('Synthetic permission denied','NotAllowedError');
  const canvas=document.createElement('canvas');canvas.width=640;canvas.height=480;const ctx=canvas.getContext('2d');let picture;
  if(window.__cameraQR){qrcode.stringToBytes=qrcode.stringToBytesFuncs['UTF-8'];const qr=qrcode(0,'M');qr.addData(window.__cameraQR);qr.make();picture=new Image();picture.src=qr.createDataURL(8,16);await picture.decode();}
  const draw=()=>{ctx.fillStyle='#fff';ctx.fillRect(0,0,640,480);if(picture)ctx.drawImage(picture,Math.round((640-picture.width)/2),Math.round((480-picture.height)/2));};
  draw();const stream=canvas.captureStream(12),timer=setInterval(draw,75);window.__cameraStreams.push(stream);
  for(const track of stream.getTracks()){const stop=track.stop.bind(track);track.stop=()=>{clearInterval(timer);stop();};}
  return stream;
 };
});
const setCamera=(code='',mode='normal')=>page.evaluate(({code,mode})=>{window.__cameraQR=code;window.__cameraMode=mode;},{code,mode});
const stopped=()=>page.waitForFunction(()=>window.__cameraStreams.every(stream=>stream.getTracks().every(track=>track.readyState==='ended')));
const live=()=>page.waitForFunction(()=>window.__cameraStreams.some(stream=>stream.getTracks().some(track=>track.readyState==='live')));
const fixedEntry=async()=>{await page.locator('[data-kind=daily]').click();await page.locator('#tab-in').click();await page.locator('#fixed').click();};
const scan=async(host,code)=>{await setCamera(code);await page.locator(host+' [data-camera-start]').click();};
try{
 await page.goto(origin+'/attendance/');await page.locator('#ck-login-code').fill('permanent-camera-isolated');await page.locator('#ck-auth-gate button[type=submit]').click();await page.locator('#fixed').click();
 await page.screenshot({path:path.join(evidence,'permanent-entry-mobile.png'),fullPage:true});
 assert.equal(await page.evaluate(()=>window.__cameraStreams.length),0);
 await scan('#terminal-content',fixed.badgeId+'|伪造二维码姓名');
 await page.locator('#terminal-content h1').filter({hasText:fixed.name}).waitFor();await stopped();
 assert.equal(count(fixed.id),0);assert.equal(requests.filter(r=>r.action==='sop_attendance_lookup').length,1);assert.doesNotMatch(await page.locator('#terminal-content').textContent(),/伪造二维码姓名/);
 await page.screenshot({path:path.join(evidence,'permanent-review-mobile.png'),fullPage:true});
 await page.locator('#terminal-content button.primary').click();await page.locator('#terminal-content .success-panel').waitFor();assert.equal(count(fixed.id),1);
 assert.equal(requests.filter(r=>r.action==='sop_attendance_checkin').length,1);

 // Repeated scan is a read of the existing attendance; it cannot create another check-in.
 await fixedEntry();await scan('#terminal-content',fixed.badgeId);await page.locator('#terminal-content .success-panel').waitFor();await stopped();
 assert.equal(count(fixed.id),1);assert.equal(requests.filter(r=>r.action==='sop_attendance_checkin').length,1);
 await fixedEntry();await scan('#terminal-content',disabled.badgeId);await page.locator('#terminal-content [role=alert]').filter({hasText:'工牌已停用'}).waitFor();await stopped();
 assert.equal(await page.locator('#terminal-content button.primary').count(),0);assert.equal(count(disabled.id),0);
 await page.screenshot({path:path.join(evidence,'permanent-disabled-mobile.png'),fullPage:true});

 // Existing daily check-out and employee in/out use the same actual decoder and keep explicit confirmation.
 await page.locator('#tab-out').click();await scan('#terminal-content',fixed.badgeId);await page.waitForFunction(code=>document.getElementById('code').value===code,fixed.badgeId);await stopped();assert.equal(signedOut(fixed.id),'');
 await page.locator('#terminal-content button.primary').click();await page.locator('#terminal-content .success-panel').waitFor();assert.ok(signedOut(fixed.id));
 await page.locator('[data-kind=employee]').click();
 for(const mode of ['in','out']){
  if(mode==='out'){await page.locator('.ck-employee-terminal [data-next]').click();await page.locator('.ck-employee-terminal [data-out]').click();}
  await scan('.ck-employee-terminal',employee.badgeId+'|伪造职员姓名');await page.waitForFunction(code=>document.getElementById('employee-term').value===code,employee.badgeId);await stopped();
  if(mode==='in')assert.equal(count(employee.id),0);else assert.equal(signedOut(employee.id),'');
  await page.locator('.ck-employee-terminal button.primary').click();await page.locator('.ck-employee-terminal .success-panel').waitFor();
 }
 assert.equal(count(employee.id),1);assert.ok(signedOut(employee.id));

 // Denial and explicit close/reopen release real MediaStream tracks in all four entry modes.
 for(const mode of ['fixed','daily-out','employee-in','employee-out']){
  stage=mode+' lifecycle';
  const host=mode.startsWith('employee')?'.ck-employee-terminal':'#terminal-content';
  if(mode==='fixed')await fixedEntry();
  else if(mode==='daily-out'){await page.locator('[data-kind=daily]').click();await page.locator('#tab-out').click();}
  else{await page.locator('[data-kind=employee]').click();await page.locator(mode==='employee-in'?'[data-in]':'[data-out]').click();}
  await setCamera('','denied');await page.locator(host+' [data-camera-start]').click();await page.locator(host+' [data-camera-status]').filter({hasText:'权限被拒绝'}).waitFor();
  assert.equal(await page.locator(host+' input').first().isEnabled(),true);assert.equal(await page.locator(host+' [data-camera-start]').isEnabled(),true);
  if(mode==='fixed')await page.screenshot({path:path.join(evidence,'permanent-permission-denied-mobile.png'),fullPage:true});
  await setCamera();await page.locator(host+' [data-camera-start]').click();await live();await page.locator(host+' [data-camera-close]').click();await stopped();
  await page.locator(host+' [data-camera-start]').click();await live();await page.locator(mode.startsWith('employee')?'[data-kind=daily]':'[data-kind=employee]').click();await stopped();
 }
 stage='pagehide';await fixedEntry();await setCamera();await page.locator('#terminal-content [data-camera-start]').click();await live();
 await page.evaluate(()=>window.dispatchEvent(new Event('pagehide')));await stopped();
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
 await page.setViewportSize({width:1280,height:900});await page.screenshot({path:path.join(evidence,'permanent-entry-desktop.png'),fullPage:true});
 assert.deepEqual(errors,[]);
 const result={passed:true,decoder:'Real shared/html5-qrcode.min.js with two scoped video.play cancellation catches',database:'Isolated in-memory Worker/SQLite',physicalCameraTested:false,streamCount:await page.evaluate(()=>window.__cameraStreams.length),allTracksStopped:true,unhandledErrors:errors.length,permanentCheckins:count(fixed.id),disabledCheckins:count(disabled.id),employeeCheckins:count(employee.id),evidence};
 fs.writeFileSync(path.join(evidence,'result.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();fs.rmSync(root,{recursive:true,force:true});}
