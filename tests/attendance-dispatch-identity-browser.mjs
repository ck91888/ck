import {fillPickWorkloadFixture} from './pick-workload-fixture.mjs';
// Actual built 001/002 pages, real Worker, synthetic in-memory database and R2 only.
import assert from 'node:assert/strict';
import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {tmpdir} from 'node:os';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),objects=new Map(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('attendance-identity-fixture'),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);}}};
let cookie='';const call=async(action,b={})=>{const r=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];const v=rememberOperation(await r.json(),cookie);assert.equal(v.ok,true,action+': '+v.error);return v;};
await call('sop_login',{sop_key:'attendance-identity-fixture'});await confirmOperation(call,'Synthetic service');
const staff=(await call('sop_attendance_employee_register',{employeeNo:'IDENTITY-QA',name:'Synthetic field',department:'bulk'})).person;await call('sop_attendance_checkin',{badge:staff.badgeId});
const root=path.resolve('worker-v2/.sop-staging-assets'),artifacts=path.join(tmpdir(),'ck-attendance-identity');fs.mkdirSync(artifacts,{recursive:true});
const server=http.createServer(async(req,res)=>{try{const u=new URL(req.url,'http://127.0.0.1');if(u.pathname.endsWith('/api')||u.pathname.endsWith('/api/file')){const chunks=[];for await(const c of req)chunks.push(c);const r=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env),headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));return;}let file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(!file.startsWith(root+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));}catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),officeCtx=await browser.newContext({viewport:{width:1440,height:1000}}),fieldCtx=await browser.newContext({viewport:{width:390,height:844}}),office=await officeCtx.newPage(),field=await fieldCtx.newPage(),errors=[];
for(const p of [office,field]){p.setDefaultTimeout(15000);p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>{console.log('UI dialog:',d.message());d.accept();});await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());}
await officeCtx.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);

const daily=(await call('sop_attendance_checkin',{name:'合成日当甲',agency:'가온'})).record;
const requests=[],alerts=[];field.on('request',r=>{if(r.postData()?.includes('"action":"sop_native_start"'))requests.push(JSON.parse(r.postData()));});field.on('dialog',d=>alerts.push(d.message()));
async function assign(code){
 await field.locator('[onclick="submitStartPickByDocs(this)"]').click();
 const crew=field.locator('dialog[open]');await crew.locator('[data-staff-badge]').fill(code);await crew.locator('[data-staff-badge]').press('Enter');
 await fillPickWorkloadFixture(crew);
 const response=field.waitForResponse(r=>r.request().postData()?.includes('"action":"sop_native_start"'));
 await crew.locator('[type=submit]').click();return (await response).json();
}
try{
 await field.goto(origin+'/001/');await field.locator('#ck-login-code').fill(staff.badgeId);await field.locator('#ck-auth-gate button[type=submit]').click();await field.locator('#ck-auth-gate').waitFor({state:'hidden'});
 await field.locator('#page-home.active').waitFor();await field.evaluate(()=>goPage('order_op_menu'));await field.locator('[onclick="goPage(\'pick_direct\')"]').click();
 await field.locator('#pickStartDocInput').fill('SYNTHETIC-PICK-IDENTITY');await field.locator('#pickStartDocInput').press('Enter');
 const rejected=await assign(daily.badgeId+'\u200b|'+daily.name);
 assert.equal(rejected.ok,false);assert.ok(rejected.error.includes(daily.badgeId+'\\u{200B}'));assert.ok(rejected.error.includes(daily.day));
 await field.waitForFunction(()=>!document.querySelector('dialog[open]'));
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,0);
 assert.ok(alerts.some(x=>x.includes(daily.badgeId+'\\u{200B}')&&x.includes(daily.day)));
 const result=await assign(daily.badgeId+'|'+daily.name+'\r\n');
 assert.equal(result.ok,true,result.error);assert.equal(requests.at(-1).workers[0].id,daily.badgeId);
 assert.equal(requests.at(-1).workers[0].name,daily.name);assert.equal(requests.at(-1).workers.length,1);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''").get(daily.badgeId).n,1);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM ck_attendance_days WHERE worker_id=?').get(daily.badgeId).n,1);
 assert.deepEqual(errors,[]);
 console.log('PASS actual /001 picking: invalid scanned identity shows exact escaped ID and KST date without writes; fresh scanner-Enter selection with CR/LF starts using the exact signed-in daily ID.');
}catch(e){await field.screenshot({path:path.join(artifacts,'failure.png'),fullPage:true});throw e;}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
