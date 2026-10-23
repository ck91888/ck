// Real Chromium, built field UI, real Worker handlers; synthetic local DB only.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
import {latestCutoff} from '../worker-v2/system-cutoff.js';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-occupancy-only')};
let cookie='';
async function call(action,data={}){
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];const r=rememberOperation(await response.json(),cookie);assert.equal(r.ok,true,JSON.stringify(r));return r;
}
await call('sop_login',{sop_key:'synthetic-occupancy-only'});await confirmOperation(call,'Synthetic operator');
const owner=(await call('sop_attendance_register',{name:'Synthetic dispatcher',agency:'CKIK'})).person;
await call('sop_attendance_checkin',{badge:owner.badgeId,agency:owner.agency});
await call('sop_daily_dispatch_update',{person_id:owner.id,badge:owner.badgeId,enabled:true,version:0,departments:['direct_ship'],reason:'Synthetic browser permission'});
const person=(await call('sop_attendance_checkin',{name:'Synthetic conflict worker',agency:'동인천'})).record;
DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status) VALUES('SYNTHETIC-OLD-JOB','change_order','direct_ship','paused')").run();
DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at) VALUES('SYNTHETIC-CLOCK','SYNTHETIC-OLD-JOB',?,?,?)").run(person.badgeId,person.name,new Date().toISOString());
const cutoffMode=process.env.CK_CUTOFF_BROWSER==='1';
if(cutoffMode){const old=new Date(Date.parse(latestCutoff(Date.now()))-3600000).toISOString(),guard=DB.raw.prepare("SELECT sql FROM sqlite_master WHERE name='ck_daily_badge_join_update'").get().sql;DB.raw.exec('DROP TRIGGER ck_daily_badge_join_update');try{DB.raw.prepare("UPDATE v2_ops_job_workers SET joined_at=? WHERE id='SYNTHETIC-CLOCK'").run(old);}finally{DB.raw.exec(guard);}DB.raw.prepare("UPDATE v2_ops_jobs SET created_at=? WHERE id='SYNTHETIC-OLD-JOB'").run(old);env.SOP_DAILY_CUTOFF_ENABLED='true';}
const before=DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all(),assets=path.resolve('worker-v2/.sop-staging-assets');
const server=http.createServer(async(req,res)=>{try{
 const u=new URL(req.url,'http://127.0.0.1');
 if(u.pathname.endsWith('/api')){const chunks=[];for await(const c of req)chunks.push(c);const r=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env),headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));return;}
 let file=path.resolve(assets,'.'+decodeURIComponent(u.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
 }catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port,browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),context=await browser.newContext({viewport:{width:390,height:844}}),page=await context.newPage(),errors=[],alerts=[];
page.on('pageerror',e=>errors.push(e.message));page.on('dialog',async d=>{alerts.push(d.message());await d.accept();});
const evidence='/tmp/ck-work-occupancy-evidence';fs.mkdirSync(evidence,{recursive:true});
try{
 const response=await context.request.post(origin+'/001/api',{data:{action:'sop_login',badge:owner.badgeId}});assert.equal((await response.json()).ok,true);
 await page.goto(origin+'/001/');await page.waitForFunction(()=>window.CKOpenFieldLabor&&window.CKSession?.user);
 if(cutoffMode){
  assert.equal(DB.raw.prepare("SELECT status FROM v2_ops_jobs WHERE id='SYNTHETIC-OLD-JOB'").get().status,'system_closed');const closed=DB.raw.prepare('SELECT * FROM v2_ops_job_workers').get();assert.equal(closed.leave_reason,'system_cutoff:unverified');assert.equal(closed.left_at,latestCutoff(Date.now()));assert.deepEqual(JSON.parse(DB.raw.prepare("SELECT before_json FROM ck_system_cutoff_audit WHERE kind='segment'").get().before_json),{...before[0]});
  await page.evaluate(()=>CKOpenFieldLabor());const personCard=page.locator('[data-peoplecards] article').filter({hasText:person.name});await personCard.locator('[data-status=unassigned]').waitFor();await personCard.getByRole('button',{name:'明细与休息 / 상세·휴식',exact:true}).click();await page.getByText('系统截止工时待核实 / 시스템 종료 시간 확인',{exact:true}).waitFor();await page.screenshot({path:evidence+'/cutoff-released-390.png',fullPage:true});await page.locator('dialog[open] [data-close-detail]').click();
  await page.evaluate(()=>goPage('generic_job',{flow_stage:'outbound',biz_class:'direct_ship',job_type:'change_order',title:'换单操作 / 송장 교체'}));await page.locator('#gjStateIdle button[onclick="startGenericJob(this)"]').click();const form=page.locator('dialog[open] form');await form.locator('[data-staff-badge]').fill(person.badgeId+'|'+person.name);await form.locator('[data-staff-add]').click();await form.getByRole('button',{name:'确认人员并开始',exact:true}).click();await page.waitForFunction(()=>document.querySelector('#gjStateWorking').style.display!=='none');assert.equal(alerts.length,0);assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,1);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);await page.screenshot({path:evidence+'/cutoff-new-work-390.png',fullPage:true});await page.reload();await page.waitForFunction(()=>window.CKSession?.user);assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_jobs WHERE status='working'").get().n,1);assert.deepEqual(errors,[]);console.log('PASS Chromium cutoff: historical worker released with original evidence, unverified details visible, real change-order starts without historical conflict, refresh keeps one new clock and no results.');
 }else{
 await page.evaluate(()=>CKOpenFieldLabor());const card=page.locator('[data-peoplecards] article').filter({hasText:person.name});await card.locator('[data-status=occupancy_conflict]').waitFor();assert.match(await card.innerText(),/暂停任务仍有未结束计时/);assert.doesNotMatch(await card.innerText(),/未分配作业/);
 for(const width of [390,1440]){await page.setViewportSize({width,height:width===390?844:1000});await page.screenshot({path:evidence+'/conflict-'+width+'.png',fullPage:true});assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));}
 await page.setViewportSize({width:390,height:844});await page.evaluate(()=>goPage('generic_job',{flow_stage:'outbound',biz_class:'direct_ship',job_type:'change_order',title:'换单操作 / 송장 교체'}));
 const button=page.locator('#gjStateIdle button[onclick="startGenericJob(this)"]');await button.click();let form=page.locator('dialog[open] form');await form.locator('[data-staff-badge]').fill(person.badgeId+'|'+person.name);await form.locator('[data-staff-add]').click();await form.locator('[name=minutes]').fill('19');await form.getByRole('button',{name:'确认人员并开始',exact:true}).click();await button.waitFor({state:'visible'});await page.waitForFunction(()=>!document.querySelector('#gjStateIdle button[onclick="startGenericJob(this)"]').disabled);
 assert.equal(alerts.length,1);assert.match(alerts[0],/已暂停/);assert.match(alerts[0],/KST/);assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all(),before);
 await button.click();form=page.locator('dialog[open] form');await form.locator('[data-staff-people]').filter({hasText:person.name}).waitFor();assert.equal(await form.locator('[name=minutes]').inputValue(),'19');await page.screenshot({path:evidence+'/retry-390.png',fullPage:true});await form.locator('[data-cancel]').click();
 await page.reload();await page.waitForFunction(()=>window.CKOpenFieldLabor&&window.CKSession?.user);await page.evaluate(()=>CKOpenFieldLabor());await page.locator('[data-peoplecards] article').filter({hasText:person.name}).locator('[data-status=occupancy_conflict]').waitFor();assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all(),before);assert.deepEqual(errors,[]);
 console.log('PASS Chromium: conflict card with original paused clock, mobile/desktop layout, change-order failure unlock, retained staff/minutes on retry, refresh preserves all clocks; local synthetic data only.');
 }
}catch(error){await page.screenshot({path:evidence+'/failure.png',fullPage:true}).catch(()=>{});throw error;}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
