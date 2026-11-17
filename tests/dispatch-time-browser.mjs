// Actual built pages + authenticated Worker API. All people/records are synthetic memory data.
import assert from 'node:assert/strict';
import {mock} from 'node:test';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
mock.timers.enable({apis:['Date'],now:Date.now()-1800000});
const f=await ownedHomeFixture();f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';f.env.SOP_DISPATCH_SHARE_ENABLED='true';
const p=(await f.ok('office','sop_attendance_register',{name:'合成日当派审界面',agency:'가온'})).person;
await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成界面授权'});
p.record=(await f.ok('office','sop_attendance_checkin',{name:'合成临时工牌界面',agency:'가온'})).record;
await f.ok('office','sop_daily_dispatch_link',{person_id:p.id,badge:p.badgeId,daily_badge:p.record.badgeId,daily_person_id:p.record.personId,attendance_id:p.record.id,reason:'合成本人核对'});
await f.ok('C','sop_login',{badge:p.record.badgeId});
const employeeJob=await f.task('A','合成职员派审界面');await f.task('C','合成日当派审界面');mock.timers.tick(10*60000);
const assets=path.resolve('worker-v2/.sop-staging-assets'),evidence='/tmp/ck-dispatch-time-browser';fs.mkdirSync(evidence,{recursive:true});
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,'http://127.0.0.1');
  if(u.pathname.endsWith('/api')){const chunks=[];for await(const c of req)chunks.push(c);const r=await f.worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),f.env),headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));return;}
  let file=path.resolve(assets,'.'+decodeURIComponent(u.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
 }catch(e){res.writeHead(500);res.end(e.message);}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),errors=[];
const cookie=k=>({name:f.cookies[k].split('=')[0],value:f.cookies[k].slice(f.cookies[k].indexOf('=')+1),url:origin});
async function pageFor(scope,width){const ctx=await browser.newContext({viewport:{width,height:1000},acceptDownloads:true});await ctx.addCookies([cookie(scope)]);const page=await ctx.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));await page.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());return page;}
async function fits(page,selector){assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'page width overflow');if(selector){const b=await page.locator(selector).boundingBox();assert.ok(b.x>=-1&&b.x+b.width<=page.viewportSize().width+1,'dialog width overflow');assert.ok(b.y>=-1&&b.y+b.height<=page.viewportSize().height+1,'dialog height overflow');}}
async function csv(page,button){const waiting=page.waitForEvent('download');await button.click();const download=await waiting;return fs.readFileSync(await download.path(),'utf8');}
try{
 for(const width of [1440,390,320]){
  const page=await pageFor('office',width);await page.goto(origin+'/shuju/?tab=employees');const root=page.locator('.ck-employee-dashboard'),row=root.locator('tbody tr').filter({hasText:f.people.A.employeeNo});await row.locator('[data-status=dispatching]').waitFor();assert.match(await row.locator('[data-dispatch-time]').innerText(),/0:10/);
  await root.locator('[data-work-status-filter]').selectOption('dispatching');assert.equal(await row.count(),1);await root.locator('[data-work-status-filter]').selectOption('');
  await row.locator('[data-work-detail-open]').click();const detail=root.locator('[data-work-detail-dialog]');await detail.locator('[data-dispatch-time]').waitFor();assert.match(await detail.locator('[data-dispatch-time]').innerText(),/0:10/);await fits(page,'[data-work-detail-dialog]');await page.screenshot({path:evidence+'/employee-detail-'+width+'.png',fullPage:false});await detail.locator('[data-close-work-detail]').click();
  const exported=await csv(page,root.locator('[data-export]'));assert.match(exported,/派审分钟/);assert.match(exported,/派审待核实分钟/);assert.match(exported,/合成派审A/);
  await page.goto(origin+'/shuju/?tab=labor');const labor=page.locator('#ck-labor-dashboard'),daily=labor.locator('[data-rows] tr').filter({hasText:p.record.name});await daily.locator('[data-status=dispatching]').waitFor();assert.equal(await labor.locator('[data-dispatch-column]').count(),1);await daily.locator('[data-person]').click();const dd=labor.locator('[data-detail-dialog]');await dd.locator('[data-dispatch-time]').waitFor();assert.match(await dd.locator('[data-dispatch-time]').innerText(),/0:10/);assert.equal(await dd.locator('[data-void]').isDisabled(),true);await fits(page,'[data-detail-dialog]');await page.screenshot({path:evidence+'/daily-detail-'+width+'.png',fullPage:false});await dd.locator('[data-close-detail]').click();assert.match(await csv(page,labor.locator('[data-export]')),/派审分钟/);await page.context().close();
 }
 console.log('PASS 1440/390/320 employee + linked-daily boards, duty status/filter, separate timeline, CSV and protected attendance');
 const field=await pageFor('A',390);await field.goto(origin+'/001/?need='+employeeJob.id);await field.locator('[data-actions]').waitFor();assert.match(await field.getByText('成功派工或处理后自动记派审',{exact:false}).innerText(),/自动记派审/);assert.equal(await field.getByRole('button',{name:'开始派审',exact:true}).count(),0);
 const page=await pageFor('office',390);await page.goto(origin+'/shuju/?tab=employees');const root=page.locator('.ck-employee-dashboard'),row=root.locator('tbody tr').filter({hasText:f.people.A.employeeNo});await row.locator('[data-status=dispatching]').waitFor();const s=f.record(employeeJob.id);await field.evaluate(async b=>{const r=await CKSession.request('sop_task_people',b);if(!r.ok)throw Error(r.error);},{id:s.id,revision:s.revision,workers:[...s.workers,{id:f.people.A.badgeId,name:f.people.A.name}],lead_id:s.lead_id,reason:'合成浏览器本人下场',client_req_id:crypto.randomUUID()});await root.locator('[data-refresh]').click();await row.locator('[data-status=working]').waitFor();mock.timers.tick(10*60000);await root.locator('[data-refresh]').click();assert.match(await row.locator('[data-dispatch-time]').innerText(),/0:10/);
 const r=f.record(employeeJob.id);await f.ok('A','sop_task_people',{id:r.id,revision:r.revision,workers:r.workers.filter(w=>w.id!==f.people.A.badgeId),lead_id:r.lead_id,reason:'合成本人退出'});await root.locator('[data-refresh]').click();await row.locator('[data-status=dispatching]').waitFor();await f.ok('office','sop_attendance_break_start',{id:f.people.A.record.id});await root.locator('[data-refresh]').click();await row.locator('[data-status=rest]').waitFor();await f.ok('office','sop_attendance_break_end',{id:f.people.A.record.id});await root.locator('[data-refresh]').click();await row.locator('[data-status=dispatching]').waitFor();
 const last=f.record(employeeJob.id);await f.ok('A','sop_task_complete_review',{id:last.id,revision:last.revision,result:{quantity:2,unit:'箱'},decision:'pass',reason:'合成最终审核'});await root.locator('[data-refresh]').click();await row.locator('[data-status=waiting_dispatch]').waitFor();assert.match(await row.locator('[data-dispatch-time]').innerText(),/0:10/);await fits(page);await page.screenshot({path:evidence+'/state-switches-390.png',fullPage:false});assert.deepEqual(errors,[]);
 console.log('PASS authenticated field continuation, actual operation/duty exclusion, rest restoration and final waiting-dispatch state; no start button or browser timer');
}finally{await browser.close();await new Promise(r=>server.close(r));f.DB.raw.close();mock.timers.reset();}
