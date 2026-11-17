// Actual /001, strict Worker authentication, Chromium native zoom, synthetic memory DB.
import assert from 'node:assert/strict';import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {tmpdir} from 'node:os';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};
const artifacts=path.resolve(process.env.CK_PARTIAL_ARRIVAL_ARTIFACTS||path.join(tmpdir(),'ck-partial-arrival'));fs.mkdirSync(artifacts,{recursive:true});let cookie='';
async function call(action,data={}){const r=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];const j=rememberOperation(await r.json(),cookie);assert.equal(j.ok,true,action+': '+JSON.stringify(j));return j;}
await call('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await confirmOperation(call,'合成理货上架派审员');
const people=[];for(const [employeeNo,name] of [['STAGE-BR-D','合成派审员 김하늘'],['STAGE-BR-A','合成长姓名跨境理货员 김가나다라마바사'],['STAGE-BR-B','合成上架乙 이지은'],['STAGE-OUTSIDE','合成无关职员']]){const p=(await call('sop_attendance_employee_register',{employeeNo,name,department:'direct_ship'})).person;p.record=(await call('sop_attendance_checkin',{badge:p.badgeId})).record;people.push(p);}
const [dispatcher,a,b,outsider]=people;
const assets=path.resolve('worker-v2/.sop-staging-assets');
const server=http.createServer(async(req,res)=>{try{const url=new URL(req.url,'http://127.0.0.1');if(url.pathname.endsWith('/api')){const chunks=[];for await(const c of req)chunks.push(c);const r=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env);const headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));return;}let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));}catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port,errors=[],scenarios=[],visual=[],requests=[];
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const body=r=>r.headers()['content-type']?.includes('application/json')?r.postDataJSON():null;
const record=name=>{scenarios.push(name);console.log('PASS '+name);};
const modal=p=>p.locator('dialog[open].ck-workflow');
async function field(context){const p=await context.newPage();p.setDefaultTimeout(15000);p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>d.accept());p.on('request',r=>{if(body(r))requests.push(body(r));});await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());await p.goto(origin+'/001/');await p.locator('#ck-login-code').fill(dispatcher.badgeId);await p.locator('#ck-auth-gate button[type=submit]').click();await p.locator('#page-home.active').waitFor();return p;}

async function add(p,person){await modal(p).locator('[data-staff-badge]').fill(person.badgeId+'|'+person.name);await modal(p).locator('[data-staff-badge]').press('Enter');await modal(p).locator('[data-staff-people]').getByText(person.name,{exact:false}).first().waitFor();}
async function start(p,code,type,staff){
 await p.evaluate(()=>{CKClearNativeJob();goPage('inbound_menu');});
 await p.locator('#page-inbound_menu [onclick*="'+type+'"]').click();
 await p.locator('#inboundCodeInput').fill(code);await p.evaluate(()=>resolveInboundCode());
 assert.equal(await p.evaluate(()=>_ibResolvedKind),'system');
 await p.locator('[onclick="startInbound(this)"]').click();await modal(p).waitFor();
 for(const person of staff)await add(p,person);
 const result=p.waitForResponse(r=>r.url().endsWith('/api')&&r.request().postDataJSON()?.action==='sop_native_start');
 await modal(p).locator('[type=submit]').click();const j=await(await result).json();assert.equal(j.ok,true,JSON.stringify(j));await p.locator('#inboundWorkingCard').waitFor({state:'visible'});await p.waitForFunction(()=>_inboundPlanLoad.status==='ready');assert.equal(await p.locator('#ck-inbound-owner-name').getAttribute('readonly'),'');assert.equal(await p.locator('#ck-inbound-owner-name').inputValue(),DB.raw.prepare('SELECT customer FROM v2_inbound_plans WHERE id=?').get(j.plan_id).customer);return j;
}
async function finish(p){const result=p.waitForResponse(r=>r.url().endsWith('/api')&&r.request().postDataJSON()?.action==='v2_inbound_job_finish');await p.locator('[onclick="finishInbound(this)"]').click();const r=await(await result).json();assert.equal(r.ok,true,JSON.stringify(r));await p.locator('#page-home.active').waitFor();await p.waitForFunction(()=>!_actionLocks.finishInbound&&!_activeJobId);}
try{
 const receiveBatch=(await call('sop_courier_receive',{operation:'start',workers:[{id:outsider.badgeId,name:outsider.name}],lead_id:outsider.badgeId})).batch;
 for(const width of [1440,390,320]){
  const context=await browser.newContext({viewport:{width,height:1000}}),p=await field(context),code='SYNTH-PARTIAL-BR-'+width,codes=[String(999551000000+width),String(999552000000+width)];
  const plan=await call('v2_inbound_plan_create',{customer:'合成分批到货 '+width,biz_classes:['direct_ship'],external_inbound_no:code,lines:[{unit_type:'courier',tracking_nos:codes}]});
  const receive=tracking_no=>call('sop_courier_receive',{tracking_no,owner:'8-1',batch_id:receiveBatch.id,scanner_badge:outsider.badgeId});
  await receive(codes[0]);
  await p.evaluate(()=>{goPage('inbound_menu');goPage('inbound',{job_type:'inbound_direct_tally',biz_class:'direct_ship'});});
  await p.evaluate(()=>loadInboundCandidates());assert.ok((await p.locator('#inboundPlanSelect').innerText()).includes(code));
  const first=await start(p,code,'inbound_direct_tally',[a]);
  assert.equal(await p.locator('.ib-putaway-input').count(),1);assert.equal(await p.locator('.ib-putaway-input').getAttribute('max'),'1');assert.equal(await p.locator('.ib-putaway-input').inputValue(),'1');
  assert.match(await p.locator('#inboundPlanInfo .ck-arrival-summary').innerText(),/1 \/ 2/);
  await p.screenshot({path:path.join(artifacts,'first-'+width+'.png'),fullPage:true});
  await p.reload();await p.locator('#page-home.active').waitFor();await p.evaluate(j=>CKOpenNativeJob(j),{id:first.job_id,job_type:'inbound_direct_tally'});await p.waitForFunction(()=>_inboundPlanLoad?.status==='ready');
  assert.equal(await p.locator('.ib-putaway-input').getAttribute('max'),'1');
  await p.locator('.ib-putaway-input').fill('2');let bad=p.waitForResponse(r=>r.url().endsWith('/api')&&r.request().postDataJSON()?.action==='v2_inbound_job_finish');await p.locator('[onclick="finishInbound(this)"]').click();assert.equal((await(await bad).json()).ok,false);await p.waitForFunction(()=>!_actionLocks.finishInbound);
  assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(first.job_id).n,0);
  await p.locator('.ib-putaway-input').fill('1');await finish(p);
  let d=await call('v2_inbound_plan_detail',{id:plan.id});assert.notEqual(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,1);assert.equal(d.plan.inbound_progress.completed,0);
  await p.evaluate(()=>{goPage('inbound_menu');goPage('inbound',{job_type:'inbound_direct_tally',biz_class:'direct_ship'});});await p.locator('#inboundCodeInput').fill(code);await p.evaluate(()=>resolveInboundCode());assert.equal(await p.evaluate(()=>_ibResolvedKind),'');assert.match(await p.locator('#ibResolveResult').innerText(),/等待后续到货/);
  await receive(codes[1]);const second=await start(p,code,'inbound_direct_tally',[a]);assert.notEqual(first.job_id,second.job_id);
  assert.equal(await p.locator('.ib-putaway-input').inputValue(),'1');assert.equal(await p.locator('.ib-putaway-input').getAttribute('max'),'1');assert.match(await p.locator('#inboundPlanInfo .ck-arrival-summary').innerText(),/2 \/ 2/);
  assert.ok(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));await p.screenshot({path:path.join(artifacts,'second-'+width+'.png'),fullPage:true});await finish(p);
  const last=await start(p,code,'inbound_direct_putaway',[b]);assert.equal(await p.locator('#ck-putaway-workload').isVisible(),true);await p.locator('#ck-putaway-locations').fill('1');await p.locator('[name=ck-putaway-forklift][value=no]').check();await finish(p);
  d=await call('v2_inbound_plan_detail',{id:plan.id});assert.equal(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,2);assert.equal(d.plan.inbound_progress.completed,1);assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id IN (?,?,?) AND left_at=''").get(first.job_id,second.job_id,last.job_id).n,0);
  record(width+'px: candidate, scan, first batch, reload, over-limit rollback, wait arrival, second bounded batch and final putaway');await context.close();
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'results.json'),JSON.stringify({scenarios,errors},null,2));
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
