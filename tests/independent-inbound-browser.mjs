// Actual /001, strict Worker authentication, Chromium native zoom, synthetic memory DB.
import assert from 'node:assert/strict';import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {tmpdir} from 'node:os';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};
const artifacts=path.resolve(process.env.CK_INBOUND_ARTIFACTS||path.join(tmpdir(),'ck-independent-inbound'));fs.mkdirSync(artifacts,{recursive:true});let cookie='';
async function call(action,data={}){const r=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];const j=rememberOperation(await r.json(),cookie);assert.equal(j.ok,true,action+': '+JSON.stringify(j));return j;}
await call('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await confirmOperation(call,'合成理货上架派审员');
const people=[];for(const [employeeNo,name] of [['STAGE-BR-D','合成派审员 김하늘'],['STAGE-BR-A','合成长姓名跨境理货员 김가나다라마바사'],['STAGE-BR-B','合成上架乙 이지은'],['STAGE-OUTSIDE','合成无关职员']]){const p=(await call('sop_attendance_employee_register',{employeeNo,name,department:'direct_ship'})).person;p.record=(await call('sop_attendance_checkin',{badge:p.badgeId})).record;people.push(p);}
const [dispatcher,a,b,outsider]=people;await call('v2_inbound_plan_create',{customer:'合成货主 김가나 搜索样例',biz_classes:['direct_ship']});
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
 assert.equal(await p.evaluate(()=>_ibResolvedKind),'independent');
 if(type==='inbound_direct_tally'){await p.locator('#inboundExternalCustomer').fill('   ');const before=requests.filter(x=>x.action==='sop_native_start').length;await p.locator('[onclick="startInbound(this)"]').click();assert.equal(requests.filter(x=>x.action==='sop_native_start').length,before);await p.locator('#inboundExternalCustomer').fill('合成货主 김가나 '+code);}
 await p.locator('[onclick="startInbound(this)"]').click();await modal(p).waitFor();
 for(const person of staff)await add(p,person);
 const result=p.waitForResponse(r=>r.url().endsWith('/api')&&r.request().postDataJSON()?.action==='sop_native_start');
 await modal(p).locator('[type=submit]').click();const j=await(await result).json();assert.equal(j.ok,true,JSON.stringify(j));await p.locator('#inboundWorkingCard').waitFor({state:'visible'});await p.waitForFunction(()=>_inboundPlanLoad.status==='ready');return j;
}
async function finish(p){const result=p.waitForResponse(r=>r.url().endsWith('/api')&&r.request().postDataJSON()?.action==='v2_inbound_job_finish');await p.locator('[onclick="finishInbound(this)"]').click();const r=await(await result).json();assert.equal(r.ok,true,JSON.stringify(r));await p.locator('#page-home.active').waitFor();await p.waitForFunction(()=>!_actionLocks.finishInbound&&!_activeJobId);}
const specs=[{width:1440,zoom:1},{width:1280,zoom:1},{width:1440,zoom:1.25},{width:1280,zoom:1.25},{width:1440,zoom:1.5},{width:1280,zoom:1.5},{width:390,zoom:1},{width:320,zoom:1}];
try{
 for(const spec of specs){
  const {width,zoom}=spec,tag=width+'-'+zoom,profile=fs.mkdtempSync('/tmp/ck-owner-browser-'),context=await chromium.launchPersistentContext(profile,{executablePath:'/usr/bin/chromium',headless:true,viewport:{width,height:900},args:['--no-sandbox']});
  const settings=await context.newPage();await settings.goto('chrome://settings/appearance');await settings.evaluate(z=>chrome.settingsPrivate.setDefaultZoom(z),zoom);await settings.close();
  const p=await field(context),code='999880002'+specs.indexOf(spec)+width;
  const capture=async name=>{const metrics=await p.evaluate(()=>({width:innerWidth,dpr:devicePixelRatio,scroll:document.documentElement.scrollWidth}));assert.ok(Math.abs(metrics.width-width/zoom)<2,JSON.stringify(metrics));assert.ok(metrics.scroll<=metrics.width+1,JSON.stringify(metrics));const cdp=await context.newCDPSession(p),layout=await cdp.send('Page.getLayoutMetrics'),shot=await cdp.send('Page.captureScreenshot',{format:'png',fromSurface:true,captureBeyondViewport:true,clip:{...layout.contentSize,scale:1}});assert.equal(Buffer.from(shot.data,'base64').readUInt32BE(16),width);fs.writeFileSync(path.join(artifacts,name+'-'+tag+'.png'),Buffer.from(shot.data,'base64'));await cdp.detach();};
  const enter=async(type,value)=>{await p.evaluate(type=>{CKClearNativeJob();goPage('inbound',{job_type:type,biz_class:'direct_ship'});},type);await p.locator('#inboundEntryCard').waitFor({state:'visible'});await p.locator('#inboundCodeInput').fill(value);await p.evaluate(()=>resolveInboundCode());};
  await enter('inbound_direct_putaway',code);assert.equal(await p.evaluate(()=>_ibResolvedKind),'');assert.match(await p.locator('#ibResolveResult').innerText(),/理货尚未完成/);
  const jobCount=()=>DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n;
  const before=jobCount();await p.locator('[onclick="startInbound(this)"]').click();assert.equal(jobCount(),before);
  await enter('inbound_direct_tally',code+'9');
  await p.evaluate(()=>{const original=api;let release;const gate=new Promise(r=>release=r);window.__releaseRecognition=release;window.__restoreRecognition=()=>window.api=original;window.api=async body=>{const reply=await original(body);if(body.action==='v2_inbound_resolve_code'){window.__recognitionWaiting=true;await gate;}return reply;};window.__pendingRecognition=resolveInboundCode();});
  await p.waitForFunction(()=>window.__recognitionWaiting);await p.locator('#inboundCodeInput').fill(code);await p.evaluate(async()=>{__releaseRecognition();await __pendingRecognition;__restoreRecognition();});assert.equal(await p.evaluate(()=>_ibResolvedKind),'');
  const tally=await start(p,code,'inbound_direct_tally',[a]);
  assert.equal(await p.locator('#ibExternalFields').isVisible(),false);assert.match(await p.locator('#inboundPlanInfo').innerText(),/独立入库单号/);
  for(const id of ['inboundExtraSort','inboundExtraLabel','inboundExtraRepair','inboundExtraOther','inboundResultNote','inboundRemark'])assert.equal(await p.locator('#'+id).isVisible(),true,id);
  assert.equal(await p.locator('#ck-putaway-workload').isVisible(),false);
  const owner=p.locator('#ck-inbound-owner-name');assert.equal(await owner.isVisible(),true);assert.equal(await owner.inputValue(),'合成货主 김가나 '+code);
  // Emulate an already-running pre-upgrade task: erase only synthetic fixture data,
  // restore the guard, then reopen the task through the real dispatcher list.
  DB.raw.exec('DROP TRIGGER ck_independent_inbound_identity');DB.raw.prepare("UPDATE v2_inbound_plans SET customer='' WHERE id=?").run(tally.plan_id);
  const guardSource=fs.readFileSync('worker-v2/index.js','utf8'),guard=guardSource.match(/`CREATE TRIGGER IF NOT EXISTS ck_independent_inbound_identity[^`]+`/)[0].slice(1,-1);DB.raw.exec(guard);
  await p.reload();await p.locator('#page-home.active').waitFor();await p.evaluate(async id=>{const r=await CKSession.request('sop_dispatch_list');await CKOpenNativeJob(r.items.find(x=>x.id===id));},tally.job_id);await p.waitForFunction(()=>_inboundPlanLoad.status==='ready');assert.equal(await owner.inputValue(),'');
  for(const value of ['', '   ','　']){await owner.fill(value);const before=requests.filter(x=>x.action==='v2_inbound_job_finish').length;await p.locator('[onclick="finishInbound(this)"]').click();await p.waitForFunction(()=>!_actionLocks.finishInbound);assert.equal(requests.filter(x=>x.action==='v2_inbound_job_finish').length,before);assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(tally.job_id).status,'working');}
  await owner.fill('合成货主 김가나');await owner.focus();await p.waitForFunction(()=>document.querySelectorAll('#ck-owner-options-1 option').length>0);assert.ok(await p.locator('#ck-owner-options-1 option').count()>0);
  await owner.fill('  合成补填货主 김가나 '+width+'  ');
  await owner.scrollIntoViewIfNeeded();const bounds=await owner.boundingBox();assert.ok(bounds.x>=0&&bounds.x+bounds.width<=width+1);assert.ok(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  // Server checks must reject a crafted putaway during tally, regardless of the UI.
  const malicious=await p.evaluate(async({code,badge,name})=>CKSession.request('sop_native_start',{payload:{action:'v2_inbound_job_start',job_type:'inbound_direct_putaway',biz_class:'direct_ship',external_inbound_no:code,client_req_id:crypto.randomUUID()},workers:[{id:badge,name}],lead_id:badge,estimated_minutes:10}).then(()=>({ok:true})).catch(e=>({ok:false,error:e.message})),{code,badge:b.badgeId,name:b.name});assert.equal(malicious.ok,false);
  await p.locator('#inboundExtraLabel').fill('7');await p.locator('#inboundExtraRepair').fill('1');await p.locator('#inboundExtraOther').fill('合成操作说明');await p.locator('#inboundResultNote').fill('合成理货说明');await p.locator('#inboundRemark').fill('合成备注');
  await capture('independent-tally');await finish(p);
  let d=await call('v2_inbound_plan_detail',{id:tally.plan_id});assert.equal(d.plan.status,'tallied_pending_putaway');assert.equal(d.plan.display_no,'');assert.equal(d.plan.customer,'合成补填货主 김가나 '+width);assert.equal(d.plan.unload_completed_at,'');
  const saved=JSON.parse(DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(tally.job_id).result_json);assert.equal(saved.extra_ops.label_qty,7);assert.equal(saved.result_note,'合成理货说明');
  await enter('inbound_direct_tally',code);assert.equal(await p.evaluate(()=>_ibResolvedKind),'');assert.match(await p.locator('#ibResolveResult').innerText(),/理货已完成/);
  await enter('inbound_direct_putaway',code);await p.waitForFunction(code=>[...document.querySelectorAll('#inboundPlanSelect option')].some(x=>x.dataset.code===code),code);
  const option=p.locator('#inboundPlanSelect option').filter({hasText:code});assert.match(await option.innerText(),/独立单号/);assert.match(await option.innerText(),/合成补填货主/);await p.locator('#inboundPlanSelect').selectOption(await option.getAttribute('value'));await p.evaluate(()=>onInboundCandidateSelect());await p.waitForFunction(()=>_ibResolvedKind==='independent');assert.equal(await p.evaluate(()=>_ibResolvedPlanId),tally.plan_id);
  // A new page must recover the stored association using only the entered number.
  await p.reload();await p.locator('#page-home.active').waitFor();
  const putaway=await start(p,code,'inbound_direct_putaway',[a,b]);assert.notEqual(tally.job_id,putaway.job_id);assert.equal(tally.plan_id,putaway.plan_id);
  assert.equal(await p.locator('#ck-putaway-workload').isVisible(),true);assert.equal(await p.locator('#ck-inbound-owner-name').getAttribute('readonly'),'');assert.equal(await p.locator('#ck-inbound-owner-name').inputValue(),'合成补填货主 김가나 '+width);assert.match(await p.locator('#ck-putaway-members').innerText(),/合成补填货主/);assert.equal(await p.locator('#inboundExtraSort').isVisible(),false);
  const outsiderLogin=await worker.fetch(new Request(origin+'/001/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_login',badge:outsider.badgeId})}),env),outsiderCookie=outsiderLogin.headers.get('set-cookie');assert.ok(outsiderCookie);
  const unauthorized=await worker.fetch(new Request(origin+'/001/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:outsiderCookie.split(';')[0]},body:JSON.stringify({action:'v2_inbound_job_finish',job_id:putaway.job_id,worker_id:a.badgeId,complete_job:true,location_count:99,used_forklift:true})}),env);assert.equal((await unauthorized.json()).ok,false);
  await p.locator('[onclick="finishInbound(this)"]').click();assert.equal(await p.locator('#inboundWorkingCard').isVisible(),true);
  await p.locator('#ck-putaway-locations').fill('4');await p.locator('[name=ck-putaway-forklift][value=no]').check();
  assert.ok(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));await capture('independent-putaway');await finish(p);
  d=await call('v2_inbound_plan_detail',{id:tally.plan_id});assert.equal(d.plan.status,'completed');assert.deepEqual(d.jobs.find(j=>j.id===putaway.job_id).putaway_workload,{location_count:4,used_forklift:false});
  assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id IN (?,?)').get(tally.job_id,putaway.job_id).n,2);
  await enter('inbound_direct_putaway',code);assert.equal(await p.evaluate(()=>_ibResolvedKind),'');assert.match(await p.locator('#ibResolveResult').innerText(),/已完成入库/);
  const dashboard=await call('v2_dashboard_order_export',{});assert.equal(dashboard.rows.filter(r=>[tally.job_id,putaway.job_id].includes(r.job_id)).length,2);
  await context.close();record('independent numeric reference, unchanged fields, durable association, strict permission and putaway at '+width+'px at '+zoom+' native zoom');
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'result.json'),JSON.stringify({ok:true,scenarios,errors},null,2));console.log(JSON.stringify({ok:true,scenarios:scenarios.length,artifacts}));
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
