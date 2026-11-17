// Actual /001, strict Worker authentication, Chromium native zoom, synthetic memory DB.
import assert from 'node:assert/strict';import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {tmpdir} from 'node:os';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};
const artifacts=path.resolve(process.env.CK_INBOUND_ARTIFACTS||path.join(tmpdir(),'ck-inbound-stages'));fs.mkdirSync(artifacts,{recursive:true});let cookie='';
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
 for(const width of [1440,1280,390,320]){
  const context=await browser.newContext({viewport:{width,height:900}}),p=await field(context),code='SYNTH-STAGE-'+width;
  const plan=await call('v2_inbound_plan_create',{customer:'合成阶段验收 '+width,biz_classes:['direct_ship'],external_inbound_no:code,lines:[{unit_type:'carton',planned_qty:8}]});
  const u=await call('v2_unload_job_start',{plan_id:plan.id,worker_id:dispatcher.badgeId});await call('v2_unload_job_finish',{job_id:u.job_id,worker_id:dispatcher.badgeId,complete_job:true,result_lines:[{unit_type:'carton',actual_qty:8}]});
  await p.evaluate(()=>goPage('inbound_menu'));assert.equal(await p.locator('#page-inbound_menu [onclick*=inbound_direct_tally]').count(),1);assert.equal(await p.locator('#page-inbound_menu [onclick*=inbound_direct_putaway]').count(),1);assert.match(await p.locator('#page-inbound_menu').innerText(),/大货理货上架/);
  await p.screenshot({path:path.join(artifacts,'menu-'+width+'.png'),fullPage:true});
  const tally=await start(p,[1280,320].includes(width)?plan.display_no:code,'inbound_direct_tally',[a]);
  for(const id of ['inboundExtraSort','inboundExtraLabel','inboundExtraRepair','inboundExtraOther','inboundResultNote','inboundRemark'])assert.equal(await p.locator('#'+id).isVisible(),true,id);
  assert.equal(await p.locator('#ck-putaway-workload').isVisible(),false);await p.locator('.ib-putaway-input').fill('8');await p.locator('#inboundExtraLabel').fill('2');await p.locator('#inboundResultNote').fill('合成理货说明');
  await p.screenshot({path:path.join(artifacts,'tally-'+width+'.png'),fullPage:true});await finish(p);let d=await call('v2_inbound_plan_detail',{id:plan.id});assert.equal(d.plan.status,'tallied_pending_putaway');assert.equal(d.lines[0].putaway_qty,8);
  const putaway=await start(p,code,'inbound_direct_putaway',[a,b]);assert.notEqual(tally.job_id,putaway.job_id);assert.equal(await p.locator('#ck-putaway-workload').isVisible(),true);assert.equal(await p.locator('#inboundExtraSort').isVisible(),false);assert.equal(await p.locator('.ib-putaway-input').isVisible(),false);
  // An unrelated employee session cannot finish another crew's stage.
  const outsiderLogin=await worker.fetch(new Request(origin+'/001/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_login',badge:outsider.badgeId})}),env);
  const outsiderCookie=outsiderLogin.headers.get('set-cookie');assert.ok(outsiderCookie);
  const unauthorized=await worker.fetch(new Request(origin+'/001/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:outsiderCookie.split(';')[0]},body:JSON.stringify({action:'v2_inbound_job_finish',job_id:putaway.job_id,worker_id:a.badgeId,complete_job:true,location_count:99,used_forklift:true})}),env);assert.equal((await unauthorized.json()).ok,false);
  assert.equal(await p.locator('[name=ck-putaway-forklift]:checked').count(),0);assert.equal(await p.locator('#ck-putaway-locations').inputValue(),'');
  await p.locator('[onclick="finishInbound(this)"]').click();await p.waitForTimeout(150);assert.equal(await p.locator('#inboundWorkingCard').isVisible(),true);
  await p.locator('#ck-putaway-locations').fill('4');await p.locator('[name=ck-putaway-forklift][value=no]').check();
  const sizes=await p.evaluate(()=>({scroll:document.documentElement.scrollWidth,width:innerWidth,card:document.getElementById('ck-putaway-workload').getBoundingClientRect().toJSON()}));assert.ok(sizes.scroll<=sizes.width+1,JSON.stringify(sizes));
  await p.screenshot({path:path.join(artifacts,'putaway-'+width+'.png'),fullPage:true});await finish(p);d=await call('v2_inbound_plan_detail',{id:plan.id});assert.equal(d.plan.status,'completed');assert.equal(d.lines[0].putaway_qty,8);assert.deepEqual(d.jobs.find(j=>j.id===putaway.job_id).putaway_workload,{location_count:4,used_forklift:false});
  const exported=await call('v2_inbound_plan_export',{}),row=exported.rows.find(r=>r.plan_id===plan.id);assert.equal(row.上架货位数,4);assert.match(row.上架叉车使用明细,/否/);
  const office=await browser.newContext({viewport:{width,height:900}});await office.addCookies([{name:cookie.split('=')[0],value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);const o=await office.newPage();o.on('pageerror',e=>errors.push(e.message));await o.goto(origin+'/002/');await o.locator('.ck-cross-nav').waitFor();await o.waitForFunction(()=>window.CKSession?.user);await o.evaluate(id=>{openInboundDetail(id);},plan.id);await o.locator('#inboundDetailBody').getByText('货位数：4',{exact:false}).waitFor({state:'attached'});await o.evaluate(()=>document.querySelectorAll('#inboundDetailBody details').forEach(el=>el.open=true));await o.locator('#inboundDetailBody').getByText('货位数：4',{exact:false}).waitFor();await o.screenshot({path:path.join(artifacts,'collaboration-'+width+'.png'),fullPage:true});
  const download=o.waitForEvent('download');await o.evaluate(()=>{document.getElementById('ibFilterStart').value='';document.getElementById('ibFilterEnd').value='';return exportInboundPlans();});const csv=fs.readFileSync(await(await download).path(),'utf8');assert.match(csv,/上架货位数/);assert.match(csv,/上架叉车使用明细/);assert.ok(csv.includes(code+'：否'));fs.writeFileSync(path.join(artifacts,'synthetic-export-'+width+'.csv'),csv);await office.close();await context.close();record('normal dispatch, unchanged tally, explicit putaway, shared export and collaboration at '+width+'px');
 }
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'result.json'),JSON.stringify({ok:true,scenarios,errors},null,2));console.log(JSON.stringify({ok:true,scenarios:scenarios.length,artifacts}));
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
