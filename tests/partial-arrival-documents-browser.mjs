// Actual /001, strict Worker authentication, Chromium native zoom, synthetic memory DB.
import assert from 'node:assert/strict';import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {tmpdir} from 'node:os';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};
const artifacts=path.resolve(process.env.CK_PARTIAL_DOCUMENT_ARTIFACTS||path.join(tmpdir(),'ck-partial-arrival-documents'));fs.mkdirSync(artifacts,{recursive:true});let cookie='';
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
 const XLSX=require('../shared/xlsx.full.min.js'),ctx=await browser.newContext({viewport:{width:1440,height:1000},acceptDownloads:true});
 await ctx.addCookies([{name:cookie.split('=')[0],value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
 const p=await ctx.newPage();p.setDefaultTimeout(15000);p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>d.accept());await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
 const code='SYNTH-PARTIAL-DOCUMENTS',codes=['999001550001','999001550002'];
 const plan=await call('v2_inbound_plan_create',{customer:'合成分批打印导出客户',biz_classes:['direct_ship'],external_inbound_no:code,lines:[{unit_type:'courier',tracking_nos:codes}]});
 const batch=(await call('sop_courier_receive',{operation:'start',workers:[{id:outsider.badgeId,name:outsider.name}],lead_id:outsider.badgeId})).batch;
 const receive=tracking_no=>call('sop_courier_receive',{tracking_no,owner:'8-1',batch_id:batch.id,scanner_badge:outsider.badgeId});
 const dispatch=type=>call('sop_native_start',{payload:{action:'v2_inbound_job_start',plan_id:plan.id,external_inbound_no:code,job_type:type,biz_class:'direct_ship',client_req_id:crypto.randomUUID()},workers:[{id:a.badgeId,name:a.name}],lead_id:a.badgeId,estimated_minutes:15});
 const complete=(j,extra)=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:a.badgeId,complete_job:true,...extra});
 const snapshot=()=>JSON.stringify(['v2_inbound_plans','v2_inbound_plan_lines','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','ck_inbound_arrival_batches'].map(t=>DB.raw.prepare('SELECT * FROM '+t+' ORDER BY rowid').all()));
 async function check(tag,received,tallied,completed){
  const before=snapshot();await p.goto(origin+'/002/?tab=inbound');await p.locator('#ibExportBtn').waitFor();
  const exported=p.waitForResponse(r=>r.request().postDataJSON()?.action==='v2_inbound_plan_export'),download=p.waitForEvent('download');await p.locator('#ibExportBtn').click();const response=await(await exported).json();assert.equal(response.ok,true,JSON.stringify(response));
  const file=await download,filename=path.join(artifacts,tag+'.csv');await file.saveAs(filename);const bytes=fs.readFileSync(filename);assert.equal(bytes.subarray(0,3).toString('hex'),'efbbbf');
  const wb=XLSX.read(bytes,{type:'buffer'}),rows=XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]],{defval:''}),row=rows.find(r=>r.plan_id===plan.id);assert.ok(row);assert.equal(rows.filter(r=>r.plan_id===plan.id).length,1);assert.equal(row.计划箱数,2);assert.equal(row.实际箱数,received);
  if(completed)assert.equal(row.状态,'已入库');else{assert.notEqual(row.状态,'已入库');assert.match(row.未完成入库类型,/代发/);}
  if(received===1)assert.equal(row.卸货完成时间,'');
  await p.evaluate(id=>openInboundDetail(id),plan.id);const button=p.locator('#inboundDetailBody button[onclick^="printIbQr"]');await button.waitFor();
  await p.evaluate(()=>{const open=window.open;window.open=(...args)=>{const w=open(...args);if(w)w.print=()=>{w.syntheticPrinted=true;};return w;};});
  const printed=p.waitForEvent('popup'),issued=p.waitForResponse(r=>r.request().postDataJSON()?.action==='v2_inbound_plan_confirm_issue');await button.click();const popup=await printed;assert.equal((await(await issued).json()).ok,true);await popup.waitForFunction(()=>window.syntheticPrinted&&document.querySelector('.inbound-cargo'));
  assert.equal(await popup.locator('.inbound-cargo tbody tr').count(),1);assert.equal((await popup.locator('.inbound-cargo tbody tr td').nth(1).innerText()).trim(),'2','print retains clearly labelled planned quantity');
  for(const tracking of codes)assert.ok((await popup.locator('body').innerText()).includes(tracking));if(!completed)assert.ok(!(await popup.locator('body').innerText()).includes('已入库'));
  await popup.pdf({path:path.join(artifacts,tag+'.pdf'),format:'A4',printBackground:true});await popup.close();await p.waitForFunction(()=>!_inboundPrintPending);
  assert.equal(snapshot(),before,'CSV and print issue preserve logistics, output and clocks');
  const detail=await call('v2_inbound_plan_detail',{id:plan.id});assert.equal(detail.lines[0].actual_qty,received);assert.equal(detail.lines[0].putaway_qty,tallied);assert.equal(detail.plan.issue_state.state,'issued');
  record(tag+': real CSV numeric quantities '+received+'/2; A4 plan quantity 2; no output or labor mutation');
 }
 await receive(codes[0]);await check('one-received',1,0,false);
 const first=await dispatch('inbound_direct_tally');await complete(first,{result_lines:[{unit_type:'courier',putaway_qty:1}]});await check('first-tallied',1,1,false);
 await receive(codes[1]);const second=await dispatch('inbound_direct_tally');await complete(second,{result_lines:[{unit_type:'courier',putaway_qty:1}]});await check('both-tallied',2,2,false);
 const last=await dispatch('inbound_direct_putaway');await complete(last,{location_count:1,used_forklift:false});await check('finally-putaway',2,2,true);
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'results.json'),JSON.stringify({scenarios,errors},null,2));await ctx.close();
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
