// Actual built application smoke coverage. Every request is localhost and every
// business record is created in an in-memory fixture, before the read-only UI run.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import {createRequire} from 'node:module';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';

const scriptRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const sourceRoot=path.resolve(process.env.CK_SMOKE_ROOT||scriptRoot);
const sourceHashes=Object.fromEntries(['shared/sop-entry.js','shared/sop-native.js','001/app.js','002/app.js'].map(file=>[file,createHash('sha256').update(fs.readFileSync(path.join(sourceRoot,file))).digest('hex')]));
const assetsRoot=path.join(sourceRoot,'worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(assetsRoot,'002/index.html')),'Build CK_SMOKE_ROOT/worker-v2/build-sop-staging.mjs first');
const source=relative=>import(pathToFileURL(path.join(sourceRoot,relative)).href);
const [{default:worker},{default:entry},{database},{digest},{operationHeaders,rememberOperation,confirmOperation}]=await Promise.all([
 source('worker-v2/index.js'),source('worker-v2/staging-entry.js'),source('tests/d1-adapter.mjs'),source('worker-v2/access-control.js'),source('tests/office-operator-fixture.mjs')
]);
const require=createRequire(import.meta.url);
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const out=path.resolve(process.env.CK_SMOKE_OUT||'/tmp/ck-ui-entry-smoke-browser');
fs.mkdirSync(out,{recursive:true});
const DB=database(),objects=new Map();
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ACCEPT_NEW:'true',SOP_PUBLIC_TEST_ACCESS:'false',SOP_ADMIN_CODE_SHA256:await digest('synthetic-entry-smoke-only'),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);}}};
let cookie='';
async function seed(action,data={}){
 const r=await worker.fetch(new Request('http://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];
 const result=rememberOperation(await r.json(),cookie);assert.equal(result.ok,true,action+': '+result.error);return result;
}
await seed('sop_login',{sop_key:'synthetic-entry-smoke-only'});
await confirmOperation(seed,'合成入口验收管理员');
const staff=(await seed('sop_attendance_employee_register',{employeeNo:'ENTRY-SMOKE',name:'合成入口派审员',department:'bulk'})).person;
await seed('sop_attendance_checkin',{badge:staff.badgeId});
const permanent=(await seed('sop_attendance_register',{name:'合成长期入口',agency:'가온'})).person;
await seed('sop_attendance_checkin',{name:'合成日当入口',agency:'가온'});
const inbound=await seed('v2_inbound_plan_create',{customer:'合成入口入库',biz_classes:['bulk'],expected_arrival:'2026-10-13',cargo_summary:'入口核查合成货物'});
const need=await seed('sop_need_create',{source_type:'inbound',source_id:inbound.id,department:'bulk',customer:'合成入口入库',title:'合成入口可继续作业',instructions:'只核查打开和返回，不提交作业',planned_quantity:10,planned_unit:'箱'});
const task=await seed('sop_task_dispatch',{department:'bulk',title:'合成入口可继续作业',need_id:need.id,job_type:'bulk_op',workers:[{id:staff.badgeId,name:staff.name}],lead_id:staff.badgeId,estimated_minutes:30});
const issue=await seed('v2_issue_create',{biz_class:'bulk',customer:'合成入口问题点',issue_description:'入口核查合成问题，不提交业务'});
await seed('sop_issue_adopt',{legacy_id:issue.id,native:true});
const check=await seed('sop_check_create',{ship_date:'2026-10-13'});
await seed('sop_check_items',{id:check.id,revision:1,items:[{barcode:'000ENTRY01',customer:'合成入口核对',qty:4},{barcode:'000ENTRY02',customer:'合成入口核对',qty:2}]});
const outboundId='SYNTHETIC-ENTRY-OUTBOUND';
DB.raw.prepare("INSERT INTO v2_outbound_orders(id,display_no,status,expected_ship_at,customer,biz_class) VALUES(?,?,'issued',?,'合成入口出库','bulk')").run(outboundId,'合成入口出库单','2026-10-14');
const businessTables=['sop_records','sop_events','v2_inbound_plans','v2_outbound_orders','v2_issue_tickets','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','ck_attendance_people','ck_attendance_days','ck_attendance_breaks'];
const snapshot=()=>JSON.stringify(Object.fromEntries(businessTables.map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()])));
const before=snapshot();
env.ASSETS={async fetch(request){
 const u=new URL(request.url);let f=path.resolve(assetsRoot,'.'+decodeURIComponent(u.pathname));
 if(f!==assetsRoot&&!f.startsWith(assetsRoot+path.sep))return new Response('denied',{status:403});
 try{if(fs.statSync(f).isDirectory())f=path.join(f,'index.html');return new Response(fs.readFileSync(f),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(f)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}
}};
const requests=[],errors=[],coverage=[],checks=[],limitations=[
 '浏览器为桌面 Chromium 的 1440px 与 390px 视口；未实测实体 iPhone、Safari、相机或扫码枪硬件。',
 '本脚本检查入口与导航生命周期，不替代派工、保存、删除、打印、签到等写操作专项测试。',
 '合成数据覆盖当前入库、出库、作业、SOP 核对和问题点；旧历史核对兼容由迁移专项测试覆盖。',
 '物料覆盖后台列表与视图导航；不提交领用、库存、采购、收货或附件。'
];
const server=http.createServer(async(req,res)=>{
 try{const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks);let action='';try{action=JSON.parse(body.toString()).action||'';}catch{}
  const r=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(body.length?{body}:{})}),env);
  if(action)requests.push({action,status:r.status});const headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();
  res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));
 }catch(error){errors.push({source:'server',message:error.message});res.writeHead(500);res.end(error.message);}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const body=r=>{try{return r.request().postDataJSON();}catch{return{};}};
async function capture(page,name,selector){
 const root=page.locator(selector);await root.waitFor({state:'visible'});
 await page.waitForFunction(selector=>document.querySelector(selector)?.innerText.trim(),selector);
 const facts=await root.evaluate(node=>({text:node.innerText.slice(0,900),height:Math.round(node.getBoundingClientRect().height),visible:true}));
 const display=await page.evaluate(()=>({width:innerWidth,pageWidth:document.documentElement.scrollWidth,activePages:[...document.querySelectorAll('.page.active')].map(n=>n.id),visibleViews:[...document.querySelectorAll('.view')].filter(n=>n.checkVisibility()).map(n=>n.id)}));
 coverage.push({name,width:display.width,selector,...facts,...display});
 assert.ok(facts.text.trim(),name+' has empty visible content');
 await page.screenshot({path:path.join(out,name+'-'+display.width+'.png'),fullPage:false});
}
async function pageFor(ctx){const p=await ctx.newPage();p.setDefaultTimeout(12000);p.on('pageerror',e=>errors.push({source:'browser',message:e.message,url:p.url()}));p.on('dialog',d=>d.dismiss());await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());return p;}
async function readComplete(page,action,run){const wait=page.waitForResponse(r=>body(r).action===action);await run();const result=await(await wait).json();assert.equal(result.ok,true,action+': '+result.error);}
async function arrived(promise,name){let timer;try{await Promise.race([promise,new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('Delayed request not observed: '+name)),12000))]);}finally{clearTimeout(timer);}}
async function officeTab(page,tab,selector){await page.locator('#mainTabs [data-tab='+tab+']').click();await page.locator(selector).waitFor({state:'visible'});}
async function hold(page,action,id){
 let entered,release;const seen=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
 const handler=async route=>{const b=body({request:()=>route.request()});if(b.action===action&&(!id||b.id===id||b.legacy_id===id||b.source_id===id)){entered();await gate;}await route.fallback();};
 await page.route('**/api',handler);return{seen,release,remove:()=>page.unroute('**/api',handler)};
}
async function navigationRace(page,kind){
 const action=kind==='check'?'sop_check_for_batch':'v2_issue_detail',id=kind==='check'?check.id:issue.id;
 await officeTab(page,kind,'#view-'+kind);const h=await hold(page,action,id);
 try{
  const row=page.locator('#'+kind+'ListBody .list-item').filter({hasText:kind==='check'?'2026-10-13':'合成入口问题点'}).first();await row.click();await arrived(h.seen,action);
  await readComplete(page,'v2_inbound_plan_list',()=>officeTab(page,'inbound','#view-inbound'));await page.locator('#inboundListBody .list-item').filter({hasText:'合成入口入库'}).waitFor();const marker=await page.locator('#view-inbound').innerText();
  const response=page.waitForResponse(r=>body(r).action===action&&(body(r).id===id||body(r).legacy_id===id));h.release();await response;await h.remove();
  await page.waitForFunction(()=>document.querySelector('#view-inbound')?.checkVisibility());
  assert.equal(await page.locator('#view-'+kind+'_detail').isVisible(),false,kind+' stale detail must remain hidden');
  assert.equal(await page.locator('#view-inbound').innerText(),marker,kind+' stale response changed current inbound page');
  checks.push({name:kind+' delayed lookup → switch to inbound → release',width:await page.evaluate(()=>innerWidth),ok:true});
 }finally{h.release();await h.remove();}
}
async function workflowRace(page,kind){
 const action=kind==='check'?'sop_check_for_batch':'sop_issue_adopt',id=kind==='check'?check.id:issue.id;
 await officeTab(page,kind,'#view-'+kind);const h=await hold(page,action,id);
 try{
  await page.locator('#'+kind+'ListBody .list-item').filter({hasText:kind==='check'?'2026-10-13':'合成入口问题点'}).first().click();await arrived(h.seen,action);
  await readComplete(page,'sop_need_groups',()=>officeTab(page,'need','#view-need'));await page.locator('#view-need [data-plan-filters]').waitFor();
  const before=await page.locator('#view-need [data-plan-filters]').count(),width=await page.evaluate(()=>innerWidth);
  await page.screenshot({path:path.join(out,'race-'+kind+'-before-'+width+'.png'),fullPage:false});
  const response=page.waitForResponse(r=>body(r).action===action&&body(r).legacy_id===id);h.release();await response;await h.remove();
  // Allow the released promise's DOM callbacks and any follow-up local read to run.
  await page.waitForTimeout(200);
  const after=await page.locator('#view-need [data-plan-filters]').count(),detailVisible=await page.locator('#view-'+kind+'_detail').isVisible();
  const result={name:kind+' delayed SOP lookup → open work-plan workflow → release',width,beforeFilters:before,afterFilters:after,detailVisible,ok:before===1&&after===1&&!detailVisible};checks.push(result);
  await page.screenshot({path:path.join(out,'race-'+kind+'-after-'+width+'.png'),fullPage:false});
  if(process.env.CK_SMOKE_RACE_ONLY!=='1')assert.equal(result.ok,true,kind+' late SOP detail destroyed current work-plan workflow: '+JSON.stringify(result));
 }finally{h.release();await h.remove();}
}
try{
 for(const width of [1440,390]){
  // Field/kiosk activation deliberately revokes a shared office browser session.
  // Each width starts with a fresh, explicitly confirmed synthetic office session.
  await seed('sop_login',{sop_key:'synthetic-entry-smoke-only'});await confirmOperation(seed,'合成入口验收管理员');
  const ctx=await browser.newContext({viewport:{width,height:width<400?844:1000}});
  await ctx.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
  const p=await pageFor(ctx);
  try{
   if(process.env.CK_SMOKE_RACE_ONLY==='1'){await p.goto(origin+'/002/');await p.locator('#dashGrid .dash-card').first().waitFor();for(const kind of ['check','issue'])await workflowRace(p,kind);continue;}
   // Cold boot and reload use a real office session and the actual staging wrapper.
   await p.goto(origin+'/002/');await p.locator('#dashGrid .dash-card').first().waitFor();await capture(p,'002-home-cold','#view-home');await p.reload();await p.locator('#dashGrid .dash-card').first().waitFor();await capture(p,'002-home-refresh','#view-home');
   await readComplete(p,'v2_inbound_plan_list',()=>officeTab(p,'inbound','#view-inbound'));await capture(p,'002-inbound-list','#view-inbound');
   await p.locator('#inboundListBody .list-item').filter({hasText:'合成入口入库'}).click();await p.waitForFunction(()=>document.querySelector('#inboundDetailBody')?.textContent.includes('合成入口入库'));await capture(p,'002-inbound-detail','#view-inbound_detail');
   await p.locator('#view-inbound_detail button[onclick*=inbound]').first().click();await capture(p,'002-inbound-return','#view-inbound');
   await readComplete(p,'v2_outbound_order_list',()=>officeTab(p,'outbound','#view-outbound'));await capture(p,'002-outbound-list','#view-outbound');
   await p.locator('#outboundListBody .list-item').filter({hasText:'合成入口出库'}).click();await p.waitForFunction(()=>document.querySelector('#outboundDetailBody')?.textContent.includes('合成入口出库'));await capture(p,'002-outbound-detail','#view-outbound_detail');
   await p.locator('#view-outbound_detail button[onclick*=outbound]').first().click();await capture(p,'002-outbound-return','#view-outbound');
   await readComplete(p,'sop_need_groups',()=>officeTab(p,'need','#view-need'));await p.locator('#view-need .ck-plan-card').waitFor();assert.equal(await p.locator('#view-need [data-plan-filters]').count(),1);await capture(p,'002-work-plans','#view-need');
   await p.locator('#view-need [data-plan-open] button').first().click();await p.locator('#groupTable tbody tr').first().waitFor();await capture(p,'002-work-detail','#view-need');await p.locator('#groupActions button').filter({hasText:'返回作业指令列表'}).click();await p.locator('#view-need [data-plan-filters]').waitFor();assert.equal(await p.locator('#view-need [data-plan-filters]').count(),1);
   await readComplete(p,'v2_verify_batch_list',()=>officeTab(p,'check','#view-check'));await capture(p,'002-check-list','#view-check');
   await p.locator('#checkListBody .list-item').filter({hasText:'2026-10-13'}).click();await p.locator('#checkDetailBody #actions').waitFor();assert.equal(await p.locator('#checkDetailBody #actions').count(),1);assert.equal(await p.locator('#checkDetailBody [onclick*=updateVerifyBatch]').count(),0);await capture(p,'002-check-detail','#view-check_detail');
   await p.locator('#view-check_detail button[onclick*=check]').first().click();await capture(p,'002-check-return','#view-check');await navigationRace(p,'check');await workflowRace(p,'check');
   await readComplete(p,'v2_issue_list',()=>officeTab(p,'issue','#view-issue'));await capture(p,'002-issue-list','#view-issue');
   await p.locator('#issueListBody .list-item').filter({hasText:'合成入口问题点'}).click();await p.locator('#issueDetailBody #actions').waitFor();assert.equal(await p.locator('#issueDetailBody #actions').count(),1);await capture(p,'002-issue-detail','#view-issue_detail');
   await p.locator('#view-issue_detail button[onclick*=issue]').first().click();await capture(p,'002-issue-return','#view-issue');await navigationRace(p,'issue');await workflowRace(p,'issue');
   await p.goto(origin+'/003/');await capture(p,'003-home-cold','#view-dashboard');await p.reload();await capture(p,'003-home-refresh','#view-dashboard');
   for(const view of ['materials','assets','purchasing','ledger','settings','dashboard']){await p.locator('#adminTabs [data-view='+view+']').click();await capture(p,'003-'+view,'#view-'+view);assert.equal(await p.locator('#appWrap .view.active').count(),1);}
   await p.goto(origin+'/shuju/');await p.waitForFunction(()=>document.querySelector('#statActiveWorkers')?.textContent!=='--');await capture(p,'shuju-realtime-cold','#tab-realtime');await p.reload();await capture(p,'shuju-realtime-refresh','#tab-realtime');
   for(const [label,selector]of [['日当人力 / 일용직','#ck-labor-dashboard'],['职员出勤 / 직원','.ck-employee-dashboard'],['派工质量与待办','#ck-dashboard']]){await p.locator('.tab-bar').getByRole('button',{name:label,exact:true}).click();await capture(p,'shuju-'+(selector==='#ck-labor-dashboard'?'daily':selector==='#ck-dashboard'?'work':'employees'),selector);assert.equal(await p.locator(selector).count(),1);}
   await p.locator('.tab-bar button[onclick*=realtime]').click();await capture(p,'shuju-realtime-return','#tab-realtime');await p.locator('.tab-bar button[onclick*=orders]').click();await capture(p,'shuju-orders','#tab-orders');await p.locator('.tab-bar button[onclick*=workhours]').click();await capture(p,'shuju-workhours','#tab-workhours');
   await p.goto(origin+'/attendance/');await p.locator('#ck-login-code').fill('synthetic-entry-smoke-only');await p.locator('#ck-auth-gate button[type=submit]').click();await p.locator('#ck-auth-gate').waitFor({state:'hidden'});await p.locator('#fixed').waitFor();await capture(p,'attendance-daily-cold','#kiosk .terminal:not([hidden])');await p.reload();await p.locator('#fixed').waitFor();assert.equal(await p.locator('.ck-attendance-kind').count(),1);await capture(p,'attendance-daily-refresh','#kiosk .terminal:not([hidden])');
   await p.locator('#fixed').click();await p.locator('#code').fill(permanent.badgeId);await p.locator('#terminal-content form button.primary').click();await p.locator('#terminal-content').getByRole('button',{name:'确认签到 / 출근 등록 확인',exact:true}).waitFor();await capture(p,'attendance-permanent-review','#terminal-content');await p.locator('#terminal-content [data-back]').click();await p.locator('#fixed').waitFor();
   await p.locator('#tab-out').click();await capture(p,'attendance-daily-out','#terminal-content');await p.locator('[data-kind=employee]').click();await capture(p,'attendance-employee-in','.ck-employee-terminal');await p.locator('.ck-employee-terminal [data-out]').click();await capture(p,'attendance-employee-out','.ck-employee-terminal');await p.locator('[data-kind=daily]').click();await capture(p,'attendance-daily-return','#kiosk .terminal:not([hidden])');
   // /001 must have its own field identity; an office cookie is not substituted.
   const fieldCtx=await browser.newContext({viewport:{width,height:width<400?844:1000}}),field=await pageFor(fieldCtx);
   try{
    await field.goto(origin+'/001/');await field.locator('#ck-login-code').fill(staff.badgeId);await field.locator('#ck-auth-gate button[type=submit]').click();await field.locator('#page-home.active').waitFor();await field.locator('[data-dispatch-job="'+task.id+'"]').waitFor();await capture(field,'001-home-cold','#page-home');await field.reload();await field.locator('[data-dispatch-job="'+task.id+'"]').waitFor();await capture(field,'001-home-refresh','#page-home');
    assert.equal(await field.locator('[data-dispatch-tasks]').count(),1);await field.locator('[data-dispatch-job="'+task.id+'"]').click();await field.locator('#page-bulk_op.active .ck-work-sheet').first().waitFor();await capture(field,'001-continue-original-task','#page-bulk_op');await field.locator('#page-bulk_op [data-home]').click();await capture(field,'001-task-return','#page-home');
    await field.locator('#page-home [onclick="goPage(\'issue_list\')"]').click();await field.locator('#issueListBody .list-item').filter({hasText:'合成入口问题点'}).waitFor();await capture(field,'001-issue-list','#page-issue_list');await field.locator('#issueListBody .list-item').filter({hasText:'合成入口问题点'}).click();await field.locator('#issueDetailBody #actions').waitFor();assert.equal(await field.locator('#issueDetailBody #actions').count(),1);await capture(field,'001-issue-detail','#page-issue_detail');await field.locator('#page-issue_detail .back-btn').click();await capture(field,'001-issue-return','#page-issue_list');await field.locator('#page-issue_list .back-btn').click();await capture(field,'001-home-return','#page-home');
   }finally{await fieldCtx.close();}
   console.log('PASS entrance lifecycle width '+width);
  }catch(error){await p.screenshot({path:path.join(out,'failure-'+width+'.png'),fullPage:false}).catch(()=>{});fs.writeFileSync(path.join(out,'failure-dom-'+width+'.txt'),await p.locator('body').innerText().catch(()=>''));throw error;}finally{await ctx.close();}
 }
 assert.deepEqual(errors,[]);assert.equal(snapshot(),before,'Read-only browser navigation must not change synthetic business rows');
 const result={ok:true,sourceRoot,sourceHashes,widths:[1440,390],coverage,checks,errors,requests,unchangedBusiness:true,limitations};
 result.ok=checks.every(c=>c.ok);fs.writeFileSync(path.join(out,'result.json'),JSON.stringify(result,null,2));console.log(JSON.stringify({ok:result.ok,entries:coverage.length,delayedNavigation:checks.length,out}));assert.equal(result.ok,true,'Navigation race reproduced; see result.json and before/after screenshots');
}catch(error){for(const ctx of browser.contexts())for(const page of ctx.pages())await page.screenshot({path:path.join(out,'failure-'+Date.now()+'.png'),fullPage:false}).catch(()=>{});fs.writeFileSync(path.join(out,'failure.json'),JSON.stringify({error:error.stack,sourceRoot,coverage,checks,errors,requests},null,2));throw error;}
finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
