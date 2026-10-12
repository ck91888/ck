// Built /002 + real Worker; every person, unload, plan and photo is synthetic and isolated.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

const require=createRequire(import.meta.url);
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),objects=new Map(),artifacts=path.join(tmpdir(),'ck-feedback-create');
fs.mkdirSync(artifacts,{recursive:true});
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('feedback-create-fixture'),R2_BUCKET:{
 async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},
 async get(key){return objects.get(key);},async delete(key){objects.delete(key);}
}};
let cookie='';
async function raw(body){
 const multi=body instanceof FormData;
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{Cookie:cookie,...operationHeaders(cookie),...(multi?{}:{'Content-Type':'application/json'})},body:multi?body:JSON.stringify(body)}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 return rememberOperation(await response.json(),cookie);
}
async function call(action,body={}){const r=await raw({action,client_req_id:crypto.randomUUID(),...body});assert.equal(r.ok,true,action+': '+(r.message||r.error));return r;}
await call('sop_login',{sop_key:'feedback-create-fixture'});await confirmOperation(call,'Synthetic feedback creator');
const crew=[];
for(const name of ['Synthetic unload lead','Synthetic unload two','Synthetic unload three'])crew.push((await call('sop_attendance_checkin',{name,agency:'가온'})).record);
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aB9sAAAAASUVORK5CYII=','base64');
async function unload(label){
 const started=await call('sop_native_start',{payload:{action:'v2_unplanned_unload_start',cargo_summary:'Synthetic '+label+' cargo',customer:'SOURCE-MARK-'+label,source_info:'Synthetic source '+label,remark:'Synthetic original '+label,client_req_id:crypto.randomUUID()},workers:crew.map(p=>({id:p.badgeId,name:p.name})),lead_id:crew[0].badgeId,estimated_minutes:15});
 const job_id=started.job_id,feedback_id=DB.raw.prepare('SELECT related_doc_id FROM v2_ops_jobs WHERE id=?').get(job_id).related_doc_id;
 DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-20*60000).toISOString(),job_id);
 const photo=new FormData();
 for(const [k,v]of Object.entries({action:'v2_attachment_upload',attachment_category:'unload_photo',job_id,related_doc_type:'field_feedback',related_doc_id:feedback_id,client_req_id:crypto.randomUUID()}))photo.set(k,v);
 photo.set('file',new File([png],'Synthetic-'+label+'.png',{type:'image/png'}));
 const uploaded=await raw(photo);assert.equal(uploaded.ok,true,uploaded.message||uploaded.error);
 await call('v2_unplanned_unload_finish',{job_id,worker_id:crew[0].badgeId,complete_job:true,result_lines:[{unit_type:'pallet',actual_qty:1}],remark:'Synthetic finished '+label});
 return {label,job_id,feedback_id,photo:uploaded.attachment};
}
const sources={};for(const label of ['cancel','bulk','direct','retry','race','late','preview'])sources[label]=await unload(label);
const sourceSnapshot=s=>({
 workers:DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY id').all(s.job_id),
 results:DB.raw.prepare('SELECT * FROM v2_ops_job_results WHERE job_id=? ORDER BY id').all(s.job_id),
 photos:DB.raw.prepare('SELECT * FROM v2_attachments WHERE related_doc_type=? AND related_doc_id=? ORDER BY id').all('field_feedback',s.feedback_id),
 job:DB.raw.prepare('SELECT created_at,updated_at,shared_result_json,status,active_worker_count FROM v2_ops_jobs WHERE id=?').get(s.job_id),
 feedback:DB.raw.prepare('SELECT content,result_lines_json,completed_at,completed_by,remark FROM v2_field_feedbacks WHERE id=?').get(s.feedback_id)
});
const originals=new Map(Object.values(sources).map(s=>[s.feedback_id,sourceSnapshot(s)]));
const count=table=>DB.raw.prepare('SELECT count(*) n FROM '+table).get().n;
const state=id=>DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(id);
const sourceState=s=>DB.raw.prepare('SELECT * FROM v2_field_feedbacks WHERE id=?').get(s.feedback_id);
const immutable=s=>assert.deepEqual(sourceSnapshot(s),originals.get(s.feedback_id),'Original unload remains unchanged: '+s.label);
const root=path.resolve(process.env.CK_BROWSER_ASSETS||'worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(root,'002/index.html')),'Build staging assets before this test; the test never builds or deploys.');
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://127.0.0.1');
 if(url.pathname.endsWith('/api')||url.pathname.endsWith('/api/file')){
  const chunks=[];for await(const chunk of req)chunks.push(chunk);
  const response=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env);
  const headers=Object.fromEntries(response.headers);if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();res.writeHead(response.status,headers);res.end(new Uint8Array(await response.arrayBuffer()));return;
 }
 let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
}catch(error){res.writeHead(500);res.end(error.message);}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const context=await browser.newContext({viewport:{width:1440,height:1000}}),page=await context.newPage(),errors=[],dialogs=[],requests=[],scenarios=[];
page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
page.on('dialog',async dialog=>{dialogs.push(dialog.message());await dialog.accept(dialog.type()==='prompt'?'Synthetic confirmation':undefined);});
page.on('request',request=>{if(request.method()==='POST'&&request.headers()['content-type']?.includes('application/json')){try{requests.push(request.postDataJSON());}catch{}}});
page.on('response',async response=>{if(response.request().headers()['content-type']?.includes('multipart/form-data')){const body=await response.json().catch(()=>null);if(body&&!body.ok)console.error('UPLOAD ERROR',JSON.stringify(body));}});
await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
await context.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
const submit=page.locator('button[onclick="submitInbound(this)"]'),banner=page.locator('[data-feedback-create-source]');
const frames=()=>page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
async function openFeedback(s){
 await page.locator('#mainTabs [data-tab=feedback]').click();
 await page.locator('#feedbackListBody .list-item').filter({hasText:sourceState(s).display_no}).click();
 await page.locator('#feedbackDetailBody').filter({hasText:sourceState(s).display_no}).waitFor();
}
async function fromFeedback(s){await openFeedback(s);await page.locator('[data-feedback-create]').click();await page.locator('#view-inbound_create').waitFor({state:'visible'});await banner.waitFor({state:'visible'});await page.waitForFunction(()=>document.querySelector('#ibcLinesBody .ibc-line-type')?.value==='pallet');}
async function ordinary(customer,biz='bulk'){
 await page.locator('#mainTabs [data-tab=inbound]').click();await page.locator('#btnNewInbound').click();
 await page.locator('#ibc-customer').fill(customer);await page.locator('.ibc-biz-chk[value='+biz+']').check();
 await page.locator('#ibcLinesBody .ibc-line-type').first().selectOption('carton');await page.locator('#ibcLinesBody .ibc-line-qty').first().fill('5');
}
async function valid(customer,biz='bulk'){await page.locator('#ibc-customer').fill(customer);await page.locator('.ibc-biz-chk[value='+biz+']').check();}
async function workMaterial(title,name,buffer){
 await page.locator('[data-add-work]').click();const row=page.locator('[data-work-row]').last();
 await row.locator('[data-work=title]').fill(title);await row.locator('[data-work=instructions]').fill('Synthetic instructions');await row.locator('[data-work=planned_quantity]').fill('1');
 await row.locator('[data-plan-upload]').setInputFiles({name,mimeType:'text/csv',buffer});
}
async function save(){
 const response=page.waitForResponse(r=>r.request().method()==='POST'&&r.request().postData()?.includes('"action":"v2_inbound_plan_create"'));
 await submit.click();const result=await(await response).json();await page.waitForFunction(()=>!window._actionLocks?.submitInbound);return result;
}
async function detail(id){await page.evaluate(id=>openInboundDetail(id),id);await page.locator('#inboundDetailBody').filter({hasText:state(id).customer}).waitFor();}
function record(name){scenarios.push(name);console.log('PASS '+name);}
const requestBody=request=>request.headers()['content-type']?.includes('application/json')?request.postDataJSON():null;
async function heldResponse(predicate){
 let ready,release,delivered;const entered=new Promise(r=>ready=r),done=new Promise(r=>delivered=r);let used=false,result;
 const handler=async route=>{const body=requestBody(route.request());if(!used&&predicate(body)){used=true;const response=await route.fetch();result=await response.json();ready();await new Promise(r=>release=r);await route.fulfill({response});delivered();return;}return route.continue();};
 await page.route('**/api',handler);
 return {entered,get result(){return result;},async finish(){release();await done;await page.unroute('**/api',handler);await frames();}};
}
try{
 await page.goto(origin+'/002/');
 // Source entry uses the complete ordinary form, with no guessed customer or business.
 await fromFeedback(sources.cancel);
 assert.equal(await page.locator('#ibc-customer').inputValue(),'');assert.equal(await page.locator('.ibc-biz-chk:checked').count(),0);
 assert.equal(await page.locator('.ibc-line-type').first().inputValue(),'pallet');assert.equal(await page.locator('.ibc-line-qty').first().inputValue(),'1');
 assert.match(await page.locator('#ibc-remark').inputValue(),/SOURCE-MARK-cancel/);assert.match(await banner.innerText(),new RegExp(sourceState(sources.cancel).display_no));
 assert.equal(count('v2_inbound_plans'),0);const cancelBefore=sourceState(sources.cancel);
 await page.screenshot({path:path.join(artifacts,'source-standard-form-desktop.png'),fullPage:true});
 await page.setViewportSize({width:390,height:844});await page.evaluate(()=>{setLang('ko');applyLang();});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.ok((await page.locator('#ibc-customer').boundingBox()).width>240);
 await page.screenshot({path:path.join(artifacts,'source-standard-form-mobile-ko.png'),fullPage:true});
 await page.locator('[data-feedback-create-cancel]').click();await page.locator('#view-feedback_detail').waitFor({state:'visible'});
 assert.equal(count('v2_inbound_plans'),0);assert.deepEqual(sourceState(sources.cancel),cancelBefore);immutable(sources.cancel);
 await page.locator('#feedbackDetailBody .ck-feedback-link input').waitFor();
 await page.locator('#feedbackDetailBody .ck-feedback-link input').fill('Synthetic no match');
 await page.locator('#feedbackDetailBody .ck-feedback-link .ck-feedback-message').filter({hasText:/没有找到|없습니다/}).waitFor();
 record('full standard form preserves source meaning; cancel has no writes; existing-plan search remains; 390px Korean no overflow');

 await page.setViewportSize({width:1440,height:1000});await page.evaluate(()=>{setLang('zh');applyLang();});
 await fromFeedback(sources.cancel);await ordinary('Synthetic ordinary after source');assert.equal(await banner.isVisible(),false);assert.doesNotMatch(await page.locator('#ibc-remark').inputValue(),/SOURCE-MARK/);
 const plain=await save();assert.equal(plain.ok,true,JSON.stringify(plain));assert.ok(!requests.filter(r=>r.action==='v2_inbound_plan_create').at(-1).feedback_context);assert.equal(sourceState(sources.cancel).status,'unloaded_pending_info');
 record('ordinary new form clears feedback source and sends ordinary create payload');

 await fromFeedback(sources.cancel);await valid('Synthetic failed source create');await page.locator('#ibc-remark').fill('Synthetic failed draft preserved');const failedBefore=count('v2_inbound_plans');
 DB.raw.exec("CREATE TRIGGER synthetic_feedback_create_failure BEFORE UPDATE ON v2_field_feedbacks BEGIN SELECT RAISE(ABORT,'Synthetic transaction unavailable'); END");
 try{const failed=await save();assert.equal(failed.ok,false);assert.equal(count('v2_inbound_plans'),failedBefore);assert.equal(await page.locator('#ibc-customer').inputValue(),'Synthetic failed source create');assert.equal(await page.locator('#ibc-remark').inputValue(),'Synthetic failed draft preserved');assert.equal(await banner.isVisible(),true);assert.equal(await submit.isEnabled(),true);immutable(sources.cancel);}finally{DB.raw.exec('DROP TRIGGER synthetic_feedback_create_failure');}
 record('transaction failure preserves source form inputs and leaves no orphan plan');

 await fromFeedback(sources.bulk);await valid('Synthetic source bulk');const beforeBulk=count('v2_inbound_plans');
 const double=await heldResponse(b=>b?.action==='v2_inbound_plan_create'&&b.customer==='Synthetic source bulk');
 await submit.evaluate(button=>{button.click();button.click();});await double.entered;
 assert.equal(requests.filter(r=>r.action==='v2_inbound_plan_create'&&r.customer==='Synthetic source bulk').length,1);
 await double.finish();await page.waitForFunction(()=>!window._actionLocks?.submitInbound);
 const bulk=double.result;assert.equal(bulk.ok,true,JSON.stringify(bulk));assert.equal(count('v2_inbound_plans'),beforeBulk+1);assert.equal(state(bulk.id).status,'completed');
 const bulkRequest=requests.find(r=>r.action==='v2_inbound_plan_create'&&r.customer==='Synthetic source bulk');assert.equal(bulkRequest.feedback_context.feedback_id,sources.bulk.feedback_id);assert.ok(bulkRequest.feedback_context.feedback_version);assert.ok(bulkRequest.feedback_context.job_version);
 const bulkDetail=await call('v2_inbound_plan_detail',{id:bulk.id});assert.equal(bulkDetail.jobs[0].id,sources.bulk.job_id);assert.equal(bulkDetail.unload_summary.completed,true);assert.equal(bulkDetail.unload_summary.result_lines[0].actual_qty,1);assert.equal(bulkDetail.arrival_photos[0].id,sources.bulk.photo.id);
 assert.equal(bulkDetail.lines[0].unit_type,'pallet');assert.equal(bulkDetail.lines[0].planned_qty,1);assert.equal(bulkDetail.lines[0].actual_qty,1);immutable(sources.bulk);
 await detail(bulk.id);await page.screenshot({path:path.join(artifacts,'linked-bulk-detail.png'),fullPage:true});
 const downloaded=await page.evaluate(async key=>{const r=await fetch('/002/api?action=v2_attachment_get&file_key='+encodeURIComponent(key));return {status:r.status,bytes:[...new Uint8Array(await r.arrayBuffer())]};},sources.bulk.photo.file_key);assert.equal(downloaded.status,200);assert.deepEqual(Buffer.from(downloaded.bytes),png);
 await page.locator('#inboundDetailBody .ck-feedback-origin button').click();await page.locator('#feedbackDetailBody [data-feedback-create-open]').waitFor();assert.equal(await page.locator('[data-feedback-create]').count(),0);await page.locator('#feedbackDetailBody [data-feedback-create-open]').click();await page.locator('#view-inbound_detail').waitFor({state:'visible'});await page.waitForFunction(id=>window._currentInboundId===id,bulk.id);
 record('double click creates one bulk plan; original job, crew, labor, quantities and exact photo bytes remain; already linked opens existing plan');

 await fromFeedback(sources.direct);await valid('Synthetic source direct','direct_ship');assert.equal(await page.locator('#ck-ibc-external').inputValue(),'');
 const direct=await save();assert.equal(direct.ok,true,JSON.stringify(direct));assert.equal(state(direct.id).status,'arrived_pending_putaway');
 const directDetail=await call('v2_inbound_plan_detail',{id:direct.id});assert.equal(directDetail.unload_summary.completed,true);assert.equal(directDetail.jobs.filter(j=>j.job_type==='unload').length,1);assert.equal(DB.raw.prepare('SELECT status FROM v2_inbound_plan_biz_tasks WHERE plan_id=? AND biz_class=?').get(direct.id,'direct_ship').status,'pending');immutable(sources.direct);
 const blockedPutaway=await raw({action:'sop_native_start',payload:{action:'v2_inbound_job_start',plan_id:direct.id,job_type:'inbound_direct',biz_class:'direct_ship',external_inbound_no:'',client_req_id:crypto.randomUUID()},workers:[{id:crew[0].badgeId,name:crew[0].name}],lead_id:crew[0].badgeId,estimated_minutes:10,client_req_id:crypto.randomUUID()});assert.equal(blockedPutaway.ok,false);assert.match(blockedPutaway.error||blockedPutaway.message,/外部|单号|번호/);
 record('direct_ship can be registered without invented references; putaway stays pending and cannot start without a reference');

 await fromFeedback(sources.retry);await valid('Synthetic lost response');let dropped=false,committed;
 const material=Buffer.from('Synthetic inbound material retained across lost create response\nitem,quantity\npallet,1\n');
 await workMaterial('Synthetic retry work','Synthetic-retry-material.csv',material);
 const lostHandler=async route=>{const body=requestBody(route.request());if(!dropped&&body?.action==='v2_inbound_plan_create'&&body.customer==='Synthetic lost response'){dropped=true;const response=await route.fetch();committed=await response.json();await route.abort('failed');return;}return route.continue();};
 await page.route('**/api',lostHandler);const retryBefore=count('v2_inbound_plans');await submit.click();await page.waitForFunction(()=>!window._actionLocks?.submitInbound);assert.equal(dropped,true);assert.equal(committed.ok,true);assert.equal(count('v2_inbound_plans'),retryBefore+1);assert.equal(await page.locator('#ibc-customer').inputValue(),'Synthetic lost response');assert.equal(await banner.isVisible(),true);
 assert.match(await page.locator('[data-work-row] [data-plan-bound]').innerText(),/Synthetic-retry-material.csv/);assert.equal(DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need'").get().n,1);
 await page.unroute('**/api',lostHandler);const retry=await save();assert.equal(retry.ok,true,JSON.stringify(retry));assert.equal(retry.id,committed.id);assert.ok(!retry.already_linked);assert.equal(count('v2_inbound_plans'),retryBefore+1);immutable(sources.retry);
 const retriedRequests=requests.filter(r=>r.action==='v2_inbound_plan_create'&&r.customer==='Synthetic lost response');assert.equal(retriedRequests.length,2);assert.equal(retriedRequests[0].client_req_id,retriedRequests[1].client_req_id);
 const retryDetail=await call('v2_inbound_plan_detail',{id:retry.id});assert.equal(retryDetail.inbound_materials.length,1);assert.equal(retryDetail.inbound_materials[0].file_name,'Synthetic-retry-material.csv');assert.deepEqual(Buffer.from(objects.get(retryDetail.inbound_materials[0].file_key).body),material);
 assert.equal(DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need'").get().n,1);
 record('lost successful response preserves draft and work materials; same-intent retry returns the same plan, one work request and exact original material bytes');

 await fromFeedback(sources.race);await valid('Synthetic losing create');const rival=await call('v2_inbound_plan_create',{customer:'Synthetic concurrent existing',biz_classes:['bulk'],lines:[{unit_type:'pallet',planned_qty:1}]});
 const link=await call('sop_feedback_link_preview',{feedback_id:sources.race.feedback_id,plan_id:rival.id});
 await call('sop_feedback_link_save',{feedback_id:sources.race.feedback_id,plan_id:rival.id,feedback_version:link.feedback_version,plan_version:link.plan_version,job_version:link.job_version,lines:link.lines.map(l=>({line_id:l.id,unit_type:l.unit_type,actual_qty:l.actual_qty}))});
 const raceBefore=count('v2_inbound_plans'),race=await save();assert.equal(count('v2_inbound_plans'),raceBefore);assert.equal(sourceState(sources.race).inbound_plan_id,rival.id);assert.equal(race.ok,true,JSON.stringify(race));assert.equal(race.already_linked,true);assert.equal(race.id,rival.id);immutable(sources.race);
 record('concurrent existing-plan association cannot leave a duplicate or orphan new plan');

 await openFeedback(sources.preview);const previewHold=await heldResponse(b=>b?.action==='sop_feedback_link_create_preview'&&b.feedback_id===sources.preview.feedback_id);
 await page.locator('[data-feedback-create]').click();await previewHold.entered;await ordinary('Synthetic preview newer form');await previewHold.finish();
 assert.equal(await page.locator('#view-inbound_create').isVisible(),true);assert.equal(await page.locator('#ibc-customer').inputValue(),'Synthetic preview newer form');assert.equal(await page.locator('.ibc-line-type').first().inputValue(),'carton');assert.equal(await banner.isVisible(),false);immutable(sources.preview);
 record('late source preview cannot replace a newer ordinary form');

 await fromFeedback(sources.late);await valid('Synthetic late source save');const sourceHold=await heldResponse(b=>b?.action==='v2_inbound_plan_create'&&b.customer==='Synthetic late source save');
 const ordinaryMaterial=Buffer.from('Synthetic ordinary frozen material A'),newSourceMaterial=Buffer.from('Synthetic newer source draft material B');
 await submit.click();await sourceHold.entered;await ordinary('Synthetic draft after source save','direct_ship');await page.locator('#ibc-remark').fill('Synthetic keep this draft');await page.locator('#ck-ibc-external').fill('SYNTHETIC-DRAFT-A');await workMaterial('Synthetic newer ordinary work','Synthetic-draft-A.csv',ordinaryMaterial);await sourceHold.finish();await page.waitForFunction(()=>!window._actionLocks?.submitInbound);
 assert.equal(sourceHold.result.ok,true,JSON.stringify(sourceHold.result));assert.equal(await page.locator('#view-inbound_create').isVisible(),true);assert.equal(await page.locator('#ibc-customer').inputValue(),'Synthetic draft after source save');assert.equal(await page.locator('#ibc-remark').inputValue(),'Synthetic keep this draft');assert.equal(await page.locator('#ck-ibc-external').inputValue(),'SYNTHETIC-DRAFT-A');assert.match(await page.locator('[data-work-row] [data-plan-bound]').innerText(),/Synthetic-draft-A.csv/);assert.equal(await banner.isVisible(),false);immutable(sources.late);
 record('late source save commits its source without wiping or navigating away from a newer draft');

 const plainHold=await heldResponse(b=>b?.action==='v2_inbound_plan_create'&&b.customer==='Synthetic draft after source save');
 await submit.click();await plainHold.entered;await fromFeedback(sources.preview);await valid('Synthetic source after ordinary save','direct_ship');await page.locator('#ibc-remark').fill('Synthetic source draft remains');await page.locator('#ck-ibc-external').fill('SYNTHETIC-DRAFT-B');await workMaterial('Synthetic newer source work','Synthetic-draft-B.csv',newSourceMaterial);await plainHold.finish();await page.waitForFunction(()=>!window._actionLocks?.submitInbound);
 assert.equal(plainHold.result.ok,true,JSON.stringify(plainHold.result));assert.equal(await page.locator('#view-inbound_create').isVisible(),true);assert.equal(await page.locator('#ibc-customer').inputValue(),'Synthetic source after ordinary save');assert.equal(await page.locator('#ibc-remark').inputValue(),'Synthetic source draft remains');assert.equal(await page.locator('#ck-ibc-external').inputValue(),'SYNTHETIC-DRAFT-B');assert.match(await page.locator('[data-work-row] [data-plan-bound]').innerText(),/Synthetic-draft-B.csv/);assert.doesNotMatch(await page.locator('[data-work-row] [data-plan-bound]').innerText(),/Synthetic-draft-A.csv/);assert.equal(await banner.isVisible(),true);
 const plainLateDetail=await call('v2_inbound_plan_detail',{id:plainHold.result.id});assert.equal(plainLateDetail.inbound_materials.length,1);assert.equal(plainLateDetail.inbound_materials[0].file_name,'Synthetic-draft-A.csv');assert.deepEqual(Buffer.from(objects.get(plainLateDetail.inbound_materials[0].file_key).body),ordinaryMaterial);
 record('late ordinary create response preserves newer source form, references and selected file; only its original material uploads to its own plan');

 for(const s of Object.values(sources))immutable(s);assert.equal(count('v2_ops_jobs'),Object.keys(sources).length);assert.equal(count('v2_ops_job_results'),Object.keys(sources).length);assert.equal(count('v2_ops_job_workers'),Object.keys(sources).length*crew.length);assert.equal(objects.size,Object.keys(sources).length+2);assert.deepEqual(errors,[]);
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:true,pageErrors:errors,dialogs,scenarios,counts:{plans:count('v2_inbound_plans'),originalUnloads:count('v2_ops_jobs'),originalWorkerSegments:count('v2_ops_job_workers'),originalResults:count('v2_ops_job_results'),originalPhotos:DB.raw.prepare("SELECT count(*) n FROM v2_attachments WHERE attachment_category='unload_photo'").get().n,newInboundMaterials:retryDetail.inbound_materials.length+plainLateDetail.inbound_materials.length,totalStoredObjects:objects.size},screenshots:['source-standard-form-desktop.png','source-standard-form-mobile-ko.png','linked-bulk-detail.png']},null,2));
 console.log('PASS actual /002 source-aware standard create, real atomic Worker links, immutable unload evidence, retry and asynchronous form isolation. Artifacts: '+artifacts);
}catch(error){
 fs.writeFileSync(path.join(artifacts,'acceptance.json'),JSON.stringify({ok:false,error:error.stack,pageErrors:errors,dialogs,scenarios},null,2));console.error('JS errors',errors);console.error('PAGE',await page.locator('body').innerText());await page.screenshot({path:path.join(artifacts,'failure.png'),fullPage:true}).catch(()=>{});throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
