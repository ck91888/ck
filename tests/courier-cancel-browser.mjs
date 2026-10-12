// Actual /002 page and Worker; all plans, uploads and receipts use isolated synthetic storage.
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
const DB=database(),objects=new Map();
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('courier-cancel-fixture'),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);}}};
let cookie='';
async function call(action,body={}){
 const response=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
 const result=rememberOperation(await response.json(),cookie);assert.equal(result.ok,true,action+': '+(result.message||result.error));return result;
}
await call('sop_login',{sop_key:'courier-cancel-fixture'});await confirmOperation(call,'Synthetic service');
const A='301000091001',B='301000091002',ACTIVE='301000091003';
const active=await call('v2_inbound_plan_create',{customer:'Synthetic active parcel',biz_classes:['bulk'],lines:[{unit_type:'courier',tracking_nos:[ACTIVE]}]});
const root=path.resolve('worker-v2/.sop-staging-assets');
const artifacts=path.join(tmpdir(),'ck-courier-cancel'+(process.env.CK_EXPECT_CANCEL_BUG?'-baseline':''));fs.mkdirSync(artifacts,{recursive:true});
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
const context=await browser.newContext({viewport:{width:1440,height:1000}}),page=await context.newPage(),errors=[],dialogs=[];
page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));page.on('dialog',async dialog=>{dialogs.push(dialog.message());await dialog.accept(dialog.type()==='prompt'?'Synthetic cancellation':undefined);});
await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
await context.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
const submit=page.locator('button[onclick="submitInbound(this)"]');
const codes=page.locator('#ibcLinesBody textarea');
async function form(customer,numbers){
 await page.locator('#mainTabs [data-tab=inbound]').click();await page.locator('#btnNewInbound').click();
 await page.locator('#ibc-customer').fill(customer);await page.locator('.ibc-biz-chk[value=bulk]').check();
 await page.locator('#ibcLinesBody .ibc-line-type').selectOption('courier');await codes.fill(numbers.join('\n'));
 await page.locator('#ibc-cargo').fill('Synthetic cargo summary');await page.locator('#ibc-purpose').fill('Synthetic purpose');await page.locator('#ibc-remark').fill('Synthetic preserved note');
}
const formState=()=>page.locator('#view-inbound_create input,#view-inbound_create select,#view-inbound_create textarea').evaluateAll(elements=>elements.map(el=>({id:el.id,type:el.type,value:el.value,checked:el.checked,disabled:el.disabled})));
async function save(){const response=page.waitForResponse(r=>r.request().method()==='POST'&&r.request().postData()?.includes('"action":"v2_inbound_plan_create"'));await submit.click();const result=await(await response).json();await page.waitForFunction(()=>!window._actionLocks.submitInbound);return result;}
async function retained(numbers){assert.equal(await submit.isEnabled(),true);assert.doesNotMatch(await submit.innerText(),/提交中|저장중/);assert.equal(await codes.inputValue(),numbers.join('\n'));assert.equal(await page.locator('#ibc-customer').inputValue(),'Synthetic replacement parcel');assert.equal(await page.locator('#ibc-cargo').inputValue(),'Synthetic cargo summary');assert.equal(await page.locator('#ibc-purpose').inputValue(),'Synthetic purpose');assert.equal(await page.locator('#ibc-remark').inputValue(),'Synthetic preserved note');assert.equal(await page.locator('.ibc-biz-chk[value=bulk]').isChecked(),true);}
try{
 await page.goto(origin+'/002/');await form('Synthetic cancelled parcel',[A,B]);
 const original=await save();assert.equal(original.ok,true,JSON.stringify(original));
 // Legacy attachments are immutable fixtures; current work materials have their own upload flow.
 DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key) VALUES('ATT-SYNTHETIC','inbound_plan',?,'inbound_material','synthetic-original.csv','synthetic-original')").run(original.id);
 objects.set('synthetic-original',{body:Buffer.from('synthetic fixture')});
 const attachment=DB.raw.prepare('SELECT * FROM v2_attachments WHERE related_doc_id=?').get(original.id);assert.ok(attachment);assert.equal(objects.size,1);
 await page.locator('#inboundListBody .list-item').filter({hasText:'Synthetic cancelled parcel'}).click();
 await page.locator('[onclick="cancelInboundPlan(this)"]').click();await page.waitForFunction(()=>!window._actionLocks.cancelInboundPlan);
 assert.equal(DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(original.id).status,'cancelled');
 await form('Synthetic replacement parcel',[A,B,ACTIVE]);
 const beforeFailure=await formState();const conflict=await save();assert.equal(conflict.ok,false);await retained([A,B,ACTIVE]);assert.deepEqual(await formState(),beforeFailure);
 assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,2);assert.equal(DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(A).plan_id,original.id);assert.equal(DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(ACTIVE).plan_id,active.id);
 await page.screenshot({path:path.join(artifacts,'conflict-inputs-retained.png'),fullPage:true});
 await codes.fill([A,B].join('\n'));const replacement=await save();
 if(process.env.CK_EXPECT_CANCEL_BUG){
  assert.equal(replacement.ok,false);assert.match(replacement.message||replacement.error,/已关联/);await retained([A,B]);await page.screenshot({path:path.join(artifacts,'cancelled-plan-blocks-reentry.png'),fullPage:true});
  console.log('PASS baseline reproduced actual /002 cancelled courier conflict; failed submission restored button and retained every tested field.');
 }else{
  assert.equal(replacement.ok,true,JSON.stringify(replacement));assert.notEqual(replacement.id,original.id);
  for(const code of [A,B])assert.equal(DB.raw.prepare('SELECT plan_id FROM ck_courier_plan_items WHERE tracking_no=?').get(code).plan_id,replacement.id);
  assert.deepEqual(DB.raw.prepare('SELECT * FROM v2_attachments WHERE id=?').get(attachment.id),attachment);assert.equal(objects.size,1);
  const history=await call('v2_inbound_plan_detail',{id:original.id});assert.equal(history.plan.status,'cancelled');assert.deepEqual(history.plan.courier_progress.items.map(x=>x.tracking_no).sort(),[A,B]);assert.equal(history.plan.courier_progress.received,0);
  await page.locator('#inboundListBody .list-item').filter({hasText:'Synthetic replacement parcel'}).click();await page.locator('#inboundDetailBody').getByText(A,{exact:true}).waitFor();assert.match(await page.locator('#inboundDetailBody').innerText(),new RegExp(A));await page.screenshot({path:path.join(artifacts,'replacement-created.png'),fullPage:true});
  console.log('PASS actual /002 multi-line courier creation → cancellation → active-conflict atomic failure with input/button retention → successful re-entry; original cancelled history, tracking numbers and attachment preserved.');
 }
 assert.deepEqual(errors,[]);console.log('Artifacts: '+artifacts);fs.writeFileSync(path.join(artifacts,'dialogs.json'),JSON.stringify(dialogs,null,2));
}catch(error){console.error('JS errors',errors);console.error('PAGE',await page.locator('body').innerText());await page.screenshot({path:path.join(artifacts,'failure.png'),fullPage:true}).catch(()=>{});throw error;
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
