// Real built collaboration UI + authenticated Worker, isolated synthetic SQLite.
import assert from 'node:assert/strict';import fs from 'node:fs';import http from 'node:http';import path from 'node:path';import {createRequire} from 'node:module';
import entry from '../worker-v2/staging-entry.js';import {database} from './d1-adapter.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
import {quantityCases,quantityColumns,quantityValues,seedQuantityPlan} from './inbound-export-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]})),XLSX=require('../shared/xlsx.full.min.js');
const root=path.resolve('worker-v2/.sop-staging-assets'),out='/tmp/ck-inbound-export-browser';fs.mkdirSync(out,{recursive:true});
const DB=database(),errors=[],requests=[],checks=[];
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:'Synthetic-export-test',ASSETS:{async fetch(r){let f=path.join(root,new URL(r.url).pathname);if(fs.statSync(f).isDirectory())f=path.join(f,'index.html');return new Response(fs.readFileSync(f),{headers:{'Content-Type':f.endsWith('.js')?'text/javascript':f.endsWith('.css')?'text/css':'text/html'}});}}};
let cookie='';async function call(action,body={}){
 const r=await entry.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];const j=rememberOperation(await r.json(),cookie);assert.equal(j.ok,true,j.error);return j;
}
await call('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await confirmOperation(call,'QA 合成导出核查员');
const expected=new Map();
for(let i=0;i<88;i++){
 const spec=quantityCases[i]||{id:'page-'+i,lines:[['courier',i+1,i,0]],expected:[i+1,i,0,0]};
 expected.set(seedQuantityPlan(DB,spec,i),spec.expected);
}
DB.raw.exec("UPDATE v2_inbound_plans SET accounted=1,unload_completed_at='2026-10-12T15:00:00Z'");
const snapshot=()=>JSON.stringify(['v2_inbound_plans','v2_inbound_plan_lines'].map(t=>DB.raw.prepare('SELECT * FROM '+t+' ORDER BY id').all())),before=snapshot();
const all=(await call('v2_inbound_plan_export')).rows;
const server=http.createServer(async(req,res)=>{try{const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks);let input={};try{input=JSON.parse(body);}catch{}if(input.action)requests.push(input);const r=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body}:{})}),env);res.writeHead(r.status,Object.fromEntries(r.headers));res.end(Buffer.from(await r.arrayBuffer()));}catch(e){errors.push(e.message);res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const input=r=>{try{return r.request().postDataJSON()||{};}catch{return{};}};
async function download(p,tag,rows){
 const response=p.waitForResponse(r=>input(r).action==='v2_inbound_plan_export'),event=p.waitForEvent('download');
 await p.locator('#ibExportBtn').click();assert.deepEqual((await(await response).json()).rows.map(r=>r.plan_id),rows.map(r=>r.plan_id));
 const file=await event,filename=path.join(out,tag+'.csv');await file.saveAs(filename);assert.match(file.suggestedFilename(),/^inbound_plans_.*\.csv$/);
 const bytes=fs.readFileSync(filename);assert.equal(bytes.subarray(0,3).toString('hex'),'efbbbf');assert.ok(bytes.includes(Buffer.from('\r\n')));
 const wb=XLSX.read(bytes,{type:'buffer'}),sheet=wb.Sheets[wb.SheetNames[0]],parsed=XLSX.utils.sheet_to_json(sheet,{defval:''});
 assert.equal(parsed.length,rows.length);assert.deepEqual(parsed.map(r=>r.plan_id),rows.map(r=>r.plan_id));
 const headers=XLSX.utils.sheet_to_json(sheet,{header:1})[0];
 for(const [i,row] of parsed.entries()){
  assert.deepEqual(quantityValues(row),expected.get(row.plan_id));
  for(const k of quantityColumns)assert.equal(sheet[XLSX.utils.encode_cell({r:i+1,c:headers.indexOf(k)})].t,'n',k+' must be numeric');
  assert.equal(row['货物摘要'],rows[i]['货物摘要']);
 }
 checks.push({tag,rows:parsed.length,bom:true,numericQuantities:true});
}
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try{
 for(const spec of [{width:1440,lang:'zh'},{width:390,lang:'ko'}]){
  const context=await browser.newContext({viewport:{width:spec.width,height:900},acceptDownloads:true});
  await context.addCookies([{name:cookie.split('=')[0],value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
  const p=await context.newPage();p.setDefaultTimeout(12000);p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>d.dismiss());await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
  const initial=p.waitForResponse(r=>input(r).action==='v2_inbound_plan_list');await p.goto(origin+'/002/?tab=inbound');assert.equal((await(await initial).json()).total,88);
  if(spec.lang==='ko')await p.evaluate(()=>setLang('ko'));
  await p.waitForFunction(()=>document.querySelectorAll('#inboundListBody .list-item').length===50);
  const next=p.waitForResponse(r=>input(r).action==='v2_inbound_plan_list'&&input(r).offset===50);
  await p.locator('#inboundListBody .pager-actions button').filter({hasText:/下一页|다음/}).first().click();const page2=await(await next).json();assert.equal(page2.items.length,38);
  await p.waitForFunction(()=>document.querySelectorAll('#inboundListBody .list-item').length===38);
  await p.locator('#view-inbound .ck-filter-drawer').evaluate(n=>n.open=true);
  await download(p,'all-from-page2-'+spec.width,all);
  // Every current filter is submitted together; export still covers all matching pages.
  await p.evaluate(()=>{for(const [id,value] of Object.entries({ibFilterStart:'2026-10-01',ibFilterEnd:'2026-10-01',ibFilterArrivalFrom:'2026-10-01',ibFilterArrivalTo:'2026-10-01',ibFilterUnloadDoneFrom:'2026-10-13',ibFilterUnloadDoneTo:'2026-10-13',ibFilterStatus:'pending',ibFilterBizClass:'bulk',ibFilterCustomer:'合成导出',inboundFilterAccounted:'1'}))document.getElementById(id).value=value;});
  const filtered=p.waitForResponse(r=>input(r).action==='v2_inbound_plan_list'&&input(r).expected_arrival_to==='2026-10-01');await p.locator('#ibQueryBtn').click();assert.equal((await(await filtered).json()).total,4);
  const selected=all.filter(r=>r['预计到达日期']==='2026-10-01');await download(p,'filtered-'+spec.width,selected);
  const exported=requests.filter(r=>r.action==='v2_inbound_plan_export').at(-1);for(const key of ['start_date','end_date','expected_arrival_from','expected_arrival_to','unload_done_date_from','unload_done_date_to','status','biz_class','customer_keyword','accounted'])assert.ok(exported[key],key);
  await p.locator('#inboundListBody .list-item').filter({hasText:'QA-EXPORT-courier-eight'}).click();await p.locator('#view-inbound_detail').waitFor({state:'visible'});
  const line=p.locator('#inboundDetailBody .line-table tbody tr').filter({hasText:/快递|택배/}).first();await line.waitFor();assert.deepEqual((await line.locator('td').allTextContents()).slice(1,3),['8','8']);
  await line.scrollIntoViewIfNeeded();await p.screenshot({path:path.join(out,'detail-eight-'+spec.width+'.png')});
  await context.close();
 }
 assert.deepEqual(errors,[]);assert.equal(snapshot(),before);
 fs.writeFileSync(path.join(out,'report.json'),JSON.stringify({ok:true,checks,unchangedBusinessData:true,errors},null,2));console.log(JSON.stringify({ok:true,checks},null,2));
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
