// Actual built /003 and authenticated Worker; loopback synthetic records only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import {createRequire} from 'node:module';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import entry from '../worker-v2/staging-entry.js';

const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const root=path.resolve('worker-v2/.sop-staging-assets'),f=await ownedHomeFixture(),errors=[],results=[];
const api=(a,b={})=>f.ok('office',a,{operator_id:'SYN-PURCHASE-BROWSER',operator_name:'合成采购表单员',...b});
const material=await api('v2_003_material_save',{name_zh:'合成表单采购耗材',category:'包材',unit:'个',opening_qty:0});
const snapshot=()=>JSON.stringify(['v2_003_purchase_orders','v2_003_purchase_order_lines','v2_003_purchase_shipments','v2_003_purchase_shipment_items','v2_003_materials','v2_003_material_txns'].map(table=>f.DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()));
f.env.ASSETS={async fetch(request){
 const u=new URL(request.url);let file=path.resolve(root,'.'+decodeURIComponent(u.pathname));
 if(file!==root&&!file.startsWith(root+path.sep))return new Response('denied',{status:403});
 try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}
}};
const server=http.createServer(async(req,res)=>{
 try{const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks),r=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(body.length?{body}:{})}),f.env),headers=Object.fromEntries(r.headers);
  if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));
 }catch(e){errors.push(e.message);res.writeHead(500);res.end(e.message);}
});
let browser;
try{
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
 browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:900}}),cookie=f.cookies.office;
  await context.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
  const page=await context.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
  try{
   await page.goto(origin+'/003/');await page.locator('#view-dashboard').waitFor({state:'visible'});
   for(const conflict of ['shipment','cancel','lines']){
    const order=await api('v2_003_purchase_request_create',{request_reason:'合成表单 '+width+' '+conflict,lines:[{material_id:material.id,requested_qty:10}]}),line=(await api('v2_003_purchase_order_detail',{id:order.id})).lines[0];
    await api('v2_003_purchase_order_update',{id:order.id,lines:[{id:line.id,ordered_qty:10,unit_cost:100}]});
    await page.evaluate(id=>openPurchaseDetail(id),order.id);await page.locator('#purchaseDetail [onclick="openPurchaseOrderModal()"]').click();await page.locator('#poLineEditor .po-qty').fill('5');
    const db=f.env.DB,original=db.batch.bind(db);let release,arrived,held=false;const gate=new Promise(r=>release=r),at=new Promise(r=>arrived=r);
    db.batch=async ss=>{if(!held&&ss.some(s=>/UPDATE\s+v2_003_purchase_order_lines\s+SET ordered_qty=/s.test(s.sql))){held=true;arrived();await gate;}return original(ss);};
    const reply=page.waitForResponse(r=>{try{return r.request().postDataJSON()?.action==='v2_003_purchase_order_update';}catch{return false;}});let before;
    try{
     await page.locator('#purchaseOrderModal button[type=submit]').click();let timer;try{await Promise.race([at,new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('browser purchase transaction not reached')),12000))]);}finally{clearTimeout(timer);}
     if(conflict==='shipment')await api('v2_003_purchase_shipment_create',{order_id:order.id,delivery_method:'express',tracking_no:'SYN-BROWSER-'+crypto.randomUUID(),items:[{order_line_id:line.id,expected_qty:8}]});
     if(conflict==='cancel')await api('v2_003_purchase_order_close',{id:order.id,mode:'cancel',reason:'合成表单取消'});
     if(conflict==='lines')f.DB.raw.prepare('DELETE FROM v2_003_purchase_order_lines WHERE id=?').run(line.id);
     before=snapshot();release();const result=await(await reply).json(),code={shipment:'ordered_qty_below_shipped',cancel:'purchase_order_closed',lines:'purchase_order_lines_changed'}[conflict];
     assert.equal(result.ok,false);assert.equal(result.error,code);await page.waitForFunction(()=>!S003.busy);assert.equal(snapshot(),before);await page.locator('#purchaseOrderModal').waitFor({state:'visible'});assert.equal(await page.locator('#poLineEditor .po-qty').inputValue(),'5');
     const messages=await page.evaluate(code=>{const old=S003.lang;S003.lang='zh';const zh=errorText(code);S003.lang='ko';const ko=errorText(code);S003.lang=old;return {zh,ko};},code);assert.notEqual(messages.zh,code);assert.notEqual(messages.ko,code);
     results.push({width,conflict,error:code,formRetained:true,businessUnchanged:true,translated:true});
    }finally{release();await reply.catch(()=>{});db.batch=original;}
    await page.locator('#purchaseOrderModal .modal-head button').click();
    if(conflict==='shipment'){
     await page.evaluate(id=>openPurchaseDetail(id),order.id);await page.locator('#purchaseDetail [onclick="openPurchaseOrderModal()"]').click();assert.equal(await page.locator('#poLineEditor .po-qty').getAttribute('min'),'8');await page.locator('#poLineEditor .po-qty').fill('8');
     const saved=page.waitForResponse(r=>{try{return r.request().postDataJSON()?.action==='v2_003_purchase_order_update';}catch{return false;}});await page.locator('#purchaseOrderModal button[type=submit]').click();assert.equal((await(await saved).json()).ok,true);await page.locator('#purchaseOrderModal').waitFor({state:'hidden'});assert.equal((await api('v2_003_purchase_order_detail',{id:order.id})).lines[0].ordered_qty,8);
     results.push({width,refreshedMin:8,validSave:true});
    }
   }
  }finally{await context.close();}
 }
 assert.deepEqual(errors,[]);console.log(JSON.stringify({ok:true,results,errors,limitations:'Local desktop Chromium viewports, not physical mobile devices or domestic network.'},null,2));
}finally{await browser?.close();if(server.listening)await new Promise(r=>server.close(r));f.DB.raw.close();}
