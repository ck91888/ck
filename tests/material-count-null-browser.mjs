// Built /002 list controls + real Worker, synthetic loopback data only.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import http from 'node:http';import {createRequire} from 'node:module';import {mock} from 'node:test';
import entry from '../worker-v2/staging-entry.js';import {outboundFixture} from './outbound-page-material-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
const f=await outboundFixture(undefined,{orders:10}),root=path.resolve('worker-v2/.sop-staging-assets'),results=[],errors=[];
f.env.ASSETS={async fetch(r){let file=path.resolve(root,'.'+new URL(r.url).pathname);if(file!==root&&!file.startsWith(root+path.sep))return new Response('denied',{status:403});try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}}};
const server=http.createServer(async(req,res)=>{try{const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks),r=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(body.length?{body}:{})}),f.env),headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(new Uint8Array(await r.arrayBuffer()));}catch(e){errors.push(e.message);res.writeHead(500);res.end(e.message);}});
let browser;
try{
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
 browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:900}}),cookie=f.cookies.office;await context.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));await page.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
  const waitList=()=>page.waitForResponse(r=>{try{return r.request().postDataJSON()?.action==='v2_outbound_order_list';}catch{return false;}}),card=page.locator('#outboundListBody .list-item').filter({hasText:'SYN-NO-6'});
  try{
   await page.goto(origin+'/002/');let response=waitList();await page.locator('#mainTabs [data-tab=outbound]').click();assert.equal((await(await response).json()).ok,true);
   const drawer=page.locator('#view-outbound .ck-filter-drawer');if(!await drawer.evaluate(n=>n.open))await drawer.locator('summary').click();
   for(const [name,value]of [['null',null],['false',false],['zero',0],['empty-string','']]){
    await f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify({status:'pending',source_type:'outbound',source_id:f.oid(6),links:[],cargo_groups:value}),'SYN-JSON-NULL');const before=await f.snapshot();
    response=waitList();await page.locator('#obFilterHasMaterial').selectOption('1');let r=await(await response).json();assert.equal(r.items.find(x=>x.id===f.oid(6)).material_count,1);await card.waitFor({state:'visible'});assert.match(await card.innerText(),/\[.* 1\]/);
    response=waitList();await page.locator('#obFilterHasMaterial').selectOption('0');r=await(await response).json();assert.equal(r.items.some(x=>x.id===f.oid(6)),false);await card.waitFor({state:'detached'});
    const detail=await f.okTarget('office','v2_outbound_order_detail',{id:f.oid(6)});assert.equal(detail.order.material_count,1);assert.equal(await f.snapshot(),before);results.push({width,value:name,list:1,detail:1,inHas:true,inNone:false,unchanged:true});
   }
  }finally{await context.close();}
 }
 assert.deepEqual(errors,[]);console.log(JSON.stringify({ok:true,results,errors,limitations:'Desktop Chromium viewports; not a physical phone or domestic network test.'},null,2));
}finally{await browser?.close();if(server.listening)await new Promise(r=>server.close(r));await f.close();mock.timers.reset();}
