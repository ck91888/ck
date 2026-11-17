// Actual /002 rich check UI, candidate Worker, in-memory synthetic evidence only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {unifiedCheckFixture} from './unified-check-fixture.mjs';

const scriptRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const sourceRoot=path.resolve(process.env.CK_SMOKE_ROOT||scriptRoot),assetsRoot=path.join(sourceRoot,'worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(assetsRoot,'002/index.html')),'Build CK_SMOKE_ROOT/worker-v2/build-sop-staging.mjs first');
const source=relative=>import(pathToFileURL(path.join(sourceRoot,relative)).href);
const [{default:worker},{default:entry},{database},{digest},{operationHeaders,rememberOperation,confirmOperation}]=await Promise.all([
 source('worker-v2/index.js'),source('worker-v2/staging-entry.js'),source('tests/d1-adapter.mjs'),source('worker-v2/access-control.js'),source('tests/office-operator-fixture.mjs')
]);
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const out=path.resolve(process.env.CK_CHECKS_OUT||'/tmp/ck-unified-check-browser');fs.mkdirSync(out,{recursive:true});
const hashFiles=['shared/sop-entry.js','shared/sop-native.js','shared/sop-native.css','worker-v2/sop.js','worker-v2/check-evidence.js','001/app.js','002/app.js'];
const hashes=()=>Object.fromEntries(hashFiles.filter(f=>fs.existsSync(path.join(sourceRoot,f))).map(f=>[f,createHash('sha256').update(fs.readFileSync(path.join(sourceRoot,f))).digest('hex')]));
const sourceHashes=hashes(),DB=database();
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ACCEPT_NEW:'true',SOP_PUBLIC_TEST_ACCESS:'false',SOP_ADMIN_CODE_SHA256:await digest('synthetic-full-check-only')};
let cookie='';
async function call(action,data={}){
 const r=await worker.fetch(new Request('http://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];const result=rememberOperation(await r.json(),cookie);assert.equal(result.ok,true,action+': '+result.error);return result;
}
await call('sop_login',{sop_key:'synthetic-full-check-only'});await confirmOperation(call,'合成全量核对验收员');
const fixture=await unifiedCheckFixture(DB,call,{volume:20001}),small=await unifiedCheckFixture(DB,call,{volume:6001,shipDate:'2026-11-02'});
await call('sop_need_create',{source_type:'inventory',supply_chain_no:'SYNTHETIC-CHECK-NAV-STOCK',department:'bulk',customer:'合成核对导航',title:'合成核对返回作业计划',instructions:'只读入口导航测试'});
const snapshot=()=>JSON.stringify({records:DB.raw.prepare('SELECT id,state,revision FROM sop_records ORDER BY id').all(),events:DB.raw.prepare('SELECT * FROM sop_events ORDER BY rowid').all(),logs:DB.raw.prepare('SELECT * FROM v2_verify_scan_logs ORDER BY id').all()});
const before=snapshot(),apiChecks=[],flows=[],visual=[],errors=[],events=[];
if(process.env.CK_CHECKS_VISUAL_ONLY!=='1')for(const f of [fixture,small]){
 const detail=await call('sop_get',{id:f.id}),e=detail.record.scan_evidence;
 assert.ok(e,'Full scan_evidence must be present');assert.equal(e.complete,true);assert.equal(e.total,f.volume);assert.equal(detail.record.revision,f.revision);
 assert.equal(e.page_size,100);assert.equal(e.logs.length,100);assert.equal(e.offset,0);
 const b=new Map(e.barcodes.map(x=>[x.barcode,x]));
 for(const [barcode,wanted]of Object.entries(f.evidence)){
  const got=b.get(barcode);assert.ok(got,barcode+' aggregate absent');for(const key of ['total','ok','overflow','not_found','duplicate','last_at'])assert.equal(got[key],wanted[key],barcode+' '+key);
  assert.deepEqual(got.pallets.map(p=>p.pallet_no).sort(),wanted.pallets,barcode+' full pallet coverage');
 }
 assert.equal(b.get('000-LARGE').pallets.length,242);assert.ok(b.get('000-LARGE').pallets.some(p=>p.pallet_no==='SYNTHETIC-OLDEST-PALLET'));
 const p=new Map(e.pallets.map(x=>[x.pallet_no,x]));assert.equal(p.size,Object.keys(f.palletEvidence).length);
 for(const [pallet,wanted]of Object.entries(f.palletEvidence))for(const key of ['total','ok','abnormal','last_at'])assert.equal(p.get(pallet)[key],wanted[key],pallet+' '+key);
 const first=await call('sop_check_scan_logs',{id:f.id,offset:0,limit:200}),next=await call('sop_check_scan_logs',{id:f.id,offset:200,limit:200}),last=await call('sop_check_scan_logs',{id:f.id,offset:f.volume-1,limit:200});
 assert.equal(first.total,f.volume);assert.equal(first.page_size,200);assert.equal(first.logs.length,200);assert.equal(next.logs.length,200);assert.equal(new Set([...first.logs,...next.logs].map(l=>l.id)).size,400);
 assert.equal(last.logs.length,1);assert.equal(last.logs[0].pallet_no,'SYNTHETIC-OLDEST-PALLET');assert.equal(last.logs[0].id,f.expectedLogs[0].id);
 apiChecks.push({volume:f.volume,revision:f.revision,barcodes:b.size,pallets:p.size,largeBarcodePallets:242,first:200,next:200,last:1,ok:true});
}
env.ASSETS={async fetch(request){const u=new URL(request.url);let f=path.resolve(assetsRoot,'.'+decodeURIComponent(u.pathname));if(f!==assetsRoot&&!f.startsWith(assetsRoot+path.sep))return new Response('denied',{status:403});try{if(fs.statSync(f).isDirectory())f=path.join(f,'index.html');return new Response(fs.readFileSync(f),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(f)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}}};
const server=http.createServer(async(req,res)=>{try{const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks);const r=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(body.length?{body}:{})}),env);const bytes=new Uint8Array(await r.arrayBuffer());if(req.url.includes('/api'))try{events.push({input:JSON.parse(body.toString()),result:JSON.parse(new TextDecoder().decode(bytes))});}catch{}const headers=Object.fromEntries(r.headers);if(r.headers.getSetCookie().length)headers['set-cookie']=r.headers.getSetCookie();res.writeHead(r.status,headers);res.end(bytes);}catch(error){errors.push({source:'server',message:error.message});res.writeHead(500);res.end(error.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const input=r=>{try{return r.request().postDataJSON();}catch{return{};}};
const root=p=>p.locator('#checkDetailBody'),rows=p=>root(p).locator('#items>tr'),row=(p,barcode)=>rows(p).filter({has: p.locator('td:first-child',{hasText:barcode})});
async function shot(p,name){const cdp=await p.context().newCDPSession(p);try{const r=await cdp.send('Page.captureScreenshot',{format:'png',fromSurface:true,captureBeyondViewport:false});fs.writeFileSync(path.join(out,name+'.png'),Buffer.from(r.data,'base64'));}finally{await cdp.detach();}}
async function prepared(ctx){
 await ctx.addCookies([{name:cookie.slice(0,cookie.indexOf('=')),value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);const p=await ctx.newPage();p.setDefaultTimeout(15000);p.on('pageerror',e=>errors.push({source:'browser',message:e.message,url:p.url()}));p.on('dialog',d=>d.dismiss());await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
 await p.goto(origin+'/002/');await p.locator('#mainTabs [data-tab=check]').click();await p.locator('#checkListBody .list-item').filter({hasText:'2026-11-01'}).click();await root(p).locator('[data-check-filters]').waitFor();assert.equal(await root(p).locator('#actions').count(),1);assert.equal(await root(p).locator('[onclick*=updateVerifyBatch]').count(),0);return p;
}
async function filter(p,key){await root(p).locator('[data-check-filter="'+key+'"]').click();assert.equal(await rows(p).count(),fixture.filterCounts[key],key+' filter rows');assert.equal(await root(p).locator('[data-check-filter="'+key+'"]').getAttribute('aria-pressed'),'true');}
async function logs(p){const d=root(p).locator('[data-check-logs]>details');await d.evaluate(n=>n.open=true);return d;}
async function waitLogPage(p,offset,run){const pending=p.waitForResponse(r=>input(r).action==='sop_check_scan_logs'&&input(r).offset===offset);await run();const result=await(await pending).json();assert.equal(result.ok,true,result.error);await p.waitForFunction(offset=>document.querySelector('[data-check-log-status]')?.textContent.startsWith(String(offset+1)+'–'),offset);return result;}
try{
 if(process.env.CK_CHECKS_VISUAL_ONLY!=='1')for(const width of [1440,390]){
  const ctx=await browser.newContext({viewport:{width,height:width<400?844:1000}});let p;
  try{
   p=await prepared(ctx);assert.match(await root(p).locator('[data-check-overview]').innerText(),/20001/);assert.match(await root(p).locator('[data-check-overview]').innerText(),new RegExp(String(fixture.planned)));
   for(const key of Object.keys(fixture.filterCounts))await filter(p,key);
   await filter(p,'all');const large=row(p,'000-LARGE'),short=row(p,'000-SHORT'),over=row(p,'000-OVER'),none=row(p,'000-NONE'),recheck=row(p,'000-RECHECK'),moved=row(p,'000-MOVED'),dup=row(p,'000-DUP'),outside=row(p,'000-OUTSIDE');
   assert.equal(await large.locator('td').nth(3).innerText(),String(fixture.large));assert.equal(await large.locator('td').nth(4).innerText(),'0');assert.match(await large.locator('td').nth(5).innerText(),/242/);await large.locator('td').nth(5).locator('summary').click();assert.match(await large.locator('td').nth(5).innerText(),/SYNTHETIC-OLDEST-PALLET/);assert.doesNotMatch(await large.locator('td').nth(6).innerText(),/未记录|미기록|^—$/);await large.locator('td').nth(5).locator('summary').click();
   assert.match(await short.locator('td').nth(7).innerText(),/少扫|부족/);assert.equal(await short.locator('td').nth(4).innerText(),'-3');assert.match(await over.locator('td').nth(7).innerText(),/超扫|초과/);assert.equal(await over.locator('td').nth(4).innerText(),'+1');assert.match(await none.locator('td').nth(7).innerText(),/未扫|미스캔/);assert.match(await none.locator('td').nth(5).innerText(),/未记录|미기록/);assert.match(await none.locator('td').nth(6).innerText(),/未记录|미기록/);
   assert.match(await recheck.locator('td').nth(7).innerText(),/待复核|재확인/);assert.match(await recheck.locator('td').nth(3).innerText(),/有效核对\s*0|유효 확인\s*0/);assert.equal(await recheck.locator('td').nth(4).innerText(),'-2');assert.match(await moved.locator('td').nth(7).innerText(),/待复核|재확인/);assert.match(await dup.locator('td').nth(7).innerText(),/核对一致|수량 일치/);assert.match(await dup.innerText(),/重复|중복/);assert.match(await outside.locator('td').nth(3).innerText(),/^2\s+.*0$/);
   await filter(p,'removed');assert.match(await row(p,'000-REMOVED').innerText(),/合成客户撤销/);assert.equal(await row(p,'000-REMOVED').getByRole('button').count(),0);await filter(p,'all');await root(p).locator('.ck-check-table').scrollIntoViewIfNeeded();await shot(p,'functional-table-'+width);
   const pallets=root(p).locator('[data-check-pallets]>details');await pallets.evaluate(n=>n.open=true);const oldest=pallets.locator('[data-check-pallet="SYNTHETIC-OLDEST-PALLET"]');await oldest.locator('summary').click();assert.match(await oldest.innerText(),/000-LARGE/);assert.match(await oldest.innerText(),new RegExp(String(fixture.large)));await oldest.scrollIntoViewIfNeeded();await shot(p,'functional-oldest-pallet-'+width);
   const log=await logs(p);assert.equal(await log.locator('[data-check-log-rows]>tr').count(),100);assert.match(await log.locator('[data-check-log-status]').innerText(),/1–100 \/ 20001/);const firstLog=await log.locator('[data-check-log-rows]').innerText();
   let failed=false;const fail=async route=>{const b=input({request:()=>route.request()});if(b.action==='sop_check_scan_logs'&&b.offset===100&&!failed){failed=true;return route.fulfill({status:503,json:{ok:false,error:'合成日志分页失败 / 합성 페이지 오류'}});}return route.fallback();};await p.route('**/api',fail);await log.locator('[data-check-log-next]').click();await log.locator('[data-check-log-error]').getByText(/合成日志分页失败/).waitFor();assert.equal(await log.locator('[data-check-log-rows]').innerText(),firstLog);await p.unroute('**/api',fail);
   const second=await waitLogPage(p,100,()=>log.locator('[data-check-log-next]').click());assert.equal(second.total,20001);assert.equal(second.logs.length,100);assert.equal(await log.locator('[data-check-log-error]').innerText(),'');await log.scrollIntoViewIfNeeded();await shot(p,'functional-logs-page2-'+width);await waitLogPage(p,0,()=>log.locator('[data-check-log-prev]').click());assert.equal(await log.locator('[data-check-log-rows]').innerText(),firstLog);
   // A late pagination result cannot replace the work-plan page or destroy it.
   let entered,release;const seen=new Promise(r=>entered=r),gate=new Promise(r=>release=r),delay=async route=>{const b=input({request:()=>route.request()});if(b.action==='sop_check_scan_logs'&&b.offset===100){entered();await gate;}return route.fallback();};await p.route('**/api',delay);await log.locator('[data-check-log-next]').click();await seen;await p.locator('[data-tab=need]').click();await p.locator('#view-need [data-plan-filters]').waitFor();const late=p.waitForResponse(r=>input(r).action==='sop_check_scan_logs'&&input(r).offset===100);release();await late;await p.unroute('**/api',delay);await p.waitForTimeout(150);assert.equal(await p.locator('#view-need [data-plan-filters]').count(),1);assert.equal(await p.locator('#view-check_detail').isVisible(),false);
   flows.push({width,filters:fixture.filterCounts,allPallets:242,logsFirst:100,logsSecond:100,failureRetry:true,latePageGuard:true,ok:true});console.log('PASS rich check flow '+width);
  }catch(error){if(p){await shot(p,'failure-functional-'+width).catch(()=>{});fs.writeFileSync(path.join(out,'failure-dom-'+width+'.txt'),await p.locator('body').innerText().catch(()=>''));}throw error;}finally{await ctx.close();}
 }
 if(process.env.CK_CHECKS_SKIP_VISUAL!=='1')for(const spec of [{width:1440,zoom:1},{width:1280,zoom:1},{width:1440,zoom:1.25},{width:1280,zoom:1.25},{width:1440,zoom:1.5},{width:1280,zoom:1.5},{width:390,zoom:1},{width:320,zoom:1}]){
  const profile=fs.mkdtempSync('/tmp/ck-check-visual-'),ctx=await chromium.launchPersistentContext(profile,{executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,viewport:{width:spec.width,height:spec.width<400?844:1000},args:['--no-sandbox']});let p;
  try{
   const settings=await ctx.newPage();await settings.goto('chrome://settings/appearance');await settings.evaluate(z=>new Promise(r=>chrome.settingsPrivate.setDefaultZoom(z,r)),spec.zoom);await settings.close();p=await prepared(ctx);const tag=spec.width+'-'+Math.round(spec.zoom*100);await root(p).locator('[data-check-overview]').scrollIntoViewIfNeeded();await shot(p,'overview-'+tag);await filter(p,'all');await root(p).locator('.ck-check-table').scrollIntoViewIfNeeded();await shot(p,'barcodes-'+tag);
   const metric=await p.evaluate(()=>({innerWidth,devicePixelRatio,pageWidth:document.documentElement.scrollWidth,columns:document.querySelector('#items')?.closest('table').querySelectorAll('thead th').length,rows:document.querySelectorAll('#items>tr').length,tableWidth:document.querySelector('.ck-check-table').getBoundingClientRect().width,filters:[...document.querySelectorAll('[data-check-filter]')].map(n=>({height:n.getBoundingClientRect().height,left:n.getBoundingClientRect().left,right:n.getBoundingClientRect().right}))}));assert.ok(Math.abs(metric.innerWidth-spec.width/spec.zoom)<2);assert.equal(metric.devicePixelRatio,spec.zoom);assert.ok(metric.pageWidth<=metric.innerWidth+1,JSON.stringify(metric));assert.equal(metric.columns,9);assert.equal(metric.rows,8);assert.ok(metric.filters.every(b=>b.left>=0&&b.right<=metric.innerWidth+1),JSON.stringify(metric));
   const pallets=root(p).locator('[data-check-pallets]>details');await pallets.evaluate(n=>n.open=true);const oldest=pallets.locator('[data-check-pallet="SYNTHETIC-OLDEST-PALLET"]');await oldest.locator('summary').click();await oldest.scrollIntoViewIfNeeded();await shot(p,'pallets-'+tag);const log=await logs(p);await log.scrollIntoViewIfNeeded();await shot(p,'logs-'+tag);await waitLogPage(p,100,()=>log.locator('[data-check-log-next]').click());await log.scrollIntoViewIfNeeded();await shot(p,'logs-page2-'+tag);
   visual.push({...spec,...metric});console.log('PASS rich check visual '+tag);
  }catch(error){if(p)await shot(p,'failure-visual-'+spec.width+'-'+Math.round(spec.zoom*100)).catch(()=>{});throw error;}finally{await ctx.close();fs.rmSync(profile,{recursive:true,force:true});}
 }
 assert.deepEqual(errors,[]);assert.equal(snapshot(),before,'Read-only evidence and pagination must not change revisions, audit, or scan logs');assert.deepEqual(hashes(),sourceHashes,'Candidate source changed during the browser run');
 fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({ok:true,sourceRoot,sourceHashes,apiChecks,flows,visual,errors,unchangedBusiness:true,limitations:['Synthetic local evidence only; no production reads or writes.','Chromium viewport/zoom coverage; no physical iPhone or Safari tested.','No scanning, changing quantity, revoking, moving date, resolving, saving round or final handover performed in this read-only UI script.']},null,2));console.log(JSON.stringify({ok:true,api:apiChecks.length,flows:flows.length,visual:visual.length,out}));
}catch(error){fs.writeFileSync(path.join(out,'failure.json'),JSON.stringify({error:error.stack,sourceRoot,sourceHashes,apiChecks,flows,visual,errors,events},null,2));throw error;}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
