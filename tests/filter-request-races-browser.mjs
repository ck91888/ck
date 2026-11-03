// Built current apps and real local Worker. Delayed reads and identities are synthetic; no live network or business writes.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import http from 'node:http';import {createHash} from 'node:crypto';import {createRequire} from 'node:module';
import entry from '../worker-v2/staging-entry.js';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
const repo=path.resolve(new URL('../',import.meta.url).pathname),assets=repo+'/worker-v2/.sop-staging-assets',out=process.env.CK_FILTER_BROWSER_OUTPUT||'/tmp/ck-filter-request-races-browser';fs.mkdirSync(out,{recursive:true});
const sha=b=>createHash('sha256').update(b).digest('hex'),hashes={};for(const file of ['002/app.js','003/app.js','shuju/app.js','shared/sop-entry.js','shared/latest-list-read.js']){assert.equal(sha(fs.readFileSync(repo+'/'+file)),sha(fs.readFileSync(assets+'/'+file)));hashes[file]=sha(fs.readFileSync(repo+'/'+file));}
const f=await ownedHomeFixture(),results=[],errors=[];for(const tag of ['OLD','NEW'])f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,display_no,customer,status,biz_class,order_date,expected_ship_at,created_at) VALUES(?,?,?,'pending_issue','bulk','2026-11-03','2026-11-03','2026-11-03T00:00:00Z')").run('SYN-RACE-'+tag,'SYN-RACE-'+tag,'SYN-'+tag);
const tables=['sop_records','sop_events','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','v2_outbound_orders','v2_003_materials','v2_003_material_txns'],snapshot=()=>JSON.stringify(tables.map(t=>f.DB.raw.prepare('SELECT * FROM '+t+' ORDER BY rowid').all()));
f.env.ASSETS={async fetch(r){let file=path.resolve(assets,'.'+new URL(r.url).pathname);if(file!==assets&&!file.startsWith(assets+path.sep))return new Response('denied',{status:403});try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('missing',{status:404});}}};
let wire=null;
const server=http.createServer(async(req,res)=>{try{const chunks=[];for await(const c of req)chunks.push(c);const body=Buffer.concat(chunks);let p={};try{p=JSON.parse(body.toString());}catch{}const response=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(body.length?{body}:{})}),f.env),bytes=new Uint8Array(await response.arrayBuffer());if(wire&&p.action==='v2_outbound_order_list'&&['SYN-OLD','SYN-NEW'].includes(p.customer_keyword)){let release;const held=new Promise(r=>release=r);wire.push({tag:p.customer_keyword,p,release});await held;}const h=Object.fromEntries(response.headers);if(response.headers.getSetCookie().length)h['set-cookie']=response.headers.getSetCookie();res.writeHead(response.status,h);res.end(bytes);}catch(e){errors.push(e.message);res.writeHead(500);res.end(e.message);}});
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES||'/opt/codex/runtimes/codex-primary-runtime/dependencies/node/node_modules']}));let browser;
async function spin(predicate){const until=Date.now()+12000;while(!predicate()){assert.ok(Date.now()<until,'wire requests did not arrive');await new Promise(r=>setTimeout(r,10));}}
try{
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});const before=snapshot();
 for(const width of [1440,390]){
  const ctx=await browser.newContext({viewport:{width,height:900}}),cookie=f.cookies.office;await ctx.addCookies([{name:cookie.split('=')[0],value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);const page=await ctx.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));await page.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
  try{
   await page.goto(origin+'/002/');await page.locator('#mainTabs').waitFor();await page.locator('#mainTabs [data-tab=outbound]').click();await page.locator('#outboundListBody .list-item').first().waitFor();const drawer=page.locator('#view-outbound .ck-filter-drawer');if(!await drawer.evaluate(n=>n.open))await drawer.locator('summary').click();
   for(const mode of ['late-old','early-unlock']){
    wire=[];const old=page.waitForResponse(r=>{try{return r.request().postDataJSON()?.customer_keyword==='SYN-OLD';}catch{return false;}}),fresh=page.waitForResponse(r=>{try{return r.request().postDataJSON()?.customer_keyword==='SYN-NEW';}catch{return false;}});
    try{await page.locator('#obFilterCustomer').fill('SYN-OLD');await page.locator('#obFilterCustomer').press('Enter');await page.locator('#obFilterCustomer').fill('SYN-NEW');await page.locator('#obFilterCustomer').press('Enter');await spin(()=>wire.length===2);
     const choose=tag=>wire.find(x=>x.tag==='SYN-'+tag);
     if(mode==='late-old'){choose('NEW').release();await fresh;await page.locator('#outboundListBody .list-item').filter({hasText:'SYN-RACE-NEW'}).waitFor();choose('OLD').release();await(await old).finished();await page.evaluate(()=>new Promise(requestAnimationFrame));assert.doesNotMatch(await page.locator('#outboundListBody').innerText(),/SYN-RACE-OLD/);assert.match(await page.locator('#outboundListBody').innerText(),/SYN-RACE-NEW/);results.push({width,kind:'outbound-wire',mode,filter:await page.locator('#obFilterCustomer').inputValue(),displayed:'SYN-RACE-NEW',new_then_old:true});}
     else{choose('OLD').release();await(await old).finished();await page.evaluate(()=>new Promise(requestAnimationFrame));assert.doesNotMatch(await page.locator('#outboundListBody').innerText(),/SYN-RACE-OLD/);results.push({width,kind:'outbound-wire',mode,new_request_pending:true,query_enabled:await page.locator('#obQueryBtn').isEnabled()});assert.equal(results.at(-1).query_enabled,false);choose('NEW').release();await fresh;await page.locator('#outboundListBody .list-item').filter({hasText:'SYN-RACE-NEW'}).waitFor();}
    }finally{for(const h of wire||[])h.release();wire=null;await Promise.allSettled([old,fresh]);}
   }
   for(const section of ['002','shuju','003']){
    if(section!=='002'){await page.goto(origin+'/'+section+'/');await page.waitForFunction(s=>s==='003'?window.S003?.role==='admin':window.CKSession?.user&&typeof loadOrders==='function',section);}
    const kinds=section==='002'?['outbound','inbound']:section==='shuju'?['orders','workhours']:['materials','ledger'];
    for(const kind of kinds)for(const mode of ['late-success','late-error','late-reject','identity',...(kind==='orders'?['aba']:[])]){
     const row=await page.evaluate(async({kind,mode})=>{
      const original=api,originalSession=window.CKSession,pending=[],tag=n=>'SYN-'+n,good=name=>{
       const item={id:tag(name),display_no:tag(name),customer:tag(name),status:'pending_issue',biz_class:'bulk',created_at:'2026-11-03T00:00:00Z'};
       if(kind==='materials')return {ok:true,items:[{id:tag(name),material_code:tag(name),name_zh:tag(name),category:'合成',unit:'个',current_qty:1,status:'active'}]};
       if(kind==='ledger')return {ok:true,items:[{id:tag(name),kind:'material',item_code:tag(name),item_name:tag(name),action_type:'inbound',qty_delta:1,operator_name:'合成',created_at:'2026-11-03T00:00:00Z',note:tag(name)}]};
       if(kind==='orders')return {ok:true,total:1,items:[{...item,job_no:tag(name),job_type:'bulk_op',status:'working',result_summary:tag(name)}],metadata:{snapshot:tag(name),as_of:'2026-11-03T01:00:00Z'}};
       if(kind==='workhours')return {ok:true,total:1,summary:{worker_count:1,job_count:1},by_worker:[{worker_name:tag(name),worker_id:tag(name),total_minutes:1}],segments:[],metadata:{snapshot:tag(name),as_of:'2026-11-03T01:00:00Z'}};
       return {ok:true,total:name==='OLD'?99:1,limit:50,offset:0,items:[{...item,status:kind==='inbound'?'pending':'pending_issue',biz_classes:['bulk']}]};
      };
      const configs={outbound:['v2_outbound_order_list','obFilterCustomer','outboundListBody',()=>loadOutboundList()],inbound:['v2_inbound_plan_list','ibFilterCustomer','inboundListBody',()=>loadInboundList()],orders:['v2_dashboard_order_list','ordersFilterDoc','ordersBody',()=>loadOrders()],workhours:['v2_dashboard_workhour_summary','whFilterWorker','whByWorkerBody',()=>loadWorkhours()],materials:['v2_003_material_list','materialSearch','materialList',()=>loadMaterials()],ledger:['v2_003_ledger_list','ledgerSearch','ledgerList',()=>loadLedger()]},[action,field,host,load]=configs[kind];
      api=(a,b)=>{const p=typeof a==='string'?{action:a,...b}:a;if(p.action===action)return new Promise((resolve,reject)=>pending.push({p,resolve,reject}));return original(a,b);};
      try{
       document.getElementById(field).value='OLD';const a=load();if(mode==='identity')window.CKSession={scope:originalSession.scope,user:{...originalSession.user,id:'SYN-NEW-IDENTITY'}};document.getElementById(field).value='NEW';const b=load();let c;
       if(mode==='aba'){document.getElementById(field).value='OLD';c=load();pending[2].resolve(good('LATEST-OLD'));await c;pending[1].resolve(good('NEW'));await b;pending[0].resolve(good('OLD'));await a;}
       else{pending[1].resolve(good('NEW'));await b;if(mode==='late-error'||mode==='late-reject'){if(mode==='late-reject'||kind==='materials'||kind==='ledger')pending[0].reject(Error('SYN-OLD-ERROR'));else pending[0].resolve({ok:false,error:'SYN-OLD-ERROR'});}else pending[0].resolve(good('OLD'));await a;}
       const text=document.getElementById(host).textContent,row={kind,mode,filter:document.getElementById(field).value,text:text.slice(0,260)};if(kind==='workhours')row.error_text=document.getElementById('whSummaryRow').textContent;
       if(kind==='orders'){row.cached_id=_ordersItems[0]?.id;row.saved_filter=_ordersQuery?.doc_no;row.snapshot=_ordersQuery?.snapshot;}
       if(kind==='workhours'){row.saved_filter=_whQuery?.worker_name;row.cached_worker=_whSummary?.by_worker[0]?.worker_name;const save=exportActualReport;let captured;exportActualReport=async p=>{captured=p;};try{await exportWorkhoursSegments();}finally{exportActualReport=save;}row.export_filter=captured?.worker_name;}
       if(kind==='outbound'||kind==='inbound')row.pager={...getPager(kind)};
       if(kind==='materials')row.cached_id=S003.materialRows[0]?.id;
       if(kind==='ledger'){row.cached_id=S003.ledgerRows[0]?.id;if(mode==='late-success'){const create=URL.createObjectURL,click=HTMLAnchorElement.prototype.click;let blob;URL.createObjectURL=b=>{blob=b;return create(b);};HTMLAnchorElement.prototype.click=()=>{};try{exportLedgerCsv();row.csv=await blob.text();}finally{URL.createObjectURL=create;HTMLAnchorElement.prototype.click=click;}}}
       return row;
      }finally{api=original;window.CKSession=originalSession;}
     },{kind,mode});
     row.width=width;row.mode_source='controlled deferred responses in actual current application functions';
     assert.ok(row.text.includes(mode==='aba'?'SYN-LATEST-OLD':'SYN-NEW'),JSON.stringify(row));assert.ok(!row.text.includes('SYN-OLD-ERROR'),JSON.stringify(row));if(kind==='workhours')assert.ok(!row.error_text.includes('SYN-OLD-ERROR'));if(mode==='aba')assert.equal(row.snapshot,'SYN-LATEST-OLD');if(kind==='orders')assert.equal(row.cached_id,mode==='aba'?'SYN-LATEST-OLD':'SYN-NEW');if(kind==='ledger'&&mode==='late-success'){assert.match(row.csv,/SYN-NEW/);assert.doesNotMatch(row.csv,/SYN-OLD/);}if(kind==='workhours')assert.equal(row.export_filter,'NEW');
     results.push(row);
    }
   }
   await page.goto(origin+'/002/');await page.locator('#mainTabs').waitFor();
   for(const kind of ['check','issue']){
    const row=await page.evaluate(async kind=>{
     const original=kind==='check'?api:readIssue,pending=[];let load,host;
     if(kind==='check'){api=p=>p.action==='v2_verify_batch_list'?new Promise(resolve=>pending.push(resolve)):original(p);load=()=>loadVerifyList();host='checkListBody';}
     else{readIssue=()=>new Promise(resolve=>pending.push(resolve));load=()=>loadIssueList();host='issueListBody';}
     try{const a=load(),b=load();pending[1]({ok:true,total:0,items:[]});await b;const fresh=document.getElementById(host).textContent;pending[0]({ok:false,error:'SYN-OLD-ERROR'});await a;return {kind,guarded:true,unchanged:document.getElementById(host).textContent===fresh};}finally{if(kind==='check')api=original;else readIssue=original;}
    },kind);assert.equal(row.unchanged,true);results.push({...row,width,mode_source:'positive existing request-identity controls'});
   }
  }finally{await ctx.close();}
 }
 assert.equal(snapshot(),before,'all list regression reads preserve synthetic business and labor rows');assert.deepEqual(errors,[]);
 fs.writeFileSync(out+'/report.json',JSON.stringify({source:'current built worktree',local_synthetic_read_only:true,existing_asset_manifest:JSON.parse(fs.readFileSync(assets+'/release.json')).commit,verified_module_hashes:hashes,results,errors,business_unchanged:true},null,2));console.log(JSON.stringify({ok:true,observations:results.length,out}));
}catch(e){fs.writeFileSync(out+'/failure.json',JSON.stringify({error:e.stack,results,errors},null,2));throw e;}finally{for(const h of wire||[])h.release();await browser?.close();if(server.listening)await new Promise(r=>server.close(r));f.DB.raw.close();}
