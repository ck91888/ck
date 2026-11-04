// Loopback only: real browser decoding/URLs, synthetic raster and cookie authorization.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import {createRequire} from 'node:module';
import {png} from './issue-attachment-fixtures.mjs';
const require=createRequire(import.meta.url);
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const source=fs.readFileSync('shared/issue-images.js','utf8');
assert.equal(fs.readFileSync('worker-v2/.sop-staging-assets/shared/issue-images.js','utf8'),source);
const gates=new Map(),counts=new Map(),results=[],errors=[];
const gate=name=>{let enter,release;const started=new Promise(r=>enter=r),wait=new Promise(r=>release=r);const value={started,wait,enter,release};gates.set(name,value);return value;};
const html=`<!doctype html><html><meta name="viewport" content="width=device-width,initial-scale=1"><style>[hidden]{display:none!important}#lightbox{display:none}#lightbox.show{display:block;position:fixed;inset:0;background:white}img{max-width:100%;max-height:75vh}</style><body><main id="photos"></main><img id="pending"><div id="lightbox"></div><script>
window._currentView='issue-detail';window.CKSession={scope:'office',user:{id:'A',role:'manager',departments:['bulk'],operation_context:{id:'operator-A'}}};
window.fixtureUser=id=>CKSession.user=id?{id,role:'manager',departments:['bulk'],operation_context:{id:'operator-'+id}}:null;
window.cache={urls:new Set(),revoked:[]};const create=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);
URL.createObjectURL=b=>{const u=create(b);cache.urls.add(u);return u;};URL.revokeObjectURL=u=>{cache.urls.delete(u);cache.revoked.push(u);revoke(u);};
window.foreign=URL.createObjectURL(new Blob(['synthetic pending upload']));document.querySelector('#pending').src=foreign;
</script><script src="/issue-images.js"></script></body></html>`;
const server=http.createServer(async(req,res)=>{
 const u=new URL(req.url,'http://127.0.0.1');res.setHeader('Cache-Control','no-store');
 if(u.pathname==='/'){res.setHeader('Content-Type','text/html;charset=utf-8');res.end(html);return;}
 if(u.pathname==='/issue-images.js'){res.setHeader('Content-Type','text/javascript');res.end(source);return;}
 if(u.pathname==='/file'){
  const name=u.searchParams.get('case');counts.set(name,(counts.get(name)||0)+1);
  const authorized=String(req.headers.cookie||'').includes('synthetic_identity=A');
  const hold=gates.get(name);if(hold){hold.enter();await hold.wait;}
  if(!authorized){res.writeHead(403);res.end('Synthetic permission denied');return;}
  res.setHeader('Content-Type','image/png');res.end(png);return;
 }
 res.writeHead(404);res.end();
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try{for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:900}}),page=await context.newPage();page.setDefaultTimeout(10000);
 page.on('pageerror',e=>errors.push(e.message));await page.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
 const change=async id=>{await context.addCookies([{name:'synthetic_identity',value:id||'locked',url:origin}]);await page.evaluate(id=>fixtureUser(id),id);};
 const fresh=async()=>{await context.addCookies([{name:'synthetic_identity',value:'A',url:origin}]);await page.goto(origin);await page.waitForFunction(()=>window.CKIssueImages);};
 const open=name=>page.evaluate(name=>CKIssueImages.open('/file?case='+name,'Synthetic.png'),name);
 const ready=()=>page.waitForFunction(()=>document.querySelector('[data-view-status]')?.textContent.includes('已加载')&&document.querySelector('#lightboxImg').naturalWidth>0);
 const empty=()=>page.waitForFunction(()=>[...cache.urls].every(u=>u===foreign));
 const foreign=async()=>assert.equal(await page.evaluate(()=>cache.urls.has(window.foreign)&&!cache.revoked.includes(window.foreign)&&document.querySelector('#pending').src===window.foreign),true);
 await fresh();let name=width+'-account';await open(name);await ready();await page.evaluate(()=>CKIssueImages.close());await change('B');await open(name);
 await page.locator('[data-view-retry]:visible').waitFor();assert.match(await page.locator('[data-view-status]').innerText(),/403/);await empty();assert.equal(counts.get(name),2);await foreign();
 await change('A');await open(name);await ready();assert.equal(counts.get(name),3);results.push({width,case:'account-cache-isolation-and-authorized-reopen',passed:true});
 await fresh();name=width+'-late-owner';let hold=gate(name);await open(name);await hold.started;await change('B');hold.release();await empty();await page.waitForFunction(()=>!document.querySelector('#lightbox').classList.contains('show'));assert.equal(counts.get(name),1);await foreign();results.push({width,case:'late-old-owner-response',passed:true});
 await fresh();name=width+'-cancel';hold=gate(name);await open(name);await hold.started;await page.evaluate(()=>CKIssueImages.close());await open(width+'-new');await ready();const shown=await page.locator('#lightboxImg').getAttribute('src');hold.release();
 // Controlled server release followed by a new browser task; old aborted work must not win.
 await page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));assert.equal(await page.locator('#lightboxImg').getAttribute('src'),shown);await foreign();results.push({width,case:'cancel-reopen-new-image',passed:true});
 await fresh();name=width+'-expiry';await page.evaluate(()=>{window.fixtureNow=Date.now();Date.now=()=>fixtureNow;});await open(name);await ready();await page.evaluate(()=>CKIssueImages.close());await page.evaluate(()=>fixtureNow+=300001);await open(name);await ready();assert.equal(counts.get(name),2);
 const pinned=await page.locator('#lightboxImg').getAttribute('src');await page.evaluate(()=>{fixtureNow+=300001;dispatchEvent(new Event('focus'));});assert.equal(await page.evaluate(u=>cache.urls.has(u),pinned),true);await page.evaluate(()=>CKIssueImages.close());await empty();await foreign();results.push({width,case:'expiry-refetch-current-pin',passed:true});
 await fresh();await open(width+'-locked');await ready();await change(null);await page.evaluate(()=>document.documentElement.classList.add('ck-auth-pending'));await empty();await page.waitForFunction(()=>!document.querySelector('#lightbox').classList.contains('show'));await open(width+'-blocked');assert.equal(counts.has(width+'-blocked'),false);await foreign();results.push({width,case:'session-lock-cleans-and-blocks',passed:true});
 await fresh();name=width+'-lifecycle';await open(name);await ready();await page.evaluate(()=>dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true})));await empty();await open(name);assert.equal(counts.get(name),1);await page.evaluate(()=>dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));await open(name);await ready();assert.equal(counts.get(name),2);
 await page.evaluate(()=>dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true})));await empty();await foreign();assert.equal(await page.evaluate(()=>new Set(cache.revoked).size===cache.revoked.length),true);results.push({width,case:'pagehide-pageshow-owned-url-cleanup',passed:true});
 await context.close();console.log('PASS identity/late-response/cancel/expiry/lock/lifecycle '+width);
 }assert.deepEqual(errors,[]);assert.equal(results.length,12);console.log(JSON.stringify({results,errors}));
}finally{for(const h of gates.values())h.release();await browser.close();await new Promise(r=>server.close(r));}
