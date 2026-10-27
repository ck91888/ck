import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{JSDOM}=require(process.env.CK_JSDOM_MODULE||require.resolve('jsdom',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const source=fs.readFileSync(new URL('../002/app.js',import.meta.url),'utf8');
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
const pause=()=>new Promise(r=>setImmediate(r));
const answer=customer=>({ok:true,items:[{id:customer,customer,status:'pending'}]});
function fixture(t,fetch,deadline=25){
 const ids=['obFilterStart','obFilterEnd','obFilterStatus','outboundFilterAccounted','obFilterBizClass','obFilterUsesStockOp','obFilterHasMaterial','obFilterCustomer'];
 const dom=new JSDOM('<div id="mainTabs"></div><div id="outboundListBody"></div><button id="obQueryBtn"></button>'+ids.map(id=>'<input id="'+id+'">').join(''));t.after(()=>dom.window.close());
 const intervals=new Set(),timeouts=new Set(),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const context={document:dom.window.document,AbortController,fetch,V2_API:'/api',getKey:()=>'',_WRITE_ACTIONS:[],_outboundListController:null,_currentView:'outbound',_currentTab:'outbound',getPager:()=>({limit:50}),getOffset:()=>0,renderPager:()=>'',L:k=>k,esc,stLabel:s=>s,accountTag:()=>'',fmtTime:s=>s||'',getLang:()=>'zh',remarkPreview:s=>s||'',goView:()=>{},loadDashboard:()=>{},setInterval:(f,ms)=>{const id=setInterval(f,ms);intervals.add(id);return id;},clearInterval:id=>{clearInterval(id);intervals.delete(id);},setTimeout:(f,ms)=>{const id=setTimeout(()=>{timeouts.delete(id);f();},ms===60000?deadline:ms);timeouts.add(id);return id;},clearTimeout:id=>{clearTimeout(id);timeouts.delete(id);}};
 vm.createContext(context);
 vm.runInContext(source.slice(source.indexOf('async function api(params, options)'),source.indexOf('async function uploadFile'))+source.slice(source.indexOf('async function loadOutboundList()'),source.indexOf('// ===== Outbound Export ====='))+source.slice(source.indexOf('function goTab(tab, btn)'),source.indexOf('// 按当前 view')),context);
 t.after(()=>{for(const id of intervals)clearInterval(id);for(const id of timeouts)clearTimeout(id);});
 return {context,body:dom.window.document.querySelector('#outboundListBody'),button:dom.window.document.querySelector('#obQueryBtn'),intervals,timeouts};
}
test('partial HTTP200 body times out, enables retry and never displays empty data; late completion cannot replace recovered rows',async t=>{
 const body=deferred();let calls=0;const f=fixture(t,async()=>({status:200,json:()=>++calls===1?body.promise:Promise.resolve(answer('RECOVERED'))}));
 await f.context.loadOutboundList();assert.match(f.body.textContent,/加载超时.*로딩 시간 초과/);assert.doesNotMatch(f.body.textContent,/no_data/);assert.equal(f.button.disabled,false);assert.equal(calls,1);
 const retry=f.body.querySelector('[data-outbound-retry]');assert.ok(retry);retry.click();await pause();assert.match(f.body.textContent,/RECOVERED/);
 body.resolve(answer('LATE-OLD'));await pause();assert.doesNotMatch(f.body.textContent,/LATE-OLD/);assert.equal(calls,2);assert.equal(f.intervals.size,0);assert.equal(f.timeouts.size,0);
});
test('a stalled request before headers also times out and preserves filters for manual retry',async t=>{
 const headers=deferred(),f=fixture(t,()=>headers.promise);f.context.document.querySelector('#obFilterCustomer').value='SYNTHETIC-FILTER';
 await f.context.loadOutboundList();assert.match(f.body.textContent,/加载失败/);assert.equal(f.context.document.querySelector('#obFilterCustomer').value,'SYNTHETIC-FILTER');assert.equal(f.button.disabled,false);headers.resolve({status:200,json:async()=>answer('OLD')});await pause();assert.doesNotMatch(f.body.textContent,/OLD/);
});
test('a newer filter request cancels the old read promptly and only the new request controls rows and button state',async t=>{
 const a=deferred(),b=deferred();let count=0;const f=fixture(t,async()=>({status:200,json:()=>++count===1?a.promise:b.promise}),60000);
 const first=f.context.loadOutboundList();await pause();const second=f.context.loadOutboundList();await first;assert.equal(f.button.disabled,true);
 b.resolve(answer('LATEST-FILTER'));await second;assert.equal(f.button.disabled,false);a.resolve(answer('STALE-FILTER'));await pause();assert.match(f.body.textContent,/LATEST-FILTER/);assert.doesNotMatch(f.body.textContent,/STALE-FILTER/);assert.equal(f.intervals.size,0);
});
test('leaving the outbound tab cancels waiting reads without showing a timeout or late response',async t=>{
 const body=deferred(),f=fixture(t,async()=>({status:200,json:()=>body.promise}),60000);const pending=f.context.loadOutboundList();await pause();f.context.goTab('home');await pending;assert.equal(f.button.disabled,false);assert.doesNotMatch(f.body.textContent,/加载失败/);body.resolve(answer('AFTER-LEAVING'));await pause();assert.doesNotMatch(f.body.textContent,/AFTER-LEAVING/);assert.equal(f.timeouts.size,0);
});
