// Deterministic DOM checks for the field-home reader. All records are synthetic.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const opts={skip:!runtime},source=readFileSync(new URL('../shared/dispatch-home-ui.js',import.meta.url),'utf8');
const settle=async()=>{for(let n=0;n<8;n++)await Promise.resolve();await new Promise(r=>setImmediate(r));};
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
function clock(w){let now=Date.parse('2026-10-12T01:00:00Z'),id=0;const timers=new Map();w.Date.now=()=>now;w.setInterval=(fn,delay)=>{timers.set(++id,{fn,delay,next:now+delay});return id;};w.clearInterval=id=>timers.delete(id);return {get count(){return timers.size;},async advance(ms){const end=now+ms;for(;;){let next;for(const t of timers.values())if(t.next<=end&&(!next||t.next<next.next))next=t;if(!next)break;now=next.next;next.next+=next.delay;next.fn();await settle();}now=end;await settle();}};}
async function fixture(t){
 const errors=[],vc=new runtime.VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
 const dom=new runtime.JSDOM('<body><main id="page-home" class="page active"><section><button data-dispatch-refresh>Refresh</button><p data-dispatch-freshness></p><div data-dispatch-tasks>Loading</div></section></main><article id="work"><input value="Unsaved synthetic draft"></article></body>',{runScripts:'outside-only',pretendToBeVisual:true,url:'https://fixture.invalid/001/',virtualConsole:vc});
 const w=dom.window,d=w.document,page=d.querySelector('main'),root=page.querySelector('section'),time=clock(w),calls=[];
 let hidden=false,data=[{id:'SYNTHETIC-JOB',name:'Synthetic original worker'}],handler=null;
 Object.defineProperty(d,'hidden',{get:()=>hidden});w.scrollTo=()=>{};w.eval(source);
 const read=async()=>{calls.push(time);return handler?handler():{items:structuredClone(data)};};
 const render=job=>{const b=d.createElement('button');b.textContent=job.id+' '+job.name;return b;};
 const mount=()=>w.CKDispatchHome(root,{page,read,render});let controller=mount();await controller.enter();await settle();
 t.after(()=>{root.__ckDispatchHome?.destroy();w.close();});
 return {w,d,page,root,time,calls,errors,get list(){return root.querySelector('[data-dispatch-tasks]');},get status(){return root.querySelector('[data-dispatch-freshness]');},get button(){return root.querySelector('[data-dispatch-refresh]');},get controller(){return controller;},set data(x){data=x;},set handler(x){handler=x;},async leave(){controller.leave();page.classList.remove('active');await settle();},async enter(){page.classList.add('active');const p=controller.enter();await settle();return p;},async hidden(value){hidden=value;d.dispatchEvent(new w.Event('visibilitychange'));await settle();},async remount(){controller=mount();await controller.enter();await settle();}};
}
test('visible home polls once per 15 seconds and replaces only changed task cards',opts,async t=>{
 const f=await fixture(t),original=f.list.firstElementChild;assert.equal(f.calls.length,1);assert.equal(f.status.dataset.freshness,'current');
 await f.time.advance(15000);assert.equal(f.calls.length,2);assert.equal(f.list.firstElementChild,original);
 f.data=[{id:'SYNTHETIC-JOB',name:'Synthetic changed worker'}];await f.time.advance(15000);assert.equal(f.calls.length,3);assert.match(f.list.textContent,/changed worker/);assert.notEqual(f.list.firstElementChild,original);assert.deepEqual(f.errors,[]);
});
test('focus and visibility return read current data, hidden pages never poll',opts,async t=>{
 const f=await fixture(t);await f.hidden(true);await f.time.advance(45000);assert.equal(f.calls.length,1);
 f.data=[];await f.hidden(false);assert.equal(f.calls.length,2);assert.match(f.list.textContent,/暂无现场在途任务/);
 f.w.dispatchEvent(new f.w.Event('focus'));await settle();assert.equal(f.calls.length,3);assert.deepEqual(f.errors,[]);
});
test('leaving home stops reads and preserves unrelated task draft and active form',opts,async t=>{
 const f=await fixture(t),work=f.d.querySelector('#work'),input=work.querySelector('input');input.value='Kept unsaved draft';input.focus();await f.leave();
 await f.time.advance(45000);f.w.dispatchEvent(new f.w.Event('focus'));await settle();assert.equal(f.calls.length,1);assert.equal(input.value,'Kept unsaved draft');assert.equal(f.d.activeElement,input);
 await f.enter();assert.equal(f.calls.length,2);assert.equal(input.value,'Kept unsaved draft');assert.equal(f.d.querySelector('#work'),work);
});
test('late response from a prior home visit is discarded before one fresh queued read',opts,async t=>{
 const f=await fixture(t),late=deferred();let first=true;f.handler=()=>{if(first){first=false;return late.promise;}return {items:[{id:'NEW-JOB',name:'Latest worker'}]};};
 const old=f.controller.refresh();await settle();assert.equal(f.calls.length,2);await f.leave();f.page.classList.add('active');const newer=f.controller.enter();await settle();
 await f.time.advance(45000);f.w.dispatchEvent(new f.w.Event('focus'));await settle();assert.equal(f.calls.length,2,'poll and return share the one pending request');
 late.resolve({items:[{id:'OLD-JOB',name:'Obsolete worker'}]});await old;await newer;await settle();assert.equal(f.calls.length,3);assert.match(f.list.textContent,/NEW-JOB/);assert.doesNotMatch(f.list.textContent,/OLD-JOB/);assert.equal(f.status.dataset.freshness,'current');
});
test('failed refresh keeps existing cards, timestamp and explicit stale state until success',opts,async t=>{
 const f=await fixture(t),before=f.list.firstElementChild,stamp=f.status.textContent;f.handler=()=>Promise.reject(Error('Synthetic offline'));await f.time.advance(15000);
 assert.equal(f.list.firstElementChild,before);assert.ok(f.status.textContent.startsWith(stamp));assert.match(f.status.textContent,/刷新失败.*保留上次数据.*过时/);assert.equal(f.status.dataset.freshness,'failed');assert.equal(f.button.disabled,false);
 f.handler=null;f.data=[];f.button.click();await settle();assert.match(f.list.textContent,/暂无现场在途任务/);assert.equal(f.status.dataset.freshness,'current');
});
test('slow and failed reads never overlap or immediately retry in a loop',opts,async t=>{
 const f=await fixture(t),late=deferred();f.handler=()=>late.promise;await f.time.advance(15000);await f.time.advance(60000);f.w.dispatchEvent(new f.w.Event('focus'));await settle();
 assert.equal(f.calls.length,2);assert.equal(f.status.dataset.freshness,'stale');late.reject(Error('Synthetic slow failure'));await settle();assert.equal(f.calls.length,2);assert.equal(f.status.dataset.freshness,'failed');f.handler=null;await f.time.advance(15000);assert.equal(f.calls.length,3);
});
test('open dialog pauses reading and rejects a pending redraw without losing user input',opts,async t=>{
 const f=await fixture(t),late=deferred();f.handler=()=>late.promise;const reading=f.controller.refresh();await settle();
 const dialog=f.d.createElement('dialog');dialog.setAttribute('open','');dialog.innerHTML='<input value="Unsaved reviewer reason">';f.d.body.append(dialog);await settle();await f.time.advance(45000);assert.equal(f.calls.length,2);
 late.resolve({items:[{id:'OLD-JOB',name:'Hidden response'}]});await reading;await settle();assert.doesNotMatch(f.list.textContent,/OLD-JOB/);assert.equal(dialog.querySelector('input').value,'Unsaved reviewer reason');
 f.handler=null;f.data=[];dialog.removeAttribute('open');await settle();assert.equal(f.calls.length,3);assert.match(f.list.textContent,/暂无现场在途任务/);
});
test('refresh keeps task-button keyboard focus and unrelated controls stable',opts,async t=>{
 const f=await fixture(t);f.list.firstElementChild.focus();f.data=[{id:'SYNTHETIC-JOB',name:'Synthetic replacement crew'}];await f.time.advance(15000);assert.equal(f.d.activeElement.dataset.dispatchJob,'SYNTHETIC-JOB');
 f.data=[];await f.time.advance(15000);assert.equal(f.d.activeElement,f.button);assert.deepEqual(f.errors,[]);
});
test('bfcache suspension, remount and destroy never multiply the timer',opts,async t=>{
 const f=await fixture(t);assert.equal(f.time.count,1);await f.remount();assert.equal(f.time.count,1);const count=f.calls.length;
 f.w.dispatchEvent(new f.w.PageTransitionEvent('pagehide',{persisted:true}));assert.equal(f.time.count,0);await f.time.advance(45000);assert.equal(f.calls.length,count);
 f.w.dispatchEvent(new f.w.PageTransitionEvent('pageshow',{persisted:true}));await settle();assert.equal(f.time.count,1);assert.equal(f.calls.length,count+1);
 f.controller.destroy();assert.equal(f.time.count,0);await f.time.advance(45000);f.w.dispatchEvent(new f.w.Event('focus'));await settle();assert.equal(f.calls.length,count+1);
});
