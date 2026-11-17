import test from 'node:test';
import assert from 'node:assert/strict';
import {cacheFixture} from './issue-preview-cache-fixture.mjs';

const user = id => ({id,role:'manager',departments:['bulk'],operation_context:{id:'operator-'+id,name:'Synthetic operator'}});
const session = (f,id='A') => f.w.CKSession={scope:'office',user:user(id)};
const ready = f => f.until(()=>f.w.document.querySelector('[data-view-status]')?.textContent.includes('已加载'));

test('a locked office session clears displayed photos and cannot reopen cached bytes',async t=>{
 const f=await cacheFixture(t);session(f);await f.add();await f.open(0);
 f.w.CKSession.user=null;f.w.document.documentElement.classList.add('ck-auth-pending');
 await f.tick();await f.tick();assert.equal(f.urls.size,0);assert.equal(f.w.document.querySelector('#lightbox').classList.contains('show'),false);
 f.w.CKIssueImages.open('/file?id=0');await f.tick();assert.equal(f.calls.length,1);assert.equal(f.urls.size,0);
});
test('a different user must fetch the same URL again and cannot see a cached predecessor photo',async t=>{
 const f=await cacheFixture(t);session(f);await f.add();await f.open(0);await f.close();
 f.w.CKSession.user=user('B');f.setHandler(()=>new Response('Synthetic denied',{status:403}));
 f.w.CKIssueImages.open('/file?id=0');await f.until(()=>!f.w.document.querySelector('[data-view-retry]').hidden);
 assert.equal(f.calls.length,2);assert.equal(f.urls.size,0);assert.match(f.w.document.querySelector('[data-view-status]').textContent,/403/);
});
test('an old response after an account change creates no object URL or new-user preview',async t=>{
 const f=await cacheFixture(t);session(f);await f.add();let finish;
 f.setHandler(()=>new Promise(r=>finish=()=>r(new Response(new Uint8Array([137,80,78,71,0,0,0,0])))));
 f.w.CKIssueImages.open('/file?id=0');await f.until(()=>finish);f.w.CKSession.user=user('B');finish();await f.tick();await f.tick();
 assert.equal(f.urls.size,0);assert.equal(f.w.document.querySelector('#lightbox').classList.contains('show'),false);
});
test('in-place permission and operator changes invalidate cache ownership',async t=>{
 for(const change of [u=>u.departments=[],u=>u.operation_context.id='other-operator',u=>u.role='viewer',u=>u.scope='field']){
  const f=await cacheFixture(t);session(f);await f.add();await f.open(0);await f.close();change(f.w.CKSession.user);
  f.w.CKIssueImages.open('/file?id=0');await ready(f);assert.equal(f.calls.length,2);assert.equal(f.revoked.length,1);
 }
});
test('replacing the session object invalidates an otherwise identical identity',async t=>{
 const f=await cacheFixture(t);session(f);await f.add();await f.open(0);await f.close();session(f);
 f.w.CKIssueImages.open('/file?id=0');await ready(f);assert.equal(f.calls.length,2);assert.equal(f.revoked.length,1);
});
test('expired previews revalidate on reopen but a currently displayed image stays pinned until close',async t=>{
 const f=await cacheFixture(t);session(f);let now=1000000;f.w.Date.now=()=>now;await f.add();await f.open(0);await f.close();
 now+=300001;f.w.CKIssueImages.open('/file?id=0');await ready(f);assert.equal(f.calls.length,2);assert.equal(f.revoked.length,1);
 const displayed=f.w.document.querySelector('#lightboxImg').src;now+=300001;f.w.dispatchEvent(new f.w.Event('focus'));await f.tick();
 assert.ok(f.urls.has(displayed));await f.close();assert.equal(f.urls.has(displayed),false);
});
