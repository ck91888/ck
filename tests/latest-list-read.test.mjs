import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
function fixture(){
 const hosts={},window={document:{getElementById:id=>hosts[id]},CKSession:{scope:'office',user:{id:'SYN-A',scope:'office',role:'manager',operation_context:{id:'SYN-CONTEXT',name:'合成操作员'}}}};let key='synthetic';
 vm.runInNewContext(fs.readFileSync(new URL('../shared/latest-list-read.js',import.meta.url),'utf8'),{window});
 const host=id=>(hosts[id]={id,isConnected:true}),begin=(scope='list',node=host('list'))=>window.CKLatestListRead.begin(scope,node,()=>key);
 return {window,host,begin,setKey:x=>key=x};
}
test('only the latest read owns its DOM and cached result, including A to B to A',()=>{
 const f=fixture(),node=f.host('list'),a=f.begin('list',node),b=f.begin('list',node),again=f.begin('list',node);assert.equal(a.current(),false);assert.equal(b.commit(),false);assert.equal(again.canUse(),false);assert.equal(again.commit(),true);assert.equal(again.canUse(),true);
});
test('independent lists do not cancel each other and replaced hosts cannot be written',()=>{
 const f=fixture(),a=f.begin('A',f.host('A')),b=f.begin('B',f.host('B'));assert.equal(a.commit(),true);assert.equal(b.current(),true);f.host('A');assert.equal(a.canUse(),false);assert.equal(b.current(),true);
});
test('shared user replacement, logout, role/context changes and same-id relogin invalidate reads and export caches',()=>{
 for(const change of [f=>f.window.CKSession.user=null,f=>f.window.CKSession.user={...f.window.CKSession.user},f=>f.window.CKSession.user.role='viewer',f=>f.window.CKSession.user.operation_context.id='SYN-OTHER',f=>f.window.CKSession={...f.window.CKSession}]){
  const f=fixture(),a=f.begin();a.commit();change(f);assert.equal(a.current(),false);assert.equal(a.canUse(),false);
 }
});
test('pending authentication cannot commit and standalone legacy identity changes invalidate reads',()=>{
 const f=fixture();f.window.CKSession.user=null;assert.equal(f.begin().commit(),false);delete f.window.CKSession;const a=f.begin();assert.equal(a.commit(),true);f.setKey('synthetic-new');assert.equal(a.canUse(),false);
});
test('raw apps include the helper before app code and staging ships its hashed asset',()=>{
 for(const app of ['002','003','shuju']){const s=fs.readFileSync(new URL('../'+app+'/index.html',import.meta.url),'utf8');assert.ok(s.indexOf('latest-list-read.js')<s.indexOf('src="app.js'));assert.ok(s.includes('latest-list-read.js'));}
 assert.match(fs.readFileSync(new URL('../worker-v2/build-sop-staging.mjs',import.meta.url),'utf8'),/'latest-list-read.js'/);
});
