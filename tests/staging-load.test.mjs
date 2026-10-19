import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import entry from '../worker-v2/staging-entry.js';
import {database} from './d1-adapter.mjs';
const root=path.resolve('worker-v2/.sop-staging-assets');

test('built shared CSS preserves every rule in order and every URL is fingerprinted',()=>{
 const source=fs.readFileSync('worker-v2/build-sop-staging.mjs','utf8');
 const original=source.match(/const originalHead='([^\n]+)';/)[1];
 const css=[...original.matchAll(/<link rel="stylesheet" href="([^\"]+)">/g)].map(m=>fs.readFileSync('.'+m[1],'utf8'));
 assert.equal(fs.readFileSync(path.join(root,'shared/staging-common.css'),'utf8'),css.join('\n'));
 for(const page of ['index.html','office-login/index.html',...['001','002','003','shuju','attendance'].map(a=>a+'/index.html')]){
  const html=fs.readFileSync(path.join(root,page),'utf8');
  assert.equal((html.match(/staging-common\.css/g)||[]).length,1,page);
  for(const [,url,hash] of html.matchAll(/(?:src|href)="([^\"]+\.(?:js|css))\?v=([a-f0-9]{12})"/g)){
   const file=path.join(root,new URL(url,'https://fixture.invalid/'+page).pathname);
   assert.equal(createHash('sha256').update(fs.readFileSync(file)).digest('hex').slice(0,12),hash,page+': '+url);
  }
 }
 const login=fs.readFileSync(path.join(root,'office-login/index.html'),'utf8');
 assert.deepEqual([...login.matchAll(/<script src="\/shared\/([^?]+)\?/g)].map(m=>m[1]),['browser-compat.js','sop-rollout.js','sop-session.js']);
 const collaboration=fs.readFileSync(path.join(root,'002/index.html'),'utf8');
 assert.doesNotMatch(collaboration,/<script src="[^\"]*xlsx\.full/);
 assert.match(collaboration,/data-excel-src="\/shared\/xlsx\.full\.min\.js\?v=[a-f0-9]{12}"/);
 for(const app of ['001','003','shuju','attendance'])assert.match(fs.readFileSync(path.join(root,app,'index.html'),'utf8'),/<script src="[^\"]*xlsx\.full/,'other applications keep their original library contract');
});

test('staging timing preserves auth, cookies and bodies without caching private responses',async t=>{
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:'Synthetic-timing-only',ASSETS:{async fetch(){return new Response('synthetic asset');}}};
 const call=body=>entry.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}),env);
 const denied=await call({action:'sop_identity'});assert.equal(denied.status,401);assert.equal((await denied.json()).ok,false);
 const login=await call({action:'sop_login',sop_key:'Synthetic-timing-only'});assert.equal(login.status,200);assert.equal((await login.json()).ok,true);assert.ok(login.headers.get('set-cookie'));
 for(const r of [denied,login]){assert.match(r.headers.get('Server-Timing'),/^app;dur=\d+\.\d$/);assert.equal(r.headers.get('Cache-Control'),'private, no-store');}
 const page=await entry.fetch(new Request('https://fixture.invalid/002/'),env);assert.equal(page.status,302);assert.equal(page.headers.get('Server-Timing'),null);
 const asset=await entry.fetch(new Request('https://fixture.invalid/shared/staging-common.css?v=0123456789ab'),env);assert.equal(await asset.text(),'synthetic asset');assert.match(asset.headers.get('Cache-Control'),/public.*immutable/);assert.equal(asset.headers.get('Server-Timing'),null);
 const options=await entry.fetch(new Request('https://fixture.invalid/api',{method:'OPTIONS'}),env);assert.equal(await options.text(),'');assert.match(options.headers.get('Access-Control-Allow-Methods'),/POST/);
});
