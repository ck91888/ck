// DOM integration tests, not a substitute for real browser/device visual acceptance.
// Run after build with CK_JSDOM_MODULE pointing at an installed jsdom API module.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const assets=path.resolve('worker-v2/.sop-staging-assets');
async function fixture(){
 const env={DB:database(),SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_AUTO_OUTBOUND:'true',SOP_ROLLOUT_DEPARTMENTS:'bulk',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'测试负责人',role:'manager',key:crypto.randomUUID()}])};
 let cookie='';async function request(body){return worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body:JSON.stringify(body)}),env);}
 const login=await request({action:'sop_login',sop_key:JSON.parse(env.SOP_USERS_JSON)[0].key});cookie=login.headers.get('set-cookie').split(';')[0];
 const seed=await (await request({action:'sop_demo_prepare',client_req_id:'dom-seed'})).json();assert.equal(seed.ok,true,seed.error);
 async function page(url){
  const errors=[];const {JSDOM,requestInterceptor,VirtualConsole}=runtime;const vc=new VirtualConsole();vc.on('jsdomError',e=>{if(!/Not implemented:|Could not parse CSS stylesheet/.test(e.message))errors.push(e.message)});
  const resources={interceptors:[requestInterceptor(req=>{const u=new URL(req.url);if(u.origin!=='https://fixture.test')throw Error('External request rejected: '+u.origin);return new Response(fs.readFileSync(path.join(assets,u.pathname)),{headers:{'Content-Type':u.pathname.endsWith('.js')?'application/javascript':'text/css'}});})]};
  const u=new URL(url,'https://fixture.test');const html=fs.readFileSync(path.join(assets,u.pathname,'index.html'),'utf8');
  const dom=new JSDOM(html,{url:u.href,runScripts:'dangerously',resources,virtualConsole:vc,pretendToBeVisual:true,beforeParse(w){
   w.fetch=async(url,options={})=>{const endpoint=new URL(url,w.location.href);assert.equal(endpoint.origin,'https://fixture.test');assert.equal(endpoint.pathname,'/api');return request(JSON.parse(options.body));};
   w.alert=message=>errors.push('alert: '+message);w.confirm=()=>true;w.prompt=()=>{throw Error('Unexpected login/name prompt')};w.scrollTo=()=>{};
   w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
  }});
  await until(()=>dom.window.CKSession?.user,errors);await until(()=>dom.window.document.querySelector('.ck-cross-nav'),errors);
  await new Promise(resolve=>setTimeout(resolve,40));assert.ok(!dom.window.document.body.textContent.includes('页面初始化失败'),dom.window.document.body.textContent.slice(-300));
  return {dom,w:dom.window,d:dom.window.document,errors};
 }
 return {page,seed,request};
}
async function until(condition,errors=[]){for(let i=0;i<100;i++){if(condition())return;await new Promise(r=>setTimeout(r,10));}throw Error('DOM condition timeout: '+errors.join('; '));}
const opts={skip:!runtime};
test('original four modules initialize under shared session without old login prompts',opts,async()=>{
 const f=await fixture();for(const app of ['/','/001/','/002/','/003/','/shuju/']){const p=await f.page(app);try{
 assert.equal(p.d.querySelector('#ck-auth-gate'),null);assert.deepEqual(p.errors,[],app);
 if(app==='/002/')assert.equal(p.d.querySelector('#page-main').classList.contains('active'),true);
 if(app==='/001/')assert.equal(p.d.querySelector('#page-home').classList.contains('active'),true);
 }finally{p.w.close();}}
});
test('original collaboration buttons open integrated needs and verification editor',opts,async()=>{
 const f=await fixture(),p=await f.page('/002/');try{
 p.d.querySelector('[data-tab=need]').click();await until(()=>p.d.querySelector('#view-need article'),p.errors);assert.match(p.d.querySelector('#view-need').textContent,/虚拟|验收/);
 await p.w.openInboundDetail(f.seed.inbound_id);await until(()=>p.d.querySelector('#inboundDetailBody .ck-inline-heading button'),p.errors);
 p.d.querySelector('#inboundDetailBody .ck-inline-heading button').click();await until(()=>p.d.querySelector('#view-need #actions a'),p.errors);
 p.d.querySelector('#btnNewCheck').click();await until(()=>p.d.querySelector('#checkListBody dialog')?.open,p.errors);
 assert.ok(p.d.querySelector('#checkListBody input[name=ship_date]'));assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('original field and dashboard integrate added controls',opts,async()=>{
 const f=await fixture(),p=await f.page('/001/');try{
 p.d.querySelector('.home-grid .home-btn.accent').click();await until(()=>p.d.querySelector('#ck-dispatch-body #content .toolbar button'),p.errors);
 p.d.querySelector('#ck-dispatch-body #content .toolbar button').click();await until(()=>p.d.querySelector('#ck-dispatch-body dialog').open,p.errors);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
 const q=await f.page('/shuju/');try{Array.from(q.d.querySelectorAll('.tab-bar button')).find(x=>x.textContent==='派工质量与待办').click();await until(()=>q.d.querySelector('#ck-dashboard article'),q.errors);assert.deepEqual(q.errors,[]);}finally{q.w.close();}
});
