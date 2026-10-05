import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import entry from '../worker-v2/staging-entry.js';
import {database} from './d1-adapter.mjs';
import {digest,accessSessionAction} from '../worker-v2/access-control.js';
import {STAGING_DATABASE} from '../worker-v2/test-data-reset.js';
const hosts=['https://sop-test.ck91888.cn','https://ck-v2-api-sop-staging.ck91888.workers.dev'];
const password='isolated-fixture-password-only',oldCode='isolated-fixture-old-code',jsonCode='isolated-fixture-json-code';
async function setup(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest(oldCode),SOP_USERS_JSON:JSON.stringify([{id:'fixture-json-admin',name:'Fixture manager',role:'manager',departments:['bulk'],key:jsonCode}]),ASSETS:{async fetch(){return new Response('fixture asset');}}};
 const cookies={office:'',kiosk:'',field:''},paths={office:'/api',kiosk:'/attendance/api',field:'/001/api'};
 async function request(scope,action,data={},options={}){
  const origin=options.origin||hosts[0],headers={'Content-Type':'application/json',Origin:origin,Cookie:options.cookie??cookies[scope],'CF-Connecting-IP':'192.0.2.9'};
  const response=await entry.fetch(new Request(origin+paths[scope],{method:'POST',headers,body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
  const body=await response.json();if(action==='sop_login'&&response.ok)cookies[scope]=response.headers.get('Set-Cookie')?.split(';')[0]||'';
  return {response,body};
 }
 const login=(scope,code,options)=>request(scope,'sop_login',{sop_key:code},options);
 await login('office',oldCode);await request('office','sop_attendance_config');
 return {DB,env,cookies,request,login};
}
test('unconfigured password retains hash recovery and JSON manager login; malformed JSON safely leaves hash recovery usable',async()=>{
 const f=await setup();
 for(const scope of ['office','kiosk'])for(const code of [oldCode,jsonCode])assert.equal((await f.login(scope,code)).response.status,200);
 f.env.SOP_USERS_JSON='invalid fixture JSON';
 for(const scope of ['office','kiosk']){
  assert.equal((await f.login(scope,oldCode)).response.status,200);
  assert.equal((await f.request(scope,'sop_identity')).response.status,200);
  assert.equal((await f.login(scope,jsonCode)).response.status,403);
 }
});
test('configured password works for office and kiosk on both exact staging domains without relying on JSON',async()=>{
 const f=await setup();f.env.SOP_ADMIN_PASSWORD=password;f.env.SOP_USERS_JSON='invalid fixture JSON';
 for(const origin of hosts)for(const scope of ['office','kiosk']){
  const {response,body}=await f.login(scope,password,{origin});assert.equal(response.status,200);
  assert.equal(body.user.id,'ck-office-admin');assert.equal(body.user.scope,scope);assert.equal(body.user.role,scope==='office'?'manager':'kiosk');
  assert.equal((await f.request(scope,'sop_identity',{}, {origin})).response.status,200);
 }
 const list=await f.request('office','sop_access_list');assert.equal(list.response.status,200);assert.equal(list.body.ok,true);
 const need=await f.request('office','sop_need_create',{department:'bulk',title:'虚拟密码路径验证',customer:'虚拟客户',instructions:'隔离测试',owner:'管理员'});assert.equal(need.body.ok,true,need.body.error);
 assert.equal((await f.request('kiosk','sop_access_list')).response.status,403,'kiosk must not acquire office privileges');
});
test('password administrator can read maintenance preview on both test domains while a previous code session is denied',async()=>{
 const f=await setup(),old=f.cookies.office;Object.assign(f.env,{SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE,SOP_ADMIN_PASSWORD:password});
 await f.login('office',password);
 for(const origin of hosts){
  const get=cookie=>entry.fetch(new Request(origin+'/api/test-data/status',{headers:{Cookie:cookie}}),f.env);
  assert.equal((await get(old)).status,403);
  const r=await get(f.cookies.office);assert.equal(r.status,200);assert.equal((await r.json()).ok,true);
 }
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_test_resets').get().n,0,'preview must not create a reset');
});
test('new password exclusively replaces both old administrator code sources, rejecting wrong password and forged old-key bodies',async()=>{
 const f=await setup();f.env.SOP_ADMIN_PASSWORD=password;
 for(const scope of ['office','kiosk'])for(const code of [oldCode,jsonCode,password+'wrong',password.toUpperCase()]){
  const {response,body}=await f.login(scope,code,{cookie:''});assert.equal(response.status,403);assert.equal(body.ok,false);
 }
 assert.equal((await f.request('office','sop_access_list',{sop_key:jsonCode,role:'manager'},{cookie:''})).response.status,401);
 assert.equal((await f.login('office',password)).response.status,200);
});
test('enabling password invalidates all hash and JSON office/kiosk sessions while preserving personnel/history',async()=>{
 const f=await setup(),sessions=[];
 const person=(await f.request('office','sop_attendance_employee_register',{name:'虚拟保留职员',employeeNo:'PASSWORD-FIXTURE',department:'bulk'})).body.person;
 await f.request('office','sop_attendance_checkin',{badge:person.badgeId});
 const counts=()=>({people:f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,days:f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n});
 const before=counts();
 for(const scope of ['office','kiosk'])for(const code of [oldCode,jsonCode]){await f.login(scope,code);sessions.push({scope,cookie:f.cookies[scope]});}
 f.env.SOP_ADMIN_PASSWORD=password;
 for(const {scope,cookie} of sessions)assert.equal((await f.request(scope,'sop_identity',{}, {cookie})).response.status,401);
 assert.deepEqual(counts(),before);
 for(const scope of ['office','kiosk']){assert.equal((await f.login(scope,password)).response.status,200);assert.equal((await f.request(scope,'sop_identity')).response.status,200);}
});
test('password rotation invalidates existing new-password sessions and accepts the replacement without changing roles',async()=>{
 const f=await setup();f.env.SOP_ADMIN_PASSWORD=password;const old=[];
 for(const scope of ['office','kiosk']){await f.login(scope,password);old.push({scope,cookie:f.cookies[scope]});}
 f.env.SOP_ADMIN_PASSWORD=password+'-rotated';
 for(const {scope,cookie} of old){assert.equal((await f.request(scope,'sop_identity',{}, {cookie})).response.status,401);assert.equal((await f.login(scope,password)).response.status,403);assert.equal((await f.login(scope,f.env.SOP_ADMIN_PASSWORD)).response.status,200);}
});
test('configured blank, whitespace, short, overlong or invalid password closes both administrator entrances without legacy fallback',async()=>{
 for(const value of ['', ' '.repeat(15),'short',password+' ', ' '+password,'a'.repeat(129),'😀'.repeat(6),password+'\n',null,undefined,123]){
  const f=await setup();const old=f.cookies.office;f.env.SOP_ADMIN_PASSWORD=value;
  assert.equal((await f.request('office','sop_identity',{}, {cookie:old})).response.status,401);
  for(const scope of ['office','kiosk'])for(const code of [oldCode,jsonCode,password])assert.equal((await f.login(scope,code)).response.status,403);
 }
});
test('password accepts 12–128 Unicode characters exactly and rejects case changes or surrounding whitespace',async()=>{
 for(const value of ['a'.repeat(12),'z'.repeat(128),'😀'.repeat(12),'密'.repeat(128)]){
  const f=await setup();f.env.SOP_ADMIN_PASSWORD=value;assert.equal((await f.login('office',value)).response.status,200);
  for(const wrong of [' '+value,value+' ',value.slice(1)])assert.equal((await f.login('office',wrong)).response.status,403);
 }
});
test('existing employee field session, badge login and attendance remain valid after administrator password enablement or invalid configuration',async()=>{
 const f=await setup();const person=(await f.request('office','sop_attendance_employee_register',{name:'虚拟现场职员',employeeNo:'FIELD-PASSWORD-FIXTURE',department:'bulk'})).body.person;
 await f.request('office','sop_attendance_checkin',{badge:person.badgeId});
 assert.equal((await f.request('field','sop_login',{badge:person.badgeId})).response.status,200);
 for(const value of [password,'']){
  f.env.SOP_ADMIN_PASSWORD=value;assert.equal((await f.request('field','sop_identity')).response.status,200);
  assert.equal((await f.request('field','sop_login',{badge:person.badgeId})).response.status,200);
  assert.equal((await f.request('field','sop_access_list')).response.status,403);
 }
});
test('password login retains the existing per-IP/scope attempt cap including correct-password attempts after blocking',async()=>{
 const f=await setup();f.env.SOP_ADMIN_PASSWORD=password;
 for(let i=0;i<30;i++)assert.equal((await f.login('office',password+'wrong')).response.status,403);
 assert.equal((await f.login('office',password)).response.status,429);
 assert.equal((await f.login('kiosk',password)).response.status,200,'separate entrance keeps its existing attempt bucket');
});
test('administrator credentials and verifiers do not enter responses or stored plaintext session records',async()=>{
 const f=await setup();f.env.SOP_ADMIN_PASSWORD=password;
 const login=await f.login('office',password),identity=await f.request('office','sop_identity');
 const row=f.DB.raw.prepare("SELECT * FROM ck_access_sessions WHERE scope='office'").get();
 assert.match(row.credential_hash,/^password-v1:[a-f0-9]{64}$/);assert.notEqual(row.credential_hash,await digest(password));
 for(const value of [password,await digest(password),row.credential_hash]){
  assert.equal(JSON.stringify([login.body,identity.body]).includes(value),false);
  assert.equal([...login.response.headers].some(([,s])=>s.includes(value)),false);
 }
 assert.equal(JSON.stringify(row).includes(password),false);
});
test('password binding is staging-access-control only and cannot enable a new production credential path',async()=>{
 const f=await setup();f.env.SOP_ADMIN_PASSWORD=password;f.env.SOP_ENVIRONMENT='production';
 const req=new Request('https://production.fixture/api',{method:'POST'});
 const r=await accessSessionAction({action:'sop_login',sop_key:password},f.env,req);assert.equal(r.status,403);
 // This internal legacy helper remains unchanged; normal production requests do
 // not call accessSessionAction because accessEnabled is false.
 assert.equal((await accessSessionAction({action:'sop_login',sop_key:oldCode},f.env,req)).status,200);
});
test('deployment never declares the password in plaintext staging or production configuration',()=>{
 const config=fs.readFileSync(new URL('../worker-v2/wrangler.sop-staging.toml',import.meta.url),'utf8');
 assert.equal(/^\s*SOP_ADMIN_PASSWORD\s*=/m.test(config),false);
 const production=fs.readFileSync(new URL('../worker-v2/wrangler.toml',import.meta.url),'utf8');
 assert.equal(/^\s*SOP_ADMIN_PASSWORD\s*=/m.test(production),false);
});
