import test from 'node:test';
import assert from 'node:assert/strict';
import {restrictionFixture} from './test-reset-restriction-fixture.mjs';
const compatUser={id:'synthetic-compat-manager',name:'合成兼容管理员',role:'manager',departments:['bulk'],key:'synthetic-compat-key'};
const authTables=['ck_access_sessions','ck_access_attempts','ck_office_operation_context'];
function compatibility(f){Object.assign(f.env,{SOP_ACCESS_CONTROL:'false',SOP_PUBLIC_TEST_ACCESS:'false',SOP_USERS_JSON:JSON.stringify([compatUser])});}
function loginCookie(response){const value=response.headers.getSetCookie().find(v=>/^ck_(?:office_access|sop_session)=/.test(v));assert.ok(value,'successful login must issue the original scoped cookie');assert.match(value,/HttpOnly/);assert.match(value,/Secure/);assert.match(value,/SameSite=Strict/);return value.split(';')[0];}

test('maintenance login cannot use GET or put credentials in the URL',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot();
 const response=await f.request('/api?'+new URLSearchParams({action:'sop_login',sop_key:'synthetic-owned-home-fixture'}));
 assert.equal(response.status,403);assert.equal(response.headers.has('Set-Cookie'),false);assert.equal(await f.snapshot(),before);
});
test('maintenance logout cannot use GET or delete the current session',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot();
 const response=await f.request('/api?action=sop_logout');
 assert.equal(response.status,403);assert.equal(response.headers.has('Set-Cookie'),false);assert.equal(await f.snapshot(),before);
});
test('same-site cross-origin maintenance logout cannot delete an administrator session',async t=>{
 const f=await restrictionFixture(t,{active:true}),before=await f.snapshot();
 const response=await f.request('/api',{body:{action:'sop_logout'},headers:{Origin:'https://untrusted.ck91888.workers.dev','Sec-Fetch-Site':'same-site'}});
 assert.equal(response.status,403);assert.equal(response.headers.has('Set-Cookie'),false);assert.equal(await f.snapshot(),before);
});
test('compatibility maintenance login cannot backfill archived business document numbers',async t=>{
 const f=await restrictionFixture(t,{active:true,partial:true});
 await f.q('CREATE TABLE ck_backup_synthetic_numbers AS SELECT * FROM sop_document_numbers').run();await f.q('DELETE FROM sop_document_numbers').run();
 const db=f.env.DB;Object.assign(f.env,{DB:{prepare:db.prepare.bind(db),batch:db.batch.bind(db)},SOP_ACCESS_CONTROL:'false',SOP_PUBLIC_TEST_ACCESS:'false',SOP_USERS_JSON:JSON.stringify([{id:'synthetic-compat-manager',name:'合成兼容管理员',role:'manager',departments:['bulk'],key:'synthetic-compat-key'}])});
 const before=await f.snapshot(),response=await f.request('/api',{body:{action:'sop_login',sop_key:'synthetic-compat-key'}}),data=await response.json();
 assert.equal(data.ok,true,JSON.stringify(data));assert.match(response.headers.get('Set-Cookie')||'',/^ck_sop_session=/);assert.equal(await f.snapshot(),before);assert.deepEqual(await f.rows('SELECT * FROM sop_document_numbers'),[]);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);
});
for(const compat of [false,true])test(`maintenance method and origin guards precede all auth writes; compatibility=${compat}`,async t=>{
 const f=await restrictionFixture(t,{active:true});if(compat)compatibility(f);const before=await f.snapshot();
 for(const action of ['sop_login','sop_identity','sop_logout','sop_operator_confirm']){
  for(const method of ['GET','PUT','DELETE']){const r=await f.request('/api'+(method==='GET'?'?'+new URLSearchParams({action,sop_key:compat?compatUser.key:'synthetic-owned-home-fixture'}):''),{method,...(method==='GET'?{}:{body:{action,sop_key:compat?compatUser.key:'synthetic-owned-home-fixture'}})});assert.equal(r.status,403,action+' '+method);assert.equal(r.headers.has('Set-Cookie'),false);}
  for(const headers of [{Origin:'https://sibling.ck91888.workers.dev','Sec-Fetch-Site':'same-site'},{Origin:'https://sop-test.ck91888.cn','Sec-Fetch-Site':'same-site'},{Origin:'null'},{'Sec-Fetch-Site':'cross-site'},{Origin:'','Sec-Fetch-Site':'cross-site'}]){const r=await f.request('/api',{body:{action,sop_key:compat?compatUser.key:'synthetic-owned-home-fixture',name:'must not change'},headers});assert.equal(r.status,403,action+' '+JSON.stringify(headers));assert.equal(r.headers.has('Set-Cookie'),false);}
  const wrongType=await f.request('/api',{body:{action},headers:{'Content-Type':'text/plain'}});assert.ok(wrongType.status>=400);assert.equal(wrongType.headers.has('Set-Cookie'),false);
 }
 assert.equal(await f.snapshot(),before);assert.equal(f.objects.size,1);
});
test('normal office maintenance login, operator confirmation, identity and logout retain their policies',async t=>{
 const f=await restrictionFixture(t,{active:true,partial:true}),before=await f.snapshot(authTables),others=await f.rows("SELECT * FROM ck_access_sessions WHERE scope<>'office' ORDER BY token_hash");
 const login=await f.request('/api',{body:{action:'sop_login',sop_key:'synthetic-owned-home-fixture',role:'viewer'}});assert.equal((await login.json()).user.role,'manager');f.cookies.office=loginCookie(login);
 const identity=await f.request('/api',{body:{action:'sop_identity'},headers:{Origin:''}}),u=(await identity.json()).user;assert.equal(identity.status,200);assert.equal(u.scope,'office');assert.equal(u.role,'manager');assert.equal(u.operation_context.name,'');
 const stale=await f.request('/api',{body:{action:'sop_operator_confirm',name:'合成维护操作员'},headers:{'X-CK-Operation-Context':'stale'}});assert.equal(stale.status,409);
 const confirmed=await f.request('/api',{body:{action:'sop_operator_confirm',name:'合成维护操作员'},headers:{'X-CK-Operation-Context':u.operation_context.id}});assert.equal((await confirmed.json()).user.operation_context.name,'合成维护操作员');
 assert.equal(await f.snapshot(authTables),before);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);
 const logout=await f.request('/api',{body:{action:'sop_logout'}});assert.equal((await logout.json()).ok,true);assert.match(logout.headers.get('Set-Cookie'),/Max-Age=0/);assert.equal((await f.request('/api',{body:{action:'sop_identity'}})).status,401);assert.deepEqual(await f.rows("SELECT * FROM ck_access_sessions WHERE scope<>'office' ORDER BY token_hash"),others);assert.equal(await f.snapshot(authTables),before);
});
test('compatibility maintenance login, cold identity and logout preserve every business and archived record',async t=>{
 const f=await restrictionFixture(t,{active:true,partial:true});compatibility(f);await f.q('CREATE TABLE ck_backup_synthetic_numbers AS SELECT * FROM sop_document_numbers').run();await f.q('DELETE FROM sop_document_numbers').run();const db=f.env.DB;f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};const before=await f.snapshot();
 const login=await f.request('/api',{body:{action:'sop_login',sop_key:compatUser.key}});assert.equal((await login.json()).user.id,compatUser.id);f.cookies.office=loginCookie(login);
 const identity=await f.request('/api',{body:{action:'sop_identity'}});assert.deepEqual((await identity.json()).user,{id:compatUser.id,name:compatUser.name,role:compatUser.role,departments:compatUser.departments,public_test:false});assert.equal(await f.snapshot(),before);
 assert.equal((await f.request('/api',{body:{action:'v2_inbound_plan_create',customer:'must not write'}})).status,503);
 const logout=await f.request('/api',{body:{action:'sop_logout'}});assert.equal((await logout.json()).ok,true);assert.match(logout.headers.get('Set-Cookie'),/ck_sop_session=;.*Max-Age=0/);f.cookies.office='';assert.equal((await(await f.request('/api',{body:{action:'sop_identity'}})).json()).unauthorized,true);assert.equal(await f.snapshot(),before);
});
test('compatibility maintenance keeps credential errors, roles and request-controlled privilege fields',async t=>{
 const f=await restrictionFixture(t,{active:true});compatibility(f);const before=await f.snapshot();
 for(const [changes,body,code] of [[{},{},'AUTH_REQUIRED'],[{}, {sop_key:'wrong',SOP_REQUEST_USER:{role:'manager'},authenticationOnly:true},'AUTH_INVALID'],[{SOP_USERS_JSON:'null'},{sop_key:compatUser.key},'AUTH_CONFIG'],[{SOP_USERS_JSON:'broken'},{sop_key:compatUser.key},'AUTH_CONFIG'],[{SOP_USERS_JSON:JSON.stringify([{...compatUser,role:'forged'}])},{sop_key:compatUser.key},'AUTH_CONFIG']]){const r=await f.request('/api',{body:{action:'sop_login',...body},changes}),data=await r.json();assert.equal(data.ok,false);assert.equal(data.code,code);assert.equal(r.headers.has('Set-Cookie'),false);}
 f.env.SOP_USERS_JSON=JSON.stringify([{...compatUser,role:'viewer'}]);const r=await f.request('/api',{body:{action:'sop_login',sop_key:compatUser.key,role:'manager'}});assert.equal((await r.json()).user.role,'viewer');f.cookies.office=loginCookie(r);assert.equal((await f.request('/api/test-data/status')).status,403);assert.equal(await f.snapshot(),before);
});
test('maintenance does not grant field or kiosk login, identity or operator confirmation',async t=>{
 const f=await restrictionFixture(t,{active:true,partial:true}),before=await f.snapshot();
 for(const [path,scope]of [['/001/api','A'],['/attendance/api','kiosk']])for(const action of ['sop_login','sop_identity','sop_operator_confirm']){const r=await f.request(path,{scope,body:{action,sop_key:'synthetic-owned-home-fixture',badge:f.people.A.badgeId,name:'must not change'}});assert.equal(r.status,503);assert.equal(r.headers.has('Set-Cookie'),false);}
 assert.equal(await f.snapshot(),before);
});
test('authentication-only is an internal maintenance option and ordinary compatibility bootstrap is unchanged',async t=>{
 const f=await restrictionFixture(t);compatibility(f);await f.q('DELETE FROM sop_document_numbers').run();const db=f.env.DB;f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};
 const response=await f.request('/api',{body:{action:'sop_login',sop_key:compatUser.key,authenticationOnly:true}});assert.equal((await response.json()).ok,true);assert.ok((await f.rows('SELECT * FROM sop_document_numbers')).length>0,'normal non-maintenance request retains its existing initialization');
});
