import test from 'node:test';
import assert from 'node:assert/strict';
import {restrictionFixture} from './test-reset-restriction-fixture.mjs';

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
