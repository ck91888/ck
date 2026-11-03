import test from 'node:test';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import fs from 'node:fs';
import {database} from './d1-adapter.mjs';import {localD1} from './d1-local-runtime.mjs';
import {ensureAttachmentKeyIndex} from '../worker-v2/attachment-key-index.js';
import worker from '../worker-v2/index.js';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
const index='idx_v2_attachments_file_key',version=fs.readFileSync(new URL('../worker-v2/index.js',import.meta.url),'utf8').match(/const CURRENT_SCHEMA_VERSION = '([^']+)'/)[1];
async function fixture(t,count=0){
 const source=database();source.raw.exec('CREATE TABLE v2_schema_meta(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)');source.raw.prepare('INSERT INTO v2_schema_meta VALUES(?,?,?)').run('schema_version',version,'2026-11-01T00:00:00Z');
 const put=source.raw.prepare('INSERT INTO v2_attachments(id,file_key,related_doc_type,related_doc_id,file_name,content_type) VALUES(?,?,?,?,?,?)');
 source.raw.exec('BEGIN');for(let i=0;i<count;i++)put.run('SYN-ATT-'+i,'synthetic/key/'+i,'inbound_plan','SYN-PLAN','合成资料'+i+'.pdf','application/pdf');source.raw.exec('COMMIT');
 let native;const db=process.env.CK_PERF_NATIVE_D1==='true'?(native=await localD1(source.raw)):source;
 t.after(async()=>{await native?.close();source.raw.close();});
 const rows=async(sql,...args)=>(await db.prepare(sql).bind(...args).all()).results;
 const snapshot=async()=>createHash('sha256').update(JSON.stringify([await rows('SELECT rowid,* FROM v2_attachments ORDER BY rowid'),await rows('SELECT * FROM v2_schema_meta ORDER BY key')])).digest('hex');
 return {db,rows,snapshot};
}
test('ten-thousand-row old database gains a non-unique key lookup without rewriting evidence',async t=>{
 const f=await fixture(t,10000),before=await f.snapshot(),sql='SELECT related_doc_type,related_doc_id FROM v2_attachments WHERE file_key=?';
 const old=await f.db.prepare(sql).bind('synthetic/key/9999').all();const oldPlan=await f.rows('EXPLAIN QUERY PLAN '+sql,'synthetic/key/9999');
 await ensureAttachmentKeyIndex(f.db);const current=await f.db.prepare(sql).bind('synthetic/key/9999').all();
 assert.deepEqual(current.results,old.results);assert.equal(await f.snapshot(),before);
 assert.ok(oldPlan.some(p=>/SCAN v2_attachments/.test(p.detail)));assert.ok((await f.rows('EXPLAIN QUERY PLAN '+sql,'synthetic/key/9999')).some(p=>p.detail.includes(index)));
 assert.equal((await f.rows("PRAGMA index_list('v2_attachments')")).find(p=>p.name===index).unique,0);
 if(process.env.CK_PERF_NATIVE_D1==='true'){assert.equal(old.meta.rows_read,10000);assert.ok(current.meta.rows_read<=3,JSON.stringify(current.meta));console.log(JSON.stringify({synthetic_rows:10000,before_rows_read:old.meta.rows_read,after_rows_read:current.meta.rows_read}));}
});
test('duplicate, empty and NULL keys retain their row ordering and metadata; new duplicates remain allowed',async t=>{
 const f=await fixture(t);for(const [i,key]of ['same','same','',null].entries())await f.db.prepare('INSERT INTO v2_attachments(id,file_key,file_name) VALUES(?,?,?)').bind('SYN-DUP-'+i,key,'合成'+i).run();
 const before=await f.snapshot(),query=()=>f.rows('SELECT rowid,id,file_name FROM v2_attachments WHERE file_key=?','same'),old=await query();
 await ensureAttachmentKeyIndex(f.db);assert.deepEqual(await query(),old);assert.equal(await f.snapshot(),before);
 await f.db.prepare("INSERT INTO v2_attachments(id,file_key) VALUES('SYN-NEW-DUP','same')").run();assert.equal((await query()).length,3);
});
test('independent cold bindings converge to one index without changing schema version or old rows',async t=>{
 const f=await fixture(t,5),before=await f.snapshot(),binding=()=>({prepare:f.db.prepare.bind(f.db),batch:f.db.batch.bind(f.db)});
 await Promise.all([ensureAttachmentKeyIndex(binding()),ensureAttachmentKeyIndex(binding())]);
 assert.equal((await f.rows("SELECT name FROM sqlite_master WHERE type='index' AND name=?",index)).length,1);assert.equal(await f.snapshot(),before);
});
test('a failed atomic index batch rolls back and a retry on the same binding is not cached as ready',async t=>{
 const f=await fixture(t,5),before=await f.snapshot();let fail=true;
 await f.db.prepare('CREATE TABLE synthetic_index_failure(allowed INTEGER CHECK(allowed=1))').run();
 const binding={prepare:f.db.prepare.bind(f.db),batch:ss=>f.db.batch(fail?[...ss,f.db.prepare('INSERT INTO synthetic_index_failure VALUES(0)')]:ss)};
 await assert.rejects(ensureAttachmentKeyIndex(binding));assert.equal((await f.rows("SELECT name FROM sqlite_master WHERE name=?",index)).length,0);assert.equal(await f.snapshot(),before);
 fail=false;await ensureAttachmentKeyIndex(binding);assert.equal((await f.rows("SELECT name FROM sqlite_master WHERE name=?",index)).length,1);assert.equal(await f.snapshot(),before);
});
test('Worker old-schema initialization refuses a failed index transaction and can recover without data changes',async t=>{
 const f=await fixture(t,3),before=await f.snapshot();let fail=true;
 await f.db.prepare('CREATE TABLE synthetic_index_failure(allowed INTEGER CHECK(allowed=1))').run();
 const binding={prepare:f.db.prepare.bind(f.db),batch:ss=>f.db.batch(fail&&ss.some(s=>s.sql.includes(index))?[...ss,f.db.prepare('INSERT INTO synthetic_index_failure VALUES(0)')]:ss)};
 const env={DB:binding,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'},call=()=>worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_identity'})}),env);
 const failed=await call();assert.equal(failed.status,500);assert.match((await failed.json()).error,/migration failed/);assert.equal((await f.rows("SELECT name FROM sqlite_master WHERE name=?",index)).length,0);assert.equal(await f.snapshot(),before);
 fail=false;const good=await call();assert.equal((await good.json()).ok,true);assert.equal(await f.snapshot(),before);
});
test('private GET authorization and first duplicate metadata are identical before and after indexing',async t=>{
 const f=await fixture(t),bytes=new Uint8Array([37,80,68,70]),env={DB:f.db,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-index-permission'),R2_BUCKET:{async get(){return {body:bytes,httpMetadata:{contentType:'application/octet-stream'}};}}};let cookie='';
 const call=async(action,body={})=>{const r=await worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,...body})}),env);const next=r.headers.getSetCookie().find(x=>x.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];const result=rememberOperation(await r.json(),cookie);assert.equal(result.ok,true,result.error);return result;};
 await call('sop_login',{sop_key:'synthetic-index-permission'});await confirmOperation(call,'合成索引验收员');
 await f.db.prepare("INSERT INTO v2_attachments(id,file_key,related_doc_type,related_doc_id,file_name,content_type) VALUES('SYN-FIRST','synthetic/duplicate','inbound_plan','SYN-PLAN','原始合成.pdf','application/pdf'),('SYN-SECOND','synthetic/duplicate','issue_ticket','SYN-ISSUE','第二合成.png','image/png')").run();
 await f.db.prepare('DROP INDEX '+index).run();
 const get=async(c=cookie)=>{const r=await worker.fetch(new Request('https://fixture.local/api/file?key=synthetic/duplicate',{headers:{Cookie:c}}),env);return {status:r.status,type:r.headers.get('Content-Type'),disposition:r.headers.get('Content-Disposition'),cache:r.headers.get('Cache-Control'),bytes:[...new Uint8Array(await r.arrayBuffer())]};};
 const before=await get(),unauth=await get('');await ensureAttachmentKeyIndex({prepare:f.db.prepare.bind(f.db),batch:f.db.batch.bind(f.db)});
 assert.deepEqual(await get(),before);assert.deepEqual(await get(''),unauth);assert.equal(before.status,200);assert.equal(before.type,'application/pdf');assert.match(before.disposition,/pdf/);assert.equal(unauth.status,401);
});
test('a real field session retains both duplicate-key denial and allowed-original access',async t=>{
 const f=await ownedHomeFixture();
 const daily=(await f.ok('office','sop_attendance_register',{name:'合成限大货派审员',agency:'가온'})).person;
 await f.ok('office','sop_attendance_checkin',{badge:daily.badgeId,agency:daily.agency});
 await f.ok('office','sop_daily_dispatch_update',{person_id:daily.id,badge:daily.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成附件权限回归'});
 const login=await f.ok('A','sop_login',{badge:daily.badgeId});assert.equal(login.user.scope,'field');assert.deepEqual(login.user.departments,['bulk']);
 let native;if(process.env.CK_PERF_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 t.after(async()=>{await native?.close();f.DB.raw.close();});let reads=0;f.env.R2_BUCKET={async get(){reads++;return {body:new Uint8Array([1,2,3]),httpMetadata:{contentType:'application/pdf'}};}};
 await ensureAttachmentKeyIndex(f.env.DB);
 await f.env.DB.prepare("INSERT INTO v2_issue_tickets(id,biz_class) VALUES('SYN-DIRECT-ISSUE','direct_ship')").run();
 for(const [id,key,type,doc]of [['SYN-DENY-1','synthetic/deny','issue_ticket','SYN-DIRECT-ISSUE'],['SYN-DENY-2','synthetic/deny','inbound_plan','SYN-PLAN'],['SYN-ALLOW-1','synthetic/allow','inbound_plan','SYN-PLAN'],['SYN-ALLOW-2','synthetic/allow','issue_ticket','SYN-DIRECT-ISSUE']])await f.env.DB.prepare('INSERT INTO v2_attachments(id,file_key,related_doc_type,related_doc_id,file_name) VALUES(?,?,?,?,?)').bind(id,key,type,doc,id+'.pdf').run();
 await f.env.DB.prepare('DROP INDEX '+index).run();
 const get=async key=>{const r=await worker.fetch(new Request('https://fixture.local/001/api/file?key='+key,{headers:{Cookie:f.cookies.A}}),f.env);return {status:r.status,bytes:[...new Uint8Array(await r.arrayBuffer())]};};
 const denied=await get('synthetic/deny'),allowed=await get('synthetic/allow');assert.equal(denied.status,401);assert.equal(allowed.status,200);assert.equal(reads,1);
 await ensureAttachmentKeyIndex({prepare:f.env.DB.prepare.bind(f.env.DB),batch:f.env.DB.batch.bind(f.env.DB)});
 assert.deepEqual(await get('synthetic/deny'),denied);assert.deepEqual(await get('synthetic/allow'),allowed);assert.equal(reads,2);
});
