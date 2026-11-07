import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {workMaterialCountFor} from '../worker-v2/work-chain.js';
import {outboundFixture,outboundHash,outboundCases,publishedOutboundSql} from './outbound-page-material-fixture.mjs';
const original=JSON.parse(fs.readFileSync(new URL('./outbound-page-material-baseline.json',import.meta.url)));
// Derived from the fixed published Worker, permitting only the explicit null
// count correction and its filter/page consequences. Keep the old hashes intact.
const corrected=JSON.parse(fs.readFileSync(new URL('./material-count-corrected-baseline.json',import.meta.url)));
const published={...original,cases:{...original.cases,...corrected.cases},modes:{...original.modes,...corrected.modes}};
test('outbound API preserves all 22 published cases with only the explicit null count/filter correction',async t=>{
 const f=await outboundFixture(t),before=await f.snapshot();
 for(const [name,body]of outboundCases){
  const {r,queries}=await f.read(body);assert.equal(outboundHash(r),published.cases[name].sha256,name);assert.equal(r.total,published.cases[name].total,name);assert.equal(r.items.length,published.cases[name].returned,name);assert.equal(queries.length,1);assert.equal(queries[0].length,2,'count and list stay in the same batch');
  const [count,list]=queries[0],optimized=!['0','1'].includes(String(body.has_material??'').trim());assert.equal(list.sql.startsWith('SELECT * FROM (WITH outbound_page AS MATERIALIZED ('),optimized,name);assert.ok(count.sql.startsWith('SELECT COUNT(*) AS c'),'the original full count query stays unchanged');
  const [a,b]=await f.env.DB.batch([f.env.DB.prepare(list.sql).bind(...list.args),f.env.DB.prepare(publishedOutboundSql(list.sql)).bind(...list.args)]);assert.deepEqual(a.results,b.results,name+' original SQL rows/order');
 }
 assert.equal(await f.snapshot(),before,'all list requests preserve material, audit and business rows');
});
test('only returned page rows evaluate material projections; native D1 does not repeat off-page material reads',async t=>{
 const f=await outboundFixture(t,{orders:1000,history:100,sequential:true}),before=await f.snapshot(),{r,queries}=await f.read({limit:50}),list=queries[0][1],old=publishedOutboundSql(list.sql);assert.equal(r.items.length,50);assert.equal(r.total,1000);assert.equal(r.items[0].id,f.oid(999));
 const rs=await f.env.DB.batch([f.env.DB.prepare(old).bind(...list.args),f.env.DB.prepare(list.sql).bind(...list.args)]);assert.deepEqual(rs[0].results,rs[1].results);
 if(process.env.CK_OUTBOUND_PAGE_NATIVE_D1==='true'){
  assert.ok(rs[1].meta.rows_read<rs[0].meta.rows_read/4,JSON.stringify(rs.map(x=>x.meta)));assert.equal(rs[0].meta.rows_written,0);assert.equal(rs[1].meta.rows_written,0);console.log(JSON.stringify({synthetic_orders:1000,before_rows_read:rs[0].meta.rows_read,after_rows_read:rs[1].meta.rows_read}));
 }else{
  const expr=workMaterialCountFor(f.env);let calls=0;f.DB.raw.function('synthetic_projection',{deterministic:true},v=>{calls++;return v;});const probe=sql=>{calls=0;f.DB.raw.prepare(sql.replace(expr+' AS material_count','synthetic_projection('+expr+') AS material_count')).all(...list.args);return calls;};
  assert.equal(probe(old),1000);assert.equal(probe(list.sql),50,'LIMIT must take effect before the display count projection');
 }
 assert.equal(await f.snapshot(),before);
});
test('material filters, disabled work-chain and load-trip candidates keep their published API and query paths',async t=>{
 const f=await outboundFixture(t),before=await f.snapshot();
 for(const [name,modify,body]of [['legacy',()=>f.env.SOP_WORK_CHAIN_ENABLED='false',{}],['legacy-has-1',()=>{},{has_material:'1'}],['legacy-has-0',()=>{},{has_material:'0'}],['non-staging',()=>{f.env.SOP_WORK_CHAIN_ENABLED='true';f.env.SOP_ENVIRONMENT='production';},{}],['load-trip',()=>f.env.SOP_ENVIRONMENT='staging',{scene:'load_trip'}]]){
  modify();const r=await f.callTarget('office','v2_outbound_order_list',body);assert.equal(outboundHash(r),published.modes[name].sha256,name);
 }
 const {queries}=await f.read({has_material:'1'});assert.ok(!queries[0][1].sql.includes('outbound_page AS MATERIALIZED'));assert.ok(queries[0][0].sql.includes(workMaterialCountFor(f.env)));assert.equal(await f.snapshot(),before);
 f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';f.env.SOP_AUTO_DISPATCH_TIME_ENABLED='true';
 await f.read({customer_keyword:'SYN-NONEXISTENT'});const config=await f.rows('SELECT * FROM ck_dispatch_time_config ORDER BY id');
 if(f.env.DB.metrics)f.env.DB.metrics.length=0;
 const active=await f.read();assert.equal(outboundHash(active.r),published.cases.default.sha256);assert.equal(active.queries.length,1);assert.equal(active.queries[0].length,2);assert.ok(active.queries[0].every(s=>s.sql.startsWith('SELECT')));
 if(f.env.DB.metrics)assert.ok(f.env.DB.metrics.every(m=>m.rows_written===0),'active attendance/duty wrappers must keep the list read-only');
 assert.deepEqual(await f.rows('SELECT * FROM ck_dispatch_time_config ORDER BY id'),config);assert.equal(await f.snapshot(),before);
});
test('office, verified field and limited badge retain baseline list access while guest access remains rejected',async t=>{
 const f=await outboundFixture(t),before=await f.snapshot();
 for(const scope of ['A','guest'])assert.equal(outboundHash(await f.callTarget(scope,'v2_outbound_order_list',{})),published.modes[scope].sha256,scope);
 const p=(await f.ok('office','sop_attendance_register',{name:'合成限直发派审员',agency:'가온'})).person;await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['direct_ship'],version:0,reason:'合成出库列表权限回归'});const login=await f.ok('A','sop_login',{badge:p.badgeId});assert.deepEqual(login.user.departments,['direct_ship']);
 assert.equal(outboundHash(await f.callTarget('A','v2_outbound_order_list',{})),published.modes['limited-field'].sha256);
 // Registration changes only separate attendance/auth tables; list-owned rows remain immutable.
 assert.equal(await f.snapshot(),before);
});
test('fresh requests observe attachment additions and batch restore before the atomic count/list read, without stale counts',async t=>{
 const f=await outboundFixture(t),first=(await f.read({limit:200})).r,before=first.items.find(x=>x.id===f.oid(1)).material_count,batch=f.env.DB.batch.bind(f.env.DB);let injected=false;
 f.env.DB.batch=async ss=>{
  if(!injected&&ss[0]?.sql.startsWith('SELECT COUNT(*) AS c FROM v2_outbound_orders')){injected=true;await f.run("UPDATE ck_batch_material_state SET removed=0,revision=2 WHERE attachment_id='SYN-BATCH'");await f.run("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key) VALUES('SYN-NEW-DIRECT','outbound_order',?,'outbound_material','synthetic/new')",f.oid(1));}
  return batch(ss);
 };
 const current=await f.read({limit:200});assert.equal(current.queries.length,1);assert.equal(current.queries[0].length,2);assert.equal(current.r.items.find(x=>x.id===f.oid(1)).material_count,before+2);
 await f.run("UPDATE ck_batch_material_state SET removed=1,revision=3 WHERE attachment_id='SYN-BATCH'");const snapshot=await f.snapshot(),latest=(await f.read({limit:200})).r;assert.equal(latest.items.find(x=>x.id===f.oid(1)).material_count,before+1);assert.equal(await f.snapshot(),snapshot);
});
test('a failed list read preserves records and the next request can recover without cached failure',async t=>{
 const f=await outboundFixture(t),before=await f.snapshot(),batch=f.env.DB.batch.bind(f.env.DB);let fail=true;
 f.env.DB.batch=async ss=>{if(fail&&ss[0]?.sql.startsWith('SELECT COUNT(*) AS c FROM v2_outbound_orders')){fail=false;throw Error('synthetic outbound read failure');}return batch(ss);};
 assert.equal((await f.callTarget('office','v2_outbound_order_list',{})).ok,false);assert.equal(outboundHash((await f.read()).r),published.cases.default.sha256);assert.equal(await f.snapshot(),before);
});
