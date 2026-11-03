// Real authenticated Worker; isolated synthetic SQLite, optionally local native D1.
import test from 'node:test';
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
 const f=await ownedHomeFixture();let native;
 if(process.env.CK_P1_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 t.after(async()=>{await native?.close();f.DB.raw.close();});
 const first=(s,...a)=>f.env.DB.prepare(s).bind(...a).first();
 const material=await f.ok('office','v2_003_material_save',{name_zh:'合成并发耗材',category:'包材',unit:'卷',opening_qty:10});
 const request=(key='SYN-ISSUE-ONCE',extra={})=>({material_id:material.id,txn_type:'issue',qty:3,department:'大货',recipient_name:'合成操作员',client_req_id:key,...extra});
 const stock=()=>first('SELECT current_qty,stock_version FROM v2_003_materials WHERE id=?',material.id),count=()=>first("SELECT COUNT(*) n FROM v2_003_material_txns WHERE material_id=? AND txn_type='issue'",material.id);
 return {...f,first,material,request,stock,count};
}
async function beforeBatch(f){
 const db=f.env.DB;let reached,release,held=false;const ready=new Promise(r=>reached=r),gate=new Promise(r=>release=r);
 f.env.DB={prepare:db.prepare.bind(db),async batch(ss){if(!held&&ss.some(s=>s.sql.includes('INSERT INTO v2_003_material_txns'))){held=true;reached();await gate;}return db.batch(ss);}};
 return{ready,release,restore(){f.env.DB=db;}};
}
test('the same concurrent material request deducts once and returns one transaction',async t=>{
 const f=await fixture(t),body=f.request();const replies=await Promise.all([f.call('office','v2_003_material_txn',body),f.call('office','v2_003_material_txn',body)]);
 assert.ok(replies.every(x=>x.ok),JSON.stringify(replies));assert.equal((await f.stock()).current_qty,7);assert.equal((await f.count()).n,1);assert.equal(replies[0].id,replies[1].id);
});
test('an earlier material request held before commit cannot deduct again after the same request commits elsewhere',async t=>{
 const f=await fixture(t),body=f.request(),hold=await beforeBatch(f),pending=f.call('office','v2_003_material_txn',body);await hold.ready;
 let saved;try{saved=await f.ok('office','v2_003_material_txn',body);}finally{hold.release();}
 const replay=await pending;hold.restore();assert.equal(replay.ok,true,JSON.stringify(replay));assert.equal((await f.stock()).current_qty,7);assert.equal((await f.count()).n,1);assert.equal(replay.id,saved.id);
});
test('different request identities each deduct once and preserve the ledger chain',async t=>{
 const f=await fixture(t),replies=await Promise.all(['SYN-ONE','SYN-TWO'].map(k=>f.call('office','v2_003_material_txn',f.request(k))));assert.ok(replies.every(x=>x.ok),JSON.stringify(replies));assert.equal((await f.stock()).current_qty,4);assert.equal((await f.count()).n,2);
 const rows=(await f.env.DB.prepare("SELECT qty_before,qty_after FROM v2_003_material_txns WHERE material_id=? AND txn_type='issue' ORDER BY qty_before DESC").bind(f.material.id).all()).results;assert.deepEqual(rows.map(x=>[x.qty_before,x.qty_after]),[[10,7],[7,4]]);
});
test('failed stock writes roll back the ledger and receipt; retry and lost-response replay remain one deduction',async t=>{
 const f=await fixture(t),body=f.request();await f.env.DB.prepare("CREATE TRIGGER synthetic_material_failure BEFORE UPDATE OF current_qty ON v2_003_materials BEGIN SELECT RAISE(ABORT,'synthetic stock failure'); END").run();
 assert.equal((await f.call('office','v2_003_material_txn',body)).ok,false);assert.equal((await f.stock()).current_qty,10);assert.equal((await f.count()).n,0);assert.equal((await f.first('SELECT COUNT(*) n FROM v2_idempotency_keys WHERE idem_key=?',body.client_req_id)).n,0);
 await f.env.DB.prepare('DROP TRIGGER synthetic_material_failure').run();const saved=await f.ok('office','v2_003_material_txn',body);const replay=await f.ok('office','v2_003_material_txn',body);assert.equal((await f.stock()).current_qty,7);assert.equal((await f.count()).n,1);assert.equal(replay.id,saved.id);
});
test('receipt persistence is part of the stock transaction and cannot fail silently',async t=>{
 const f=await fixture(t),body=f.request();
 await f.env.DB.prepare("CREATE TRIGGER synthetic_receipt_failure BEFORE INSERT ON v2_idempotency_keys WHEN NEW.action='v2_003_material_txn' BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END").run();
 assert.equal((await f.call('office','v2_003_material_txn',body)).ok,false);assert.equal((await f.stock()).current_qty,10);assert.equal((await f.count()).n,0);
 await f.env.DB.prepare('DROP TRIGGER synthetic_receipt_failure').run();await f.ok('office','v2_003_material_txn',body);assert.equal((await f.stock()).current_qty,7);assert.equal((await f.count()).n,1);
});
for(const [txn_type,extra,expected] of [['inbound',{},13],['use',{},7],['return',{},13],['adjust',{delta:2},12],['stocktake',{counted_qty:8},8]]){
 test('concurrent '+txn_type+' retry publishes one original receipt',async t=>{
  const f=await fixture(t),body=f.request('SYN-'+txn_type,{txn_type,...extra}),hold=await beforeBatch(f),pending=f.call('office','v2_003_material_txn',body);await hold.ready;
  let saved;try{saved=await f.ok('office','v2_003_material_txn',body);}finally{hold.release();}
  const replay=await pending;hold.restore();assert.equal(replay.ok,true,JSON.stringify(replay));assert.equal(replay.id,saved.id);assert.equal((await f.stock()).current_qty,expected);
  assert.equal((await f.first('SELECT COUNT(*) n FROM v2_003_material_txns WHERE material_id=? AND txn_type=?',f.material.id,txn_type)).n,1);
 });
}
test('a receipt key belonging to another action cannot be replayed as a material transaction',async t=>{
 const f=await fixture(t),body=f.request();await f.env.DB.prepare('INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES(?,?,?,?)').bind(body.client_req_id,'synthetic_other_action',JSON.stringify({ok:true,id:'SYN-OTHER'}),new Date().toISOString()).run();
 const reply=await f.call('office','v2_003_material_txn',body);assert.equal(reply.ok,false);assert.equal(reply.error,'request_id_conflict');assert.equal((await f.stock()).current_qty,10);assert.equal((await f.count()).n,0);
});
