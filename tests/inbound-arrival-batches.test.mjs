import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {inboundArrivalState,inboundArrivalScope,arrivalBatchOutput,arrivalBatchStatement,arrivalSettlementSQL} from '../worker-v2/inbound-arrival-batches.js';
function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const e={DB};
 DB.raw.exec(`INSERT INTO v2_inbound_plans(id,status,biz_class,biz_classes_json,external_inbound_no) VALUES('SYNTH-P','pending','direct_ship','["direct_ship"]','SYNTH-EXT');
 INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty) VALUES('SYNTH-L','SYNTH-P',1,'courier',2);
 INSERT INTO ck_courier_plan_items VALUES('999001880001','SYNTH-P','SYNTH-L'),('999001880002','SYNTH-P','SYNTH-L');`);
 const receive=async(n)=>DB.prepare(`INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name) VALUES(?,?,'8-1','2026-11-03T00:00:00Z','SYNTH-W','合成收货员','SYNTH-D','合成派审员')`).bind('SYNTH-R'+n,'99900188000'+n).run();
 return {DB,e,receive};
}
test('partial allowance uses immutable receipt evidence even when cached actual quantity is stale',async t=>{
 const {e,receive}=fixture(t);let s=await inboundArrivalState(e,'SYNTH-P');assert.equal(s.received,0);assert.equal(s.remaining,0);
 assert.equal(await inboundArrivalScope(e,{id:'SYNTH-P'},'SYNTH-EXT','inbound_direct_tally'),null);
 await receive(1);s=await inboundArrivalState(e,'SYNTH-P');assert.equal(s.received,1);assert.equal(s.fully_arrived,false);assert.equal(s.remaining,1);
 const scope=await inboundArrivalScope(e,{id:'SYNTH-P'},'SYNTH-EXT','inbound_direct_tally');await receive(2);
 assert.equal(scope.received,1);assert.equal(scope.lines[0].available,1);
 assert.throws(()=>arrivalBatchOutput(scope,{result_lines:[{unit_type:'courier',putaway_qty:2}]}),/超过/);
 assert.equal(arrivalBatchOutput(scope,{result_lines:[{unit_type:'courier',putaway_qty:1}]}).reduce((n,l)=>n+l.putaway_qty,0),1);
});
test('mixed vehicle quantities are unavailable before the physical unload milestone',async t=>{
 const {DB,e,receive}=fixture(t);await receive(1);
 DB.raw.exec(`INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty,actual_qty) VALUES('SYNTH-TRUCK','SYNTH-P',2,'carton',5,5)`);
 let s=await inboundArrivalState(e,'SYNTH-P');assert.equal(s.lines.find(l=>l.unit_type==='carton').available,0);assert.equal(s.remaining,1);
 await receive(2);s=await inboundArrivalState(e,'SYNTH-P');assert.equal(s.fully_arrived,false);
 DB.raw.exec(`UPDATE v2_inbound_plans SET unload_completed_at='2026-11-03T01:00:00Z' WHERE id='SYNTH-P'`);
 s=await inboundArrivalState(e,'SYNTH-P');assert.equal(s.fully_arrived,true);assert.equal(s.remaining,7);
});
test('same-unit lines receive each unit of output once and reject negative, unknown, duplicate or excessive output',()=>{
 const scope={fully_arrived:false,lines:[{line_id:'SYNTH-A',unit_type:'carton',available:2},{line_id:'SYNTH-B',unit_type:'carton',available:3}]};
 assert.deepEqual(arrivalBatchOutput(scope,{result_lines:[{unit_type:'carton',putaway_qty:4}]}).map(l=>l.putaway_qty),[2,2]);
 for(const lines of [[],[{unit_type:'carton',putaway_qty:-1}],[{unit_type:'carton',putaway_qty:6}],[{unit_type:'pallet',putaway_qty:1}],[{unit_type:'carton',putaway_qty:1},{unit_type:'carton',putaway_qty:1}],[{unit_type:'carton',putaway_qty:'invalid'}]])assert.throws(()=>arrivalBatchOutput(scope,{result_lines:lines}));
});
test('additive batch metadata is immutable and does not change old stock, labor or receipt facts',async t=>{
 const {DB,e,receive}=fixture(t);await receive(1);const scope=await inboundArrivalScope(e,{id:'SYNTH-P'},'SYNTH-EXT','inbound_direct_tally');
 const before=DB.raw.prepare('SELECT * FROM ck_courier_receipts').all();
 DB.raw.exec(`INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,inbound_external_no,status) VALUES('SYNTH-J','inbound_direct_tally','inbound_plan','SYNTH-P','SYNTH-EXT','working')`);
 await arrivalBatchStatement(e,'SYNTH-J',scope,'2026-11-03T01:00:00Z').run();
 assert.throws(()=>DB.raw.exec(`UPDATE ck_inbound_arrival_batches SET external_no='OTHER'`),/immutable/);
 assert.deepEqual(DB.raw.prepare('SELECT * FROM ck_courier_receipts').all(),before);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,0);
 const sql=arrivalSettlementSQL("'SYNTH-P'");assert.equal((await DB.prepare('SELECT '+sql+' AS settled').first()).settled,0);
});
