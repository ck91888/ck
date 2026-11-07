import test from 'node:test';
import assert from 'node:assert/strict';
import {mixedShippingFixture} from './mixed-shipping-fixture.mjs';
const passed=r=>assert.equal(r.ok,true,JSON.stringify(r));
const denied=r=>assert.equal(r.ok,false,JSON.stringify(r));
const fiveAndBox=[{quantity:5,unit:'托'},{quantity:1,unit:'箱'}];
test('five linked pallets plus one separately verified carton: approval unlocks only the carton and preserves every original business row',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need(),first=await f.book(n.id,[f.booking(5,'托')]);passed(first);
 const oldNeed=await f.get(n.id),task=await f.get(n.task_id),order=f.DB.raw.prepare('SELECT * FROM v2_outbound_orders').all(),outputs=f.DB.raw.prepare('SELECT * FROM v2_ops_job_results').all(),labor=f.DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all();
 denied(await f.book(n.id,[f.booking(1,'箱')]));passed(await f.propose(n.id));
 denied(await f.book(n.id,[f.booking(1,'箱')]));
 denied(await f.change('service','sop_shipping_output_review',n.id,{change_id:(await f.get(n.id)).shipping_output_change.id,decision:'pass',reason:'无审核权限'}));
 passed(await f.review(n.id));const available=await f.availability(n.id);assert.deepEqual(available.by_unit,[{unit:'托',quantity:5,used:5,remaining:0},{unit:'箱',quantity:1,used:0,remaining:1}]);assert.equal(available.remaining,null);
 const next=await f.book(n.id,[f.booking(1,'箱')]);passed(next);const final=await f.get(n.id);assert.equal(final.status,'linked');assert.deepEqual(final.result,n.result);assert.deepEqual(final.links[0],oldNeed.links[0]);assert.equal(final.shipping_output.version,1);assert.equal(final.shipping_output.actor_id,'QA-service');assert.equal(final.shipping_output.review.actor_id,'QA-reviewer');
 assert.deepEqual(await f.get(n.task_id),task);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_ops_job_results').all(),outputs);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_ops_job_workers').all(),labor);assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').all(first.outbounds[0].id),order);
 const extra=f.DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').get(next.outbounds[0].id);assert.equal(extra.planned_box_count,1);assert.equal(extra.planned_pallet_count,0);assert.equal(JSON.parse(extra.stock_operation_result_json).shipping_output.version,1);
 const events=f.DB.raw.prepare("SELECT * FROM sop_events WHERE record_id=? AND action LIKE 'sop_shipping_output_%' ORDER BY expected_revision").all(n.id);assert.equal(events.length,2);assert.equal(JSON.parse(events[0].before_json).shipping_output,undefined);assert.equal(JSON.parse(events[1].after_json).shipping_output.review.decision,'pass');
});
test('unlinked mixed output, repeated partial bookings, cancellation release, and shipped/missing orders remain consumed per unit',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need();passed(await f.propose(n.id,[{quantity:5,unit:'托'},{quantity:3,unit:'箱'}]));passed(await f.review(n.id));
 passed(await f.book(n.id,[f.booking(2,'托'),f.booking(1,'箱')]));const second=await f.book(n.id,[f.booking(3,'托'),f.booking(1,'箱')]);passed(second);assert.deepEqual((await f.availability(n.id)).by_unit.map(c=>c.remaining),[0,1]);
 denied(await f.book(n.id,[f.booking(1,'托')]));denied(await f.book(n.id,[f.booking(2,'箱')]));
 passed(await f.call('manager','v2_outbound_order_update_status',{id:second.outbounds[1].id,status:'cancelled'}));assert.deepEqual((await f.availability(n.id)).by_unit.map(c=>c.remaining),[0,2]);
 f.DB.raw.prepare("UPDATE v2_outbound_orders SET status='shipped' WHERE id=?").run(second.outbounds[0].id);
 denied(await f.call('manager','v2_outbound_order_update_status',{id:second.outbounds[0].id,status:'cancelled'}));
 denied(await f.propose(n.id,[{quantity:4,unit:'托'},{quantity:3,unit:'箱'}]));
 f.DB.raw.prepare('DELETE FROM v2_outbound_orders WHERE id=?').run(second.outbounds[0].id);
 assert.equal((await f.availability(n.id)).by_unit[0].remaining,0,'orphan allocation cannot be reused');
});
test('strict quantities, explicit units and synonyms cannot convert packaging metrics into shipping capacity',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need();for(const components of [[],[{quantity:0,unit:'箱'}],[{quantity:-1,unit:'箱'}],[{quantity:1.5,unit:'箱'}],[{quantity:1,unit:'unknown'}],[{quantity:Number.MAX_SAFE_INTEGER+1,unit:'箱'}]])denied(await f.propose(n.id,components));
 assert.deepEqual((await f.availability(n.id)).by_unit.map(x=>[x.quantity,x.unit]),[[5,'托']]);
 passed(await f.propose(n.id,[{quantity:2,unit:' PALLETS '},{quantity:3,unit:'托盘'},{quantity:1,unit:'Boxes'}]));passed(await f.review(n.id));assert.deepEqual((await f.get(n.id)).shipping_output.components,fiveAndBox);
 for(const row of [f.booking(0,'箱'),f.booking(-1,'箱'),f.booking(1.1,'箱'),f.booking(1,''),f.booking(1,'件'),f.booking(6,'托')])denied(await f.book(n.id,[row]));
 passed(await f.book(n.id,[f.booking(2,'pallet'),f.booking(3,'托盘'),f.booking(1,'carton')]));assert.deepEqual((await f.availability(n.id)).by_unit.map(x=>x.remaining),[0,0]);
});
test('existing roles, departments and task ownership guard approval; returned changes never increase capacity',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need();for(const role of ['viewer','other'])denied(await f.change(role,'sop_shipping_output_propose',n.id,{components:fiveAndBox,reason:'合成权限'}));
 passed(await f.propose(n.id));const pending=await f.get(n.id);denied(await f.change('service','sop_shipping_output_review',n.id,{change_id:pending.shipping_output_change.id,decision:'pass',reason:'合成权限'}));
 passed(await f.review(n.id,{decision:'return',reason:'请核实独立散箱'}));assert.equal((await f.get(n.id)).shipping_output,undefined);denied(await f.book(n.id,[f.booking(1,'箱')]));
 passed(await f.propose(n.id));passed(await f.change('dispatcher','sop_shipping_output_review',n.id,{change_id:(await f.get(n.id)).shipping_output_change.id,decision:'pass',reason:'原派审员复核'}));
 const original=f.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(n.task_id).state,d=JSON.parse(original);d.owner_id='QA-someone-else';f.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(d),n.task_id);
 passed(await f.propose(n.id));denied(await f.change('dispatcher','sop_shipping_output_review',n.id,{change_id:(await f.get(n.id)).shipping_output_change.id,decision:'pass',reason:'非负责人'}));
});
test('same requests replay, changed payloads reject, lost acknowledgements and concurrent proposals/reviews are atomic',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need(),body={id:n.id,revision:n.revision,components:fiveAndBox,reason:'合成核实',client_req_id:crypto.randomUUID()};
 const two=await Promise.all([f.call('service','sop_shipping_output_propose',body),f.call('service','sop_shipping_output_propose',body)]);passed(two[0]);assert.deepEqual(two[0],two[1]);denied(await f.call('service','sop_shipping_output_propose',{...body,reason:'changed'}));
 const latest=await f.get(n.id),review={id:n.id,revision:latest.revision,change_id:body.client_req_id,decision:'pass',reason:'合成审核',client_req_id:crypto.randomUUID()};
 const batch=f.DB.batch.bind(f.DB);let lost=false;f.DB.batch=async statements=>{const r=await batch(statements);if(!lost&&statements.some(s=>s.args.includes('sop_shipping_output_review'))){lost=true;throw Error('synthetic lost acknowledgement');}return r;};
 const approved=await f.call('reviewer','sop_shipping_output_review',review);passed(approved);assert.equal(lost,true);assert.deepEqual(await f.call('reviewer','sop_shipping_output_review',review),approved);assert.equal((await f.get(n.id)).shipping_output.version,1);
 const start=await f.get(n.id),newBody={id:n.id,revision:start.revision,components:fiveAndBox,reason:'再次核实'};const competing=await Promise.all([f.call('service','sop_shipping_output_propose',newBody),f.call('service','sop_shipping_output_propose',newBody)]);assert.equal(competing.filter(r=>r.ok).length,1);
});
test('independent concurrent bookings cannot oversell; review rechecks occupied quantities and concurrent revision',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need();passed(await f.propose(n.id,[{quantity:5,unit:'托'},{quantity:2,unit:'箱'}]));passed(await f.review(n.id));passed(await f.propose(n.id,fiveAndBox));
 const start=await f.get(n.id),body={id:n.id,revision:start.revision,outbounds:[f.booking(2,'箱')]};const results=await Promise.all([f.call('manager','v2_outbound_order_batch_create',body),f.call('manager','v2_outbound_order_batch_create',body)]);assert.equal(results.filter(r=>r.ok).length,1);denied(await f.review(n.id));assert.equal((await f.get(n.id)).shipping_output.components[1].quantity,2);
 const review={id:n.id,revision:start.revision,change_id:start.shipping_output_change.id,decision:'pass',reason:'旧版审核'};denied(await f.call('reviewer','sop_shipping_output_review',review));
 const n2=await f.need();passed(await f.propose(n2.id,[{quantity:5,unit:'托'},{quantity:2,unit:'箱'}]));passed(await f.review(n2.id));passed(await f.propose(n2.id,fiveAndBox));const same=await f.get(n2.id);
 const race=await Promise.all([f.call('service','sop_shipping_output_review',{id:n2.id,revision:same.revision,change_id:same.shipping_output_change.id,decision:'pass',reason:'无权并发审核'}),f.call('reviewer','sop_shipping_output_review',{id:n2.id,revision:same.revision,change_id:same.shipping_output_change.id,decision:'pass',reason:'并发审核'}),f.call('manager','v2_outbound_order_batch_create',{id:n2.id,revision:same.revision,outbounds:[f.booking(2,'箱')]})]);assert.equal(race.filter(r=>r.ok).length,1);assert.equal((await f.availability(n2.id)).by_unit.every(c=>c.used<=c.quantity),true);
});
test('closed, cancelled, direct-forward, grouped and unreviewed records cannot use whole-job correction',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need(),original=f.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(n.id).state;
 for(const patch of [{status:'closed'},{status:'cancelled'},{result:null,status:'assigned'},{operation_kind:'direct_forward'},{cargo_groups:{groups:[{id:'QA',processes:[{id:'QA-P'}]}]}}]){
  f.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify({...JSON.parse(original),...patch}),n.id);denied(await f.propose(n.id));
 }
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM sop_events WHERE action='sop_shipping_output_propose'").get().n,0);
});
test('legacy single-create and historical-link endpoints enforce the selected mixed unit and consumed capacity',async t=>{
 const f=await mixedShippingFixture(t),n=await f.need();passed(await f.propose(n.id));passed(await f.review(n.id));let current=await f.get(n.id);
 const base={customer:n.customer,biz_class:'bulk',expected_ship_at:'2026-10-20',outbound_mode:'customer_pickup',sop_existing_need_id:n.id,sop_need_revision:current.revision,sop_link_quantity:5};denied(await f.call('manager','v2_outbound_order_create',base));passed(await f.call('manager','v2_outbound_order_create',{...base,sop_link_unit:'托盘'}));
 // A synthetic independent existing shipping plan, never a live user document.
 const tstamp=new Date().toISOString();f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,customer,biz_class,status,planned_box_count,created_at,updated_at) VALUES('QA-HIST',?,'bulk','pending_issue',1,?,?)").run(n.customer,tstamp,tstamp);
 for(const status of ['cancelled','shipped','completed']){f.DB.raw.prepare('UPDATE v2_outbound_orders SET status=? WHERE id=?').run(status,'QA-HIST');denied(await f.change('service','sop_need_link',n.id,{outbound_id:'QA-HIST',quantity:1,unit:'箱'}));}
 f.DB.raw.prepare("UPDATE v2_outbound_orders SET status='pending_issue' WHERE id='QA-HIST'").run();
 f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,related_doc_type,related_doc_id) VALUES('QA-LOAD','load_outbound','working','outbound_order','QA-HIST')").run();denied(await f.change('service','sop_need_link',n.id,{outbound_id:'QA-HIST',quantity:1,unit:'箱'}));f.DB.raw.prepare("DELETE FROM v2_ops_jobs WHERE id='QA-LOAD'").run();
 denied(await f.change('service','sop_need_link',n.id,{outbound_id:'QA-HIST',quantity:1,unit:'托'}));passed(await f.change('service','sop_need_link',n.id,{outbound_id:'QA-HIST',quantity:1,unit:'箱'}));denied(await f.change('service','sop_need_link',n.id,{outbound_id:'QA-HIST',quantity:1,unit:'箱'}));assert.equal((await f.get(n.id)).status,'linked');
});
test('different needs cannot concurrently claim one historical order; status/load races roll back the need and claim',async t=>{
 const f=await mixedShippingFixture(t),a=await f.need(),b=await f.need();for(const n of [a,b]){passed(await f.propose(n.id));passed(await f.review(n.id));}
 const stamp=new Date().toISOString(),create=id=>f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,customer,biz_class,status,planned_box_count,created_at,updated_at) VALUES(?,?,'bulk','pending_issue',1,?,?)").run(id,a.customer,stamp,stamp);create('QA-SHARED');
 const race=await Promise.all([f.change('service','sop_need_link',a.id,{outbound_id:'QA-SHARED',quantity:1,unit:'箱'}),f.change('service','sop_need_link',b.id,{outbound_id:'QA-SHARED',quantity:1,unit:'箱'})]);assert.equal(race.filter(r=>r.ok).length,1);assert.equal((await f.get(a.id)).links.length+(await f.get(b.id)).links.length,1);
 const free=(await f.get(a.id)).links.length?b:a,original=f.DB.batch.bind(f.DB);
 for(const mode of ['cancel','load']){const id='QA-RACE-'+mode;create(id);const before=await f.get(free.id);let injected=false;f.DB.batch=async statements=>{if(!injected&&statements.some(s=>s.args.includes('sop_need_link_claim'))){injected=true;if(mode==='cancel')f.DB.raw.prepare("UPDATE v2_outbound_orders SET status='cancelled' WHERE id=?").run(id);else f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,status,related_doc_type,related_doc_id) VALUES(?,'load_outbound','working','outbound_order',?)").run('QA-LOAD-RACE',id);}return original(statements);};denied(await f.change('service','sop_need_link',free.id,{outbound_id:id,quantity:1,unit:'箱'}));assert.equal(injected,true);assert.deepEqual(await f.get(free.id),before);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM sop_events WHERE request_id=?').get('LINK-'+id).n,0);f.DB.batch=original;}
});
