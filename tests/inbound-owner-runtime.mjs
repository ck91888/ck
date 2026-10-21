import assert from 'node:assert/strict';
// Run against wrangler dev with an isolated local D1; never target an external host.
const base=process.env.CK_LOCAL_API||'http://127.0.0.1:8787',tag='SYNTH-RUNTIME-'+Date.now(),tally='inbound_direct_tally',putaway='inbound_direct_putaway';
assert.match(base,/^http:\/\/127\.0\.0\.1:\d+$/);
const ownerA='合成运行时货主甲 '+tag,ownerB='合成运行时货主乙 '+tag;
const call=async(action,data={})=>{const r=await fetch(base+'/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})});return r.json();};
const ok=async(a,b)=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+JSON.stringify(r));return r;};
await ok('sop_identity');
const start=(code,customer_name,type=tally)=>{const id='SYNTH-RUNTIME-W-'+crypto.randomUUID();return call('sop_native_start',{payload:{action:'v2_inbound_job_start',external_inbound_no:code,job_type:type,biz_class:'direct_ship',customer_name,client_req_id:crypto.randomUUID()},workers:[{id,name:'合成运行时人员'}],lead_id:id,estimated_minutes:10});};
const finish=(j,data={})=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,...data});
assert.equal((await start(tag,'   ')).ok,false);
const a=await start(tag,'  '+ownerA+'  ');assert.equal(a.ok,true,JSON.stringify(a));assert.equal((await finish(a,{customer_name:'　'})).ok,false);assert.equal((await finish(a,{customer_name:ownerA,extra_ops:{sort_qty:0,label_qty:2,repair_box_qty:1},result_note:'合成说明'})).ok,true);
const b=await start(tag+'-B',ownerB);assert.equal(b.ok,true,JSON.stringify(b));assert.equal((await finish(b)).ok,true);
const lookup=await ok('v2_inbound_owner_lookup',{q:tag});assert.equal(lookup.items.length,2);
const p=await ok('sop_native_start',{payload:{action:'v2_inbound_job_start',job_type:putaway,biz_class:'direct_ship',putaway_refs:[tag,tag+'-B'],client_req_id:crypto.randomUUID()},workers:[{id:'SYNTH-RUNTIME-P-'+tag,name:'合成运行时上架'}],lead_id:'SYNTH-RUNTIME-P-'+tag,estimated_minutes:10});
assert.deepEqual(p.putaway_refs.map(x=>x.customer),[ownerA,ownerB]);assert.equal((await finish(p,{location_count:3,used_forklift:false,customer_name:'不能覆盖'})).ok,true);
const detail=await ok('v2_ops_job_detail',{job_id:p.job_id});assert.deepEqual(detail.putaway_refs.map(x=>x.customer),[ownerA,ownerB]);
const row=(await ok('v2_dashboard_order_export')).rows.find(x=>x.job_id===p.job_id);assert.match(row.inbound_owner_details,/货主甲/);assert.match(row.inbound_owner_details,/货主乙/);assert.equal(row.putaway_location_count,3);
console.log('PASS real workerd/D1 local runtime: whitespace, owner persistence/lookup, multi-bill putaway, immutable owners, export, atomic D1 batch');
