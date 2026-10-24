import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
const tally='inbound_direct_tally',putaway='inbound_direct_putaway';
function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+JSON.stringify(r));return r;};
 const start=(code,customer_name='合成货主甲',type=tally)=>{const who='SYNTH-'+crypto.randomUUID();return call('sop_native_start',{payload:{action:'v2_inbound_job_start',job_type:type,biz_class:'direct_ship',external_inbound_no:code,customer_name,client_req_id:crypto.randomUUID()},workers:[{id:who,name:'合成人员'}],lead_id:who,estimated_minutes:10});};
 const finish=(j,data={})=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,...(j.putaway_refs?{location_count:2,used_forklift:false}:{}),...data});
 const plan=id=>DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(id),job=id=>DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(id);
 const source=fs.readFileSync(new URL('../worker-v2/index.js',import.meta.url),'utf8');const migrations=vm.runInNewContext(source.slice(source.indexOf('const MIGRATIONS ='),source.indexOf('// 每次发布迁移变化时'))+'\nMIGRATIONS');
 const legacyEmpty=id=>{DB.raw.exec('DROP TRIGGER ck_independent_inbound_identity');DB.raw.prepare("UPDATE v2_inbound_plans SET customer='' WHERE id=?").run(id);for(const sql of migrations.filter(sql=>/^(?:DROP|CREATE) TRIGGER.*ck_independent_inbound_identity/.test(sql)))DB.raw.exec(sql);};
 return {DB,env,call,ok,start,finish,plan,job,migrations,legacyEmpty};
}
test('independent creation requires trimmed owner; failed requests create no session, crew or audit',async t=>{
 const f=fixture(t);for(const value of ['','   ','\t\n','\u3000\u00a0',42,'x'.repeat(121),'合成\u0000货主'])assert.equal((await f.start('SYNTH-EMPTY',value)).ok,false,JSON.stringify(value));
 // Explicitly omit customer_name as well.
 assert.equal((await f.call('sop_native_start',{payload:{action:'v2_inbound_job_start',job_type:tally,biz_class:'direct_ship',external_inbound_no:'SYNTH-NONE',client_req_id:crypto.randomUUID()},workers:[{id:'SYNTH-W',name:'合成'}],lead_id:'SYNTH-W',estimated_minutes:10})).ok,false);
 for(const table of ['v2_inbound_plans','v2_ops_jobs','v2_ops_job_workers','sop_events'])assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM '+table).get().n,0);
});
test('finish rejects explicit blank and saves owner with unchanged quantity fields, results and putaway link',async t=>{
 const f=fixture(t),j=await f.start('SYNTH-OWNER','  合成货主甲 김가나  ');assert.equal(j.ok,true);assert.equal(f.plan(j.plan_id).customer,'合成货主甲 김가나');
 for(const customer_name of ['', '   ','\u3000','x'.repeat(121)]){assert.equal((await f.finish(j,{customer_name})).ok,false);assert.equal(f.job(j.job_id).status,'working');assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);}
 const extra_ops={sort_qty:0,label_qty:3,repair_box_qty:2,other_op_remark:'合成说明'};
 assert.equal((await f.finish(j,{customer_name:'  合成货主乙  ',extra_ops,result_note:'合成理货说明',remark:'合成备注',result_lines:[]})).ok,true);
 assert.equal(f.plan(j.plan_id).customer,'合成货主乙');const d=await f.ok('v2_inbound_plan_detail',{id:j.plan_id});assert.equal(d.plan.customer,'合成货主乙');
 const saved=JSON.parse(f.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(j.job_id).result_json);assert.deepEqual(saved.extra_ops,extra_ops);assert.equal(saved.result_note,'合成理货说明');assert.equal(saved.owner_recorded.customer,'合成货主乙');assert.equal(saved.inbound_owners[0].customer,'合成货主乙');
 const candidate=(await f.ok('v2_inbound_plan_ops_candidates',{scene:'putaway',biz_class:'direct_ship'})).items.find(x=>x.id===j.plan_id);assert.equal(candidate.customer,'合成货主乙');
 const p=await f.start('SYNTH-OWNER','伪造覆盖',putaway);assert.equal(p.ok,true);assert.equal(p.putaway_refs[0].customer,'合成货主乙');assert.equal((await f.finish(p,{customer_name:'伪造覆盖'})).ok,true);assert.equal(f.plan(j.plan_id).customer,'合成货主乙');
 assert.equal((await f.finish(j,{customer_name:'重试覆盖'})).already_completed,true);assert.equal(f.plan(j.plan_id).customer,'合成货主乙');assert.throws(()=>f.DB.raw.prepare("UPDATE v2_inbound_plans SET customer='覆盖' WHERE id=?").run(j.plan_id),/identity/);
 const exported=(await f.ok('v2_dashboard_order_export')).rows;for(const row of exported.filter(r=>[j.job_id,p.job_id].includes(r.job_id))){assert.equal(row.customer,'合成货主乙');assert.match(row.inbound_owner_details,/SYNTH-OWNER：合成货主乙/);}
});
test('external aliases of registered system plans reuse immutable owner without repeated entry',async t=>{
 const f=fixture(t),p=await f.ok('v2_inbound_plan_create',{customer:'系统合成货主',external_inbound_no:'9998800033',biz_classes:['direct_ship'],lines:[{unit_type:'carton',planned_qty:5}]});
 const u=await f.ok('v2_unload_job_start',{plan_id:p.id,worker_id:'SYNTH-U'});await f.ok('v2_unload_job_finish',{job_id:u.job_id,worker_id:'SYNTH-U',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:5}]});
 for(const code of [p.id,p.display_no,'9998800033']){const r=await f.ok('v2_inbound_resolve_code',{code,biz_class:'direct_ship',job_type:tally});assert.equal(r.kind,'system');assert.equal(r.plan.customer,'系统合成货主');}
 const j=await f.start('9998800033','恶意覆盖');assert.equal(j.ok,true);assert.equal(j.plan_id,p.id);assert.equal((await f.finish(j,{customer_name:'   ',result_lines:[{unit_type:'carton',putaway_qty:5}]})).ok,true);assert.equal(f.plan(p.id).customer,'系统合成货主');
});
test('ongoing pre-upgrade independent tally must backfill owner at finish',async t=>{
 const f=fixture(t),j=await f.start('SYNTH-OLD-ACTIVE');f.legacyEmpty(j.plan_id);const before=f.job(j.job_id);
 assert.equal((await f.finish(j)).ok,false);assert.deepEqual(f.job(j.job_id),before);assert.equal(f.plan(j.plan_id).customer,'');
 assert.equal((await f.finish(j,{customer_name:'  合成旧任务补填  '})).ok,true);assert.equal(f.plan(j.plan_id).customer,'合成旧任务补填');
});
test('completed historical empty owners remain empty, compatible with retries and putaway',async t=>{
 const f=fixture(t),j=await f.start('SYNTH-OLD-DONE');assert.equal((await f.finish(j)).ok,true);f.legacyEmpty(j.plan_id);
 assert.equal((await f.finish(j,{customer_name:'不能猜测补填'})).already_completed,true);assert.equal(f.plan(j.plan_id).customer,'');
 const p=await f.start('SYNTH-OLD-DONE',undefined,putaway);assert.equal(p.ok,true);assert.equal(p.putaway_refs[0].customer,'');assert.equal((await f.finish(p)).ok,true);assert.equal(f.plan(j.plan_id).customer,'');
 const rows=(await f.ok('v2_dashboard_order_export')).rows;assert.match(rows.find(r=>r.job_id===p.job_id).inbound_owner_details,/未记录/);assert.equal((await f.ok('v2_inbound_plan_detail',{id:j.plan_id})).plan.customer,'');
});
test('multi putaway preserves each original bill and owner in task, result and export',async t=>{
 const f=fixture(t),a=await f.start('SYNTH-MULTI-A','合成货主甲'),b=await f.start('SYNTH-MULTI-B','合成货主乙');for(const j of [a,b])assert.equal((await f.finish(j)).ok,true);
 const p=await f.ok('sop_native_start',{payload:{action:'v2_inbound_job_start',job_type:putaway,biz_class:'direct_ship',putaway_refs:['SYNTH-MULTI-A','SYNTH-MULTI-B'],client_req_id:crypto.randomUUID()},workers:[{id:'SYNTH-MULTI-W',name:'合成共同上架'}],lead_id:'SYNTH-MULTI-W',estimated_minutes:10});
 assert.deepEqual(p.putaway_refs.map(x=>x.customer),['合成货主甲','合成货主乙']);
 const listed=(await f.ok('sop_dispatch_list')).items.find(x=>x.id===p.job_id);assert.deepEqual(listed.putaway_refs.map(x=>x.customer),['合成货主甲','合成货主乙']);assert.equal(listed.customer,'合成货主甲、合成货主乙');
 assert.equal((await f.finish(p,{customer_name:'不能覆盖全部'})).ok,true);assert.deepEqual([f.plan(a.plan_id).customer,f.plan(b.plan_id).customer],['合成货主甲','合成货主乙']);
 const d=await f.ok('v2_ops_job_detail',{job_id:p.job_id});assert.deepEqual(d.putaway_refs.map(x=>x.customer),['合成货主甲','合成货主乙']);assert.deepEqual(JSON.parse(d.results[0].result_json).inbound_owners.map(x=>x.customer),['合成货主甲','合成货主乙']);
 const rows=(await f.ok('v2_dashboard_order_export')).rows.filter(r=>r.job_id===p.job_id);assert.equal(rows.length,1);assert.equal(rows[0].putaway_location_count,2);assert.equal(rows[0].inbound_owner_details,'SYNTH-MULTI-A：合成货主甲；SYNTH-MULTI-B：合成货主乙');
});
test('owner persistence failure rolls back completion, clocks and results; retry remains safe',async t=>{
 const f=fixture(t),j=await f.start('SYNTH-ROLLBACK-OWNER'),before=f.job(j.job_id);
 f.DB.raw.exec("CREATE TRIGGER synth_owner_failure BEFORE UPDATE OF customer ON v2_inbound_plans BEGIN SELECT RAISE(ABORT,'synthetic owner failure'); END");
 assert.equal((await f.finish(j,{customer_name:'合成新货主'})).ok,false);assert.deepEqual(f.job(j.job_id),before);assert.equal(f.plan(j.plan_id).customer,'合成货主甲');assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(j.job_id).n,1);
 f.DB.raw.exec('DROP TRIGGER synth_owner_failure');assert.equal((await f.finish(j,{customer_name:'合成新货主'})).ok,true);
});
test('concurrent completions with different owners preserve the winning result and owner',async t=>{
 const f=fixture(t),j=await f.start('SYNTH-OWNER-RACE');const r=await Promise.all(['合成甲','合成乙'].map(customer_name=>f.finish(j,{customer_name})));assert.ok(r.every(x=>x.ok));
 const rows=f.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').all(j.job_id);assert.equal(rows.length,1);assert.equal(JSON.parse(rows[0].result_json).owner_recorded.customer,f.plan(j.plan_id).customer);
});
test('owner lookup returns existing registered names, supports search and cannot create master data',async t=>{
 const f=fixture(t);await f.ok('v2_inbound_plan_create',{customer:'合成 검색 화주',biz_classes:['direct_ship']});const before=f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n;
 assert.deepEqual((await f.ok('v2_inbound_owner_lookup',{q:'검색'})).items.map(x=>x.customer),['合成 검색 화주']);assert.deepEqual((await f.ok('v2_inbound_owner_lookup',{q:"' OR 1=1 --"})).items,[]);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,before);
});
