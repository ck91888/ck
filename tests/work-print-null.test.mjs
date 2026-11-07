// Full Worker API with synthetic data in memory or local workerd D1 only.
import test from 'node:test';import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';import {localD1} from './d1-local-runtime.mjs';
async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});const f=await ownedHomeFixture();let runtime;
 t.after(async()=>{await runtime?.close();f.DB.raw.close();});
 f.rows=async(sql,...args)=>(await f.env.DB.prepare(sql).bind(...args).all()).results;
 f.ready=async()=>{if(process.env.CK_PRINT_NULL_NATIVE_D1==='true'&&!runtime){runtime=await localD1(f.DB.raw);f.env.DB=runtime;}};
 f.get=id=>f.ok('office','sop_get',{id}).then(r=>r.record);
 f.add=async title=>{const n=await f.need(title);return f.get(n.id);};
 f.seed=async(x,after_json,at='2026-11-02T00:00:00.000Z',actor='合成历史打印员')=>{const request='SYN-NULL-'+crypto.randomUUID();await f.env.DB.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').bind(request,x.id,x.revision,'sop_need_print_request','synthetic:print-null',actor,'{}',after_json,'{}',at).run();return request;};
 f.events=id=>f.rows("SELECT * FROM sop_events WHERE action='sop_need_print_request' AND record_id=? ORDER BY created_at DESC,request_id DESC",id);
 f.snapshot=async(audit=true)=>JSON.stringify(await Promise.all(['sop_records','sop_document_numbers',...(audit?['sop_events']:[]),'v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','v2_inbound_plans','v2_inbound_plan_lines','v2_outbound_orders','v2_outbound_order_lines','v2_attachments'].map(table=>f.rows('SELECT * FROM '+table+' ORDER BY rowid'))));
 f.body=xs=>({client_req_id:crypto.randomUUID(),items:xs.map(x=>({id:x.id,revision:x.revision,document_key:x.print_status.document_key}))});
 return f;
}
test('JSON null and whitespace null histories leave detail, full list, ordinary filters and SQL group scope readable',async t=>{
 const f=await fixture(t),bad=await f.add('合成空快照'),healthy=await f.add('合成正常历史');await f.ready();
 await f.seed(bad,'null');await f.seed(bad,' \n null \t','2026-11-02T01:00:00.000Z','合成最近请求人');const before=await f.snapshot();
 const x=await f.get(bad.id);assert.equal(x.print_status.state,'reprint');assert.equal(x.print_status.request_count,2);assert.equal(x.print_status.current_count,0);assert.equal(x.print_status.last_by,'合成最近请求人');assert.equal(x.print_status.last_at,'2026-11-02T01:00:00.000Z');assert.equal(x.print_status.last_requirement_version,undefined);assert.equal(x.revision,bad.revision);assert.equal(x.status,bad.status);
 const list=await f.ok('office','sop_need_groups',{limit:1});assert.equal(list.total,2);assert.equal(list.items.length,1);
 const filtered=await f.ok('office','sop_need_groups',{reference:healthy.supply_chain_no,limit:1,offset:9999});assert.equal(filtered.total,1);assert.equal(filtered.offset,0);assert.equal(filtered.items[0].items[0].id,healthy.id);assert.equal(filtered.items[0].print_status.state,'unknown');
 const empty=await f.ok('office','sop_need_groups',{customer:'SYN-ABSENT-CUSTOMER'});assert.equal(empty.total,0);
 const group=await f.ok('office','sop_need_groups',{need_id:bad.id});assert.equal(group.items[0].print_status.state,'reprint');assert.equal((await f.get(healthy.id)).print_status.state,'unknown');
 assert.equal(await f.snapshot(),before);assert.deepEqual((await f.events(bad.id)).map(e=>e.after_json),[' \n null \t','null']);
});
test('latest null keeps older valid current matches and latest actor/time without inventing a requirement version',async t=>{
 const f=await fixture(t),x=await f.add('合成最近空快照');await f.ready();
 await f.seed(x,JSON.stringify({document_key:x.print_status.document_key,requirement_version:1}));await f.seed(x,'null','2026-11-02T01:00:00.000Z','合成最近空快照人');const before=await f.snapshot();
 const current=await f.get(x.id);assert.equal(current.print_status.state,'requested');assert.equal(current.print_status.request_count,2);assert.equal(current.print_status.current_count,1);assert.equal(current.print_status.last_by,'合成最近空快照人');assert.equal(current.print_status.last_requirement_version,undefined);
 const group=(await f.ok('office','sop_need_groups',{need_id:x.id})).items[0];assert.equal(group.print_status.state,'requested');assert.equal(group.print_status.requested,1);assert.equal(await f.snapshot(),before);
});
test('older null does not poison a newer valid request or lose current-version counts',async t=>{
 const f=await fixture(t),x=await f.add('合成旧空快照');await f.ready();await f.seed(x,'null');await f.seed(x,JSON.stringify({document_key:x.print_status.document_key,requirement_version:1}),'2026-11-02T01:00:00.000Z','合成合法最近打印人');const before=await f.snapshot();
 const current=await f.get(x.id);assert.equal(current.print_status.state,'requested');assert.equal(current.print_status.request_count,2);assert.equal(current.print_status.current_count,1);assert.equal(current.print_status.last_by,'合成合法最近打印人');assert.equal(current.print_status.last_requirement_version,1);
 assert.equal((await f.ok('office','sop_need_groups',{need_id:x.id})).items[0].print_status.state,'requested');assert.equal(await f.snapshot(),before);
});
test('stale valid version mixed with null stays reprint until the current version is actually requested',async t=>{
 const f=await fixture(t),old=await f.add('合成要求变更');await f.ok('office','sop_need_update',{id:old.id,revision:old.revision,instructions:'合成第二版要求',owner:'合成经办人'});const current=await f.get(old.id);assert.notEqual(current.print_status.document_key,old.print_status.document_key);await f.ready();
 await f.seed(current,JSON.stringify({document_key:old.print_status.document_key,requirement_version:old.requirement_version||1}));await f.seed(current,'null','2026-11-02T01:00:00.000Z');const before=await f.snapshot();
 const x=await f.get(current.id);assert.equal(x.print_status.state,'reprint');assert.equal(x.print_status.current_count,0);assert.equal((await f.ok('office','sop_need_groups',{need_id:x.id})).items[0].print_status.state,'reprint');assert.equal(await f.snapshot(),before);
 const original=await f.events(x.id),business=await f.snapshot(false),body=f.body([x]),saved=await f.ok('office','sop_need_print_request',body);assert.deepEqual(await f.ok('office','sop_need_print_request',body),saved);
 const after=await f.get(x.id);assert.equal(after.print_status.state,'requested');assert.equal(after.print_status.request_count,3);assert.equal(after.print_status.current_count,1);assert.equal(await f.snapshot(false),business);const events=await f.events(x.id);assert.equal(events.length,3);for(const e of original)assert.deepEqual(events.find(n=>n.request_id===e.request_id),e);
});
test('real limited badge scope still hides and rejects the bulk order with null history',async t=>{
 const f=await fixture(t),x=await f.add('合成部门隔离'),p=(await f.ok('office','sop_attendance_register',{name:'合成限直发核查员',agency:'가온'})).person;
 await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['direct_ship'],version:0,reason:'合成空快照权限核查'});const login=await f.ok('A','sop_login',{badge:p.badgeId});assert.deepEqual(login.user.departments,['direct_ship']);await f.ready();await f.seed(x,'null');const before=await f.snapshot();
 const list=await f.ok('A','sop_need_groups',{});assert.equal(list.items.length,0);assert.equal(list.total,0);const detail=await f.call('A','sop_get',{id:x.id});assert.equal(detail.ok,false);assert.doesNotMatch(detail.error,/Cannot read|TypeError/);
 const printed=await f.call('A','sop_need_print_request',f.body([x]));assert.equal(printed.ok,false);assert.match(printed.error,/权限/);assert.equal((await f.get(x.id)).print_status.state,'reprint');assert.equal(await f.snapshot(),before);
});
test('concurrent normal print retries append one object snapshot, preserve null audit and reject changed content',async t=>{
 const f=await fixture(t),x=await f.add('合成重复打印'),other=await f.add('合成另一作业单');await f.ready();await f.seed(x,'null');const original=await f.events(x.id),before=await f.snapshot(false),body=f.body([x]);
 const [a,b]=await Promise.all([f.ok('office','sop_need_print_request',body),f.ok('office','sop_need_print_request',body)]);assert.deepEqual(a,b);const events=await f.events(x.id);assert.equal(events.length,2);assert.deepEqual(events.find(e=>e.request_id===original[0].request_id),original[0]);assert.equal(JSON.parse(events.find(e=>e.request_id===body.client_req_id).after_json).document_key,x.print_status.document_key);
 const saved=await f.snapshot();const conflict=await f.call('office','sop_need_print_request',{...f.body([other]),client_req_id:body.client_req_id});assert.equal(conflict.ok,false);assert.match(conflict.error,/其他内容/);assert.equal(await f.snapshot(),saved);assert.equal(await f.snapshot(false),before);
 const read=await f.get(x.id);assert.equal(read.print_status.state,'requested');assert.equal(read.print_status.current_count,1);assert.equal(read.print_status.request_count,2);
});
