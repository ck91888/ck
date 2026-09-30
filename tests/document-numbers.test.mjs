import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
import {database} from './d1-adapter.mjs';import worker from '../worker-v2/index.js';import {handleSop} from '../worker-v2/sop.js';
import {NUMBER_SCHEMA,ensureDocumentNumbers,recordNumbers,jobNumbers} from '../worker-v2/document-numbers.js';
const actor={id:'M',name:'Fixture manager',role:'manager',departments:['bulk']};
function setup(){const DB=database(),env={DB,SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ENVIRONMENT:'staging',SOP_PUBLIC_TEST_ACCESS:'true',SOP_REQUEST_USER:actor};
 const api=async(action,body={})=>{const result=await handleSop({action,client_req_id:crypto.randomUUID(),...body},env);assert.equal(result.ok,true,result.error);return result;};return {DB,env,api};}
const insert=(DB,id,kind,t,data={})=>DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,1,?,?,?)').run(id,kind,'bulk',JSON.stringify({created_at:t,title:'Fixture',customer:'Example',status:'pending',...data}),t);
test('historical backfill is ordered, stable and separate from revisions; Korean date and atomic creation numbering',async()=>{
 const {DB,env}=setup();DB.raw.exec('DROP TRIGGER sop_document_number_insert; DELETE FROM sop_document_numbers');
 const t='2026-09-29T16:00:00Z';for(const id of ['NEED-B','NEED-A','NEED-C'])insert(DB,id,'need',t);
 const before=DB.raw.prepare('SELECT * FROM sop_records ORDER BY id').all();await ensureDocumentNumbers(env);
 let rows=DB.raw.prepare('SELECT * FROM sop_document_numbers ORDER BY sequence').all();assert.deepEqual(rows.map(r=>r.display_no),['ZY-20260930-001','ZY-20260930-002','ZY-20260930-003']);assert.deepEqual(rows.map(r=>r.record_id),['NEED-A','NEED-B','NEED-C']);
 assert.deepEqual(DB.raw.prepare('SELECT * FROM sop_records ORDER BY id').all(),before);
 for(const sql of NUMBER_SCHEMA)DB.raw.exec(sql);assert.deepEqual(DB.raw.prepare('SELECT * FROM sop_document_numbers ORDER BY sequence').all(),rows);
 await Promise.all(Array.from({length:15},(_,i)=>DB.batch([DB.prepare('INSERT INTO sop_records VALUES(?,?,1,?,?,?)').bind('NEED-'+i,'need','bulk',JSON.stringify({created_at:t}),t)])));
 rows=DB.raw.prepare('SELECT display_no FROM sop_document_numbers').all();assert.equal(new Set(rows.map(r=>r.display_no)).size,18);
 DB.raw.exec("DELETE FROM sop_records WHERE id='NEED-A'");insert(DB,'NEED-LATE','need','2026-09-29T15:01:00Z');assert.equal(DB.raw.prepare("SELECT display_no FROM sop_document_numbers WHERE record_id='NEED-LATE'").get().display_no,'ZY-20260930-019');
 await assert.rejects(DB.batch([DB.prepare('INSERT INTO sop_records VALUES(?,?,1,?,?,?)').bind('ROLLBACK','need','bulk',JSON.stringify({created_at:t}),t),DB.prepare('INSERT INTO missing_table VALUES(1)')]));assert.equal(DB.raw.prepare("SELECT COUNT(*) n FROM sop_document_numbers WHERE record_id='ROLLBACK'").get().n,0);
});
test('one plan number is returned by detail, groups, linked plans, search and scans; inventory stays inventory',async()=>{
 const {DB,env,api}=setup();DB.raw.prepare("INSERT INTO v2_inbound_plans(id,display_no,customer,status) VALUES('IB-internal','RU-20260930-005','Example','pending')").run();
 const created=await api('sop_need_create',{department:'bulk',source_type:'inbound',source_id:'IB-internal',title:'Two pallets',customer:'Example',instructions:'Keep together',planned_quantity:2,planned_unit:'托'});
 const n=(await api('sop_get',{id:created.id})).record;assert.match(n.display_no,/^ZY-\d{8}-\d{3}$/);assert.equal(n.source_display_no,'RU-20260930-005');
 assert.equal((await api('sop_get',{id:n.display_no.toLowerCase()})).record.id,n.id);
 assert.equal((await api('sop_linked',{source_id:'IB-internal'})).items[0].display_no,n.display_no);
 const group=(await api('sop_need_groups',{need_id:n.id})).items[0];assert.equal(group.display_no,n.source_display_no);assert.equal(group.items[0].display_no,n.display_no);
 for(const search of [n.display_no,'RU-20260930-005'])assert.equal((await api('sop_work_need_search',{search})).items[0].display_no,n.display_no);
 for(const code of [n.display_no,n.id,'CKWORK|'+n.id+'|1']){const r=await api('sop_field_resolve',{code});assert.equal(r.need.display_no,n.display_no);assert.equal(r.source.number,'RU-20260930-005');}
 const stock=await api('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'0000456',title:'Stock work',customer:'Example',instructions:'Label',planned_quantity:1,planned_unit:'箱'});
 const inv=(await api('sop_get',{id:stock.id})).record;assert.equal(inv.source_type,'inventory');assert.equal(inv.source_display_no,'');assert.equal(inv.supply_chain_no,'0000456');
 await api('sop_need_update',{id:n.id,revision:n.revision,title:n.title,owner:n.owner,instructions:'Updated',reason:'customer update'});const latest=(await api('sop_get',{id:n.id})).record;assert.equal(latest.display_no,n.display_no);assert.equal((await api('sop_field_resolve',{code:'CKWORK|'+n.id+'|1'})).changed,true);
 const foreign=await handleSop({action:'sop_field_resolve',code:n.display_no},{...env,SOP_REQUEST_USER:{id:'V',role:'dispatcher',departments:['import']}});assert.equal(foreign.ok,false);
});
test('task cards, job reports, searches and exports show business numbers while retaining link IDs',async()=>{
 const {DB,env,api}=setup();DB.raw.prepare("INSERT INTO v2_inbound_plans(id,display_no,customer,status) VALUES('IB-source','RU-20260930-010','Example','pending')").run();
 const n=await api('sop_need_create',{department:'bulk',source_type:'inbound',source_id:'IB-source',title:'Pallet work',customer:'Example',instructions:'Palletize',planned_quantity:2,planned_unit:'托'});
 const need=(await api('sop_get',{id:n.id})).record;
 const task=await api('sop_task_create',{department:'bulk',title:'Pallet work',need_id:need.display_no,job_type:'bulk_op',estimated_minutes:30,workers:[{id:'W1',name:'Worker'}],lead_id:'W1'});
 const t=(await api('sop_list',{kind:'task'})).items[0];assert.equal(t.work_plan_no,need.display_no);assert.equal(t.source_display_no,'RU-20260930-010');assert.match(t.display_no,/^RW-/);
 const result=await jobNumbers(env,[{id:task.id,related_doc_id:'IB-source'}]);assert.equal(result[0].business_no,need.display_no);assert.equal(result[0].related_doc_id,'IB-source');
 const nativeApi=async(action,body={})=>{const response=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...body})}),env);const r=await response.json();assert.equal(r.ok,true,r.error);return r;};
 assert.equal((await nativeApi('v2_ops_job_detail',{job_id:task.id})).job.display_no,need.display_no);
 assert.equal((await nativeApi('v2_order_ops_job_list')).items.find(x=>x.id===task.id).display_no,need.display_no);
 assert.equal((await nativeApi('v2_dashboard_order_list',{doc_no:need.display_no})).items[0].display_no,need.display_no);
 assert.equal((await nativeApi('v2_dashboard_order_detail',{job_id:task.id})).job.business_no,need.display_no);
 assert.equal((await nativeApi('v2_dashboard_order_export',{doc_no:'RU-20260930-010'})).rows[0]['单号'],need.display_no);
 DB.raw.exec("INSERT INTO v2_ops_jobs(id,related_doc_type,related_doc_id,job_type) VALUES('JOB-external','work_order','0000123','bulk_op')");assert.equal((await jobNumbers(env,[{id:'JOB-external'}]))[0].business_no,'0000123');
 DB.raw.exec("INSERT INTO v2_ops_jobs(id,job_type,display_no) VALUES('JOB-pick','pick_direct','PK-20260930-001'); INSERT INTO v2_ops_job_pick_docs(id,job_id,pick_doc_no) VALUES('P1','JOB-pick','0000088')");
 const pick=(await jobNumbers(env,[{id:'JOB-pick'}]))[0];assert.equal(pick.business_no,'0000088');assert.equal(pick.trip_no,'PK-20260930-001');
 DB.raw.exec("INSERT INTO v2_inbound_plans(id,display_no,status,updated_at) VALUES('IB-truck-1','RU-20260930-011','pending','v1'),('IB-truck-2','RU-20260930-012','pending','v1'); INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id) VALUES('JOB-truck','unload','inbound_plan','IB-truck-1'); INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot) VALUES('JOB-truck','IB-truck-2',2,'v1','[]'),('JOB-truck','IB-truck-1',1,'v1','[]')");
 const trip=(await jobNumbers(env,[{id:'JOB-truck'}]))[0];assert.equal(trip.business_no,'RU-20260930-011、RU-20260930-012');assert.equal(trip.inbound_plan_no,trip.business_no);
});
test('single and batch print use business numbers, keep the legacy QR payload and respect selected language',()=>{
 let html='',payload='',lang='zh';const printWindow={document:{write:s=>html=s,close(){}},focus(){},print(){},close(){}};
 const qrcode=()=>({addData:s=>payload=s,make(){},createSvgTag:()=>'<svg aria-label="scan"></svg>'});qrcode.stringToBytesFuncs={'UTF-8':v=>v};
 const context={window:{open:()=>printWindow,getLang:()=>lang},qrcode,console,Date};context.window.window=context.window;vm.createContext(context);
 vm.runInContext(fs.readFileSync(new URL('../shared/document-labels.js',import.meta.url),'utf8'),context);
 const x={id:'NEED-private',display_no:'ZY-20260930-001',source_id:'IB-private',source_display_no:'RU-20260930-005',source_type:'inbound',customer:'Example',instructions:'<img onerror=alert(1)>',title:'Pallet work',requirement_version:2};
 context.window.CKNeedPrint(x);assert.match(html,/作业计划号：ZY-20260930-001/);assert.match(html,/来源入库计划：RU-20260930-005/);assert.doesNotMatch(html,/NEED-private|IB-private|공급망|供应链单号/);assert.match(html,/&lt;img/);assert.equal(payload,'CKWORK|NEED-private|2');
 lang='ko';context.window.CKNeedPrint(x);assert.match(html,/작업 계획번호：ZY-/);assert.match(html,/원본 입고 계획：RU-/);assert.doesNotMatch(html,/作业计划号/);
 lang='zh';context.window.CKNeedPrint({...x,source_type:'inventory',source_display_no:'',supply_chain_no:'00004'});assert.match(html,/货物来源：库内库存/);assert.match(html,/供应链单号：00004/);
 for(const f of ['sop-planning-ui.js','field-work.js'])vm.runInContext(fs.readFileSync(new URL('../shared/'+f,import.meta.url),'utf8'),context);
 context.window.CKNeedPrint(x);assert.match(html,/ZY-20260930-001/);assert.doesNotMatch(html,/NEED-private|IB-private/);
 context.CKWorkNeedsTable=context.window.CKWorkNeedsTable;
 context.window.CKPrintWorkGroup({source_id:'IB-private',source_type:'inbound',display_no:'RU-20260930-005',customer:'Example',items:[x,{...x,id:'NEED-private-2',display_no:'ZY-20260930-002'}]});
 assert.match(html,/RU-20260930-005/);assert.match(html,/ZY-20260930-001/);assert.match(html,/ZY-20260930-002/);assert.doesNotMatch(html,/NEED-private|IB-private/);
});

