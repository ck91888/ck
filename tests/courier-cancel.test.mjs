import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

const C1='301000008101',C2='301000008102';
const actor='QA courier history operator',stamp='2026-10-01T00:00:00.000Z';
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const objects=new Map();
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_USERS_JSON:JSON.stringify([{id:'QA-COURIER-HISTORY',name:actor,role:'manager',scope:'office',departments:['bulk','direct_ship','import'],key:'qa-courier-history-fixture-only'}]),R2_BUCKET:{async put(key,bytes,meta){objects.set(key,{body:await new Response(bytes).text(),httpMetadata:meta.httpMetadata});},async get(key){return objects.get(key);},async delete(){throw Error('Courier plan recreation must not delete attachment objects');}}};
 let cookie='';
 const raw=async body=>{const multi=body instanceof FormData,response=await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{Cookie:cookie,...operationHeaders(cookie),...(!multi?{'Content-Type':'application/json'}:{})},body:multi?body:JSON.stringify(body)}),env);if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];return rememberOperation(await response.json(),cookie);};
 const call=(action,data={})=>raw({action,client_req_id:crypto.randomUUID(),...data});
 const ok=async(action,data={})=>{const result=await call(action,data);assert.equal(result.ok,true,action+': '+(result.error||result.message));return result;};
 await ok('sop_login',{sop_key:'qa-courier-history-fixture-only'});await confirmOperation(call,actor);
 const createBody=(codes=[C1,C2],extra={})=>({customer:'QA synthetic courier history',biz_classes:['direct_ship'],external_inbound_no:'QA-COURIER-HISTORY-WMS',lines:[{unit_type:'courier',planned_qty:codes.length,tracking_nos:codes}],...extra});
 const create=(codes,extra)=>ok('v2_inbound_plan_create',createBody(codes,extra));
 const cancel=id=>ok('v2_inbound_plan_cancel',{inbound_plan_id:id,reason:'QA synthetic plan recreation'});
 const detail=id=>ok('v2_inbound_plan_detail',{id});
 const rows=(sql,...args)=>DB.raw.prepare(sql).all(...args);
 const graph=id=>({plan:rows('SELECT * FROM v2_inbound_plans WHERE id=?',id),lines:rows('SELECT * FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY id',id),tasks:rows('SELECT * FROM v2_inbound_plan_biz_tasks WHERE plan_id=? ORDER BY id',id),attachments:rows("SELECT * FROM v2_attachments WHERE related_doc_type='inbound_plan' AND related_doc_id=? ORDER BY id",id),audit:rows("SELECT * FROM ck_plan_operation_audit WHERE doc_type='inbound' AND doc_id=? ORDER BY rowid",id)});
 const history=id=>rows('SELECT * FROM ck_courier_plan_item_history WHERE plan_id=? ORDER BY tracking_no',id);
 const receiptGraph=()=>Object.fromEntries(['ck_courier_receipts','ck_courier_events','ck_courier_batches','ck_courier_batch_items'].map(table=>[table,rows('SELECT * FROM '+table+' ORDER BY rowid')]));
 const allGraph=()=>Object.fromEntries(['v2_inbound_plans','v2_inbound_plan_lines','v2_inbound_plan_biz_tasks','v2_attachments','ck_plan_operation_audit','ck_courier_plan_items','ck_courier_plan_item_history','ck_courier_receipts','ck_courier_events','ck_courier_batch_items','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','v2_outbound_orders','sop_records','sop_events'].map(table=>[table,rows('SELECT * FROM '+table+' ORDER BY rowid')]));
 const attach=async id=>{const body=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:id,attachment_category:'inbound_material',client_req_id:crypto.randomUUID()}))body.set(k,v);body.set('file',new File(['QA synthetic attachment,3\n'],'qa-courier-history.csv',{type:'text/csv'}));const result=await raw(body);assert.equal(result.ok,true,result.error);return result.attachment||result;};
 let batch,scanner;
 const scan=async(code,expectSuccess=true)=>{if(!batch){const attendance=(await ok('sop_attendance_checkin',{name:'QA synthetic courier receiver',agency:'가온'})).record;scanner=attendance.badgeId;batch=(await ok('sop_courier_receive',{operation:'start',workers:[{id:scanner,name:attendance.name}],lead_id:scanner})).batch;}return (expectSuccess?ok:call)('sop_courier_receive',{batch_id:batch.id,scanner_badge:scanner,tracking_no:code,owner:'8-1'});};
 return {DB,env,objects,call,ok,createBody,create,cancel,detail,rows,graph,history,receiptGraph,allGraph,attach,scan};
}
const progress=detail=>detail.plan.courier_progress;
function assertProgress(detail,received,total=2){assert.equal(progress(detail).total,total);assert.equal(progress(detail).received,received);assert.equal(progress(detail).items.length,total);assert.equal(new Set(progress(detail).items.map(x=>x.tracking_no)).size,total);}

test('unreceived recreation preserves the cancelled plan, lines, tasks, attachments and real operator audit',async t=>{
 const f=await fixture(t),old=await f.create();const attachment=await f.attach(old.id);
 await f.ok('v2_inbound_plan_update',{id:old.id,biz_classes:['direct_ship'],remark:'QA original office instructions'});await f.cancel(old.id);
 const before=f.graph(old.id),blobs=[...f.objects];assert.deepEqual(before.audit.map(x=>x.kind),['create','update','cancel']);assert.ok(before.audit.every(x=>x.actor_name===actor));
 const replacement=await f.create();assert.notEqual(replacement.id,old.id);assert.notEqual(replacement.display_no,old.display_no);
 assert.deepEqual(f.graph(old.id),before);assert.deepEqual([...f.objects],blobs);assert.ok(f.objects.has(attachment.file_key));
 assert.equal(f.rows('SELECT count(*) n FROM ck_courier_plan_items WHERE plan_id=?',old.id)[0].n,0);assert.equal(f.rows('SELECT count(*) n FROM ck_courier_plan_items WHERE plan_id=?',replacement.id)[0].n,2);
 const archived=f.history(old.id);assert.equal(archived.length,2);assert.ok(archived.every(x=>x.replacement_plan_id===replacement.id&&x.actor_name===actor&&x.actor_id==='QA-COURIER-HISTORY'&&x.released_at));assert.ok(archived.every(x=>JSON.parse(x.receipt_json).receipt_id===undefined));
 assertProgress(await f.detail(old.id),0);assertProgress(await f.detail(replacement.id),0);
 assert.deepEqual(f.graph(old.id),before,'opening archived detail must not rewrite the historical plan');
});

test('partial receipt recreation preserves receipt identity and freezes the old 1/2 snapshot while the new plan reaches 2/2',async t=>{
 const f=await fixture(t),old=await f.create(),received=await f.scan(C1);assert.equal(received.duplicate,false);
 const oldProgress=progress(await f.detail(old.id));assert.equal(oldProgress.received,1);await f.cancel(old.id);
 const oldGraph=f.graph(old.id),receiptGraph=f.receiptGraph(),replacement=await f.create();
 assert.deepEqual(f.receiptGraph(),receiptGraph,'recreation must not insert receipts, receiving events or batch memberships');assert.deepEqual(f.graph(old.id),oldGraph);
 const archived=f.history(old.id);assert.equal(archived.length,2);const snapshot=JSON.parse(archived.find(x=>x.tracking_no===C1).receipt_json);assert.equal(snapshot.receipt_id,received.item.id);assert.equal(snapshot.batch_id,received.item.batch_id);assert.equal(snapshot.batch_no,received.item.batch_no);assert.equal(snapshot.scanner_id,received.item.scanner_id);assert.equal(snapshot.received_at,received.item.received_at);
 assert.deepEqual(progress(await f.detail(old.id)),oldProgress);assertProgress(await f.detail(replacement.id),1);
 assert.equal(f.graph(replacement.id).lines[0].actual_qty,1,'receipt count must not double-count active and archived mappings');
 const newReceipt=await f.scan(C2);assert.equal(newReceipt.item.plan_id,replacement.id);assert.equal(newReceipt.duplicate,false);
 const current=await f.detail(replacement.id);assertProgress(current,2);assert.equal(current.plan.status,'arrived_pending_putaway');assert.equal(current.lines[0].actual_qty,2);
 assertProgress(await f.detail(old.id),1);assert.deepEqual(progress(await f.detail(old.id)),oldProgress);assert.deepEqual(f.graph(old.id),oldGraph);
 assert.deepEqual(f.rows('SELECT * FROM ck_courier_receipts WHERE id=?',received.item.id),receiptGraph.ck_courier_receipts);assert.deepEqual(f.rows('SELECT * FROM ck_courier_events WHERE receipt_id=? ORDER BY rowid',received.item.id),receiptGraph.ck_courier_events);assert.deepEqual(f.rows('SELECT * FROM ck_courier_batch_items WHERE receipt_id=?',received.item.id),receiptGraph.ck_courier_batch_items);
 assert.equal(f.receiptGraph().ck_courier_receipts.length,2);assert.equal(f.receiptGraph().ck_courier_events.length,2);
});

test('all received direct-ship parcels can be recreated as arrived pending putaway without duplicating receiving work',async t=>{
 const f=await fixture(t),old=await f.create();await f.scan(C1);await f.scan(C2);const arrived=await f.detail(old.id);assertProgress(arrived,2);assert.equal(arrived.plan.status,'arrived_pending_putaway');
 await f.cancel(old.id);const original=f.graph(old.id),receipts=f.receiptGraph(),replacement=await f.create();
 assert.deepEqual(f.receiptGraph(),receipts);assert.deepEqual(f.graph(old.id),original);assert.equal(f.history(old.id).length,2);
 const current=await f.detail(replacement.id);assertProgress(current,2);assert.equal(current.plan.status,'arrived_pending_putaway');assert.equal(current.lines[0].actual_qty,2);
 assert.deepEqual(progress(await f.detail(old.id)),progress(arrived));assert.equal(f.rows("SELECT count(*) n FROM v2_ops_jobs WHERE related_doc_type='inbound_plan'")[0].n,0,'recreation must not invent unloading or putaway work');
});

test('fully received bulk parcels retain completed status and cannot be cancelled for recreation',async t=>{
 const f=await fixture(t),old=await f.create([C1,C2],{biz_classes:['bulk'],external_inbound_no:''});await f.scan(C1);await f.scan(C2);const completed=await f.detail(old.id);assert.equal(completed.plan.status,'completed');assertProgress(completed,2);
 const before=f.allGraph();assert.equal((await f.call('v2_inbound_plan_cancel',{inbound_plan_id:old.id,reason:'QA must remain completed'})).ok,false);assert.equal((await f.call('v2_inbound_plan_create',f.createBody([C1,C2],{biz_classes:['bulk'],external_inbound_no:''}))).ok,false);assert.deepEqual(f.allGraph(),before);
});

test('successive recreations retain each historical snapshot and block restoration or deletion without removing attachments',async t=>{
 const f=await fixture(t);let current=await f.create();await f.scan(C1);const originals=[];
 for(let i=0;i<3;i++){
  await f.attach(current.id);const frozen=progress(await f.detail(current.id));assert.equal(frozen.received,1);await f.cancel(current.id);const before=f.graph(current.id),old=current;current=await f.create();
  originals.push({id:old.id,replacement:current.id,before,frozen});assert.equal(f.history(old.id).length,2);assert.ok(f.history(old.id).every(x=>x.replacement_plan_id===current.id));assert.deepEqual(f.graph(old.id),before);assertProgress(await f.detail(current.id),1);
 }
 await f.scan(C2);assertProgress(await f.detail(current.id),2);
 assert.equal(f.rows('SELECT count(*) n FROM ck_courier_plan_items')[0].n,2);assert.equal(f.rows('SELECT count(*) n FROM ck_courier_plan_item_history')[0].n,6);
 for(const old of originals){
  assert.deepEqual(progress(await f.detail(old.id)),old.frozen);assert.deepEqual(f.graph(old.id),old.before);
  const before=f.allGraph(),blobs=[...f.objects];assert.equal((await f.call('v2_inbound_plan_update_status',{id:old.id,status:'pending'})).ok,false);assert.equal((await f.call('v2_inbound_plan_delete',{id:old.id})).ok,false);
  assert.deepEqual(f.allGraph(),before);assert.deepEqual([...f.objects],blobs);assert.ok(old.before.attachments.every(x=>f.objects.has(x.file_key)));
 }
});

const planField=(field,value)=>(f,p)=>f.DB.raw.prepare('UPDATE v2_inbound_plans SET '+field+'=? WHERE id=?').run(value,p.id);
const lineField=(field,value)=>(f,p)=>f.DB.raw.prepare('UPDATE v2_inbound_plan_lines SET '+field+'=? WHERE plan_id=?').run(value,p.id);
function historicalJob(f,p,status){const id='QA-HISTORICAL-JOB';f.DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,status,created_at,updated_at) VALUES(?,'unload','inbound_plan',?,?,?,?)").run(id,p.id,status,stamp,stamp);return id;}
const unsafe=[
 ['accounted',planField('accounted',1)],
 ['force-completed',planField('force_completed',1)],
 ['manual completion',planField('manual_completed_at',stamp)],
 ['putaway quantity',lineField('putaway_qty',1)],
 ['courier actual quantity unsupported by receipts',lineField('actual_qty',1)],
 ['soft deletion',planField('is_deleted',1)],
 ['truck unloading milestone',(f,p)=>{f.DB.raw.prepare("INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty,actual_qty) VALUES('QA-TRUCK-LINE',?,2,'carton',3,0)").run(p.id);planField('unload_completed_at',stamp)(f,p);}],
 ['truck actual quantity',(f,p)=>f.DB.raw.prepare("INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty,actual_qty) VALUES('QA-TRUCK-LINE',?,2,'carton',3,1)").run(p.id)],
 ...['pending','working','awaiting_close','paused','completed'].map(status=>['job '+status,(f,p)=>historicalJob(f,p,status)]),
 ['cancelled job with worker history',(f,p)=>{const id=historicalJob(f,p,'cancelled');f.DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES('QA-WORK-HISTORY',?,'QA-WORKER','QA historical worker',?,?,5)").run(id,stamp,stamp);}],
 ['cancelled job with result history',(f,p)=>{const id=historicalJob(f,p,'cancelled');f.DB.raw.prepare("INSERT INTO v2_ops_job_results(id,job_id,box_count,result_json,created_at) VALUES('QA-RESULT-HISTORY',?,1,'{}',?)").run(id,stamp);}],
 ...['pending','pending_issue','cancelled','completed'].map(status=>['outbound '+status,(f,p)=>f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,source_inbound_plan_id,status,customer,created_at) VALUES('QA-OUTBOUND-HISTORY',?,?,'QA synthetic outbound',?)").run(p.id,status,stamp)]),
 ...['working','closed'].map(status=>['SOP need '+status,(f,p)=>f.DB.raw.prepare('INSERT INTO sop_records(id,kind,revision,department,state,updated_at) VALUES(?,?,?,?,?,?)').run('QA-NEED-HISTORY','need',1,'bulk',JSON.stringify({id:'QA-NEED-HISTORY',source_type:'inbound',source_id:p.id,status,customer:'QA synthetic courier history',title:'QA historical work need'}),stamp)]),
 ['completed business task',(f,p)=>f.DB.raw.prepare("UPDATE v2_inbound_plan_biz_tasks SET status='completed',completed_at=?,completion_source='job' WHERE plan_id=?").run(stamp,p.id)]
];

for(const [name,seed] of unsafe)test('legacy cancelled plan with '+name+' cannot release a waybill or create partial replacement data',async t=>{
 const f=await fixture(t),old=await f.create();await seed(f,old);
 // Construct historical state directly: the current API rightly prevents some
 // of these cancellations, but old cancelled rows still require release guards.
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(old.id);
 const before=f.allGraph(),failed=await f.call('v2_inbound_plan_create',f.createBody());assert.equal(failed.ok,false,name+': '+JSON.stringify(failed));assert.deepEqual(f.allGraph(),before);
 assert.equal(f.history(old.id).length,2);assert.ok(f.history(old.id).every(h=>h.replacement_plan_id===''));assert.equal(f.rows('SELECT count(*) n FROM ck_courier_plan_items WHERE plan_id=?',old.id)[0].n,2);
});

test('cancellation after scanner preflight permits receipt without updating the cancelled plan',async t=>{
 const f=await fixture(t),old=await f.create([C1]),batch=f.DB.batch.bind(f.DB);let injected=false,afterCancel;
 f.DB.batch=async statements=>{
  if(!injected&&statements.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_courier_receipts'))){
   injected=true;f.DB.batch=batch;await f.cancel(old.id);afterCancel=f.graph(old.id);
  }
  return batch(statements);
 };
 const scan=await f.scan(C1,false);assert.equal(injected,true,'cancel must commit between the scanner preflight and receipt batch');assert.equal(scan.ok,true,scan.error);assert.equal(scan.item.plan_id,null);
 for(const table of ['ck_courier_receipts','ck_courier_events','ck_courier_batch_items'])assert.equal(f.rows('SELECT count(*) n FROM '+table)[0].n,1,table);
 assert.deepEqual(f.graph(old.id),afterCancel);assertProgress(await f.detail(old.id),0,1);assert.equal(f.history(old.id).length,1);
});

test('a scan already in flight follows the replacement claim after cancellation and recreation',async t=>{
 const f=await fixture(t),old=await f.create([C1]),batch=f.DB.batch.bind(f.DB);let injected=false,replacement,afterCancel;
 f.DB.batch=async statements=>{
  if(!injected&&statements.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_courier_receipts'))){
   injected=true;f.DB.batch=batch;await f.cancel(old.id);afterCancel=f.graph(old.id);replacement=await f.create([C1]);
   assert.equal(f.rows('SELECT count(*) n FROM ck_courier_receipts')[0].n,0);assertProgress(await f.detail(old.id),0,1);assertProgress(await f.detail(replacement.id),0,1);
  }
  return batch(statements);
 };
 const scan=await f.scan(C1);assert.equal(injected,true,'recreation must commit between the scanner preflight and receipt batch');assert.equal(scan.duplicate,false);assert.equal(scan.item.plan_id,replacement.id);assert.equal(scan.plan.plan_id,replacement.id);
 for(const table of ['ck_courier_receipts','ck_courier_events','ck_courier_batch_items'])assert.equal(f.rows('SELECT count(*) n FROM '+table)[0].n,1,table);
 assert.deepEqual(f.graph(old.id),afterCancel);assertProgress(await f.detail(old.id),0,1);assert.equal(JSON.parse(f.history(old.id)[0].receipt_json).receipt_id,undefined);
 const current=await f.detail(replacement.id);assertProgress(current,1,1);assert.equal(current.plan.status,'arrived_pending_putaway');assert.equal(current.lines[0].actual_qty,1);
 assert.equal(f.rows('SELECT receipt_id FROM ck_courier_batch_items')[0].receipt_id,scan.item.id);assert.equal(f.rows('SELECT receipt_id FROM ck_courier_events')[0].receipt_id,scan.item.id);
});
