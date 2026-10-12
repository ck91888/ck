// Actual Worker APIs, a controlled KST clock, and synthetic SQLite :memory: only.
import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {CREW_BORROW_SCHEMA} from '../worker-v2/crew-borrow.js';
import {activeAttendanceSQL} from '../worker-v2/attendance-management.js';

const NativeDate=Date,day='2026-10-12';
const instant=time=>new NativeDate(`${day}T${time}:00+09:00`).toISOString();

function fixture(t){
  let clock=NativeDate.parse(instant('09:00'));
  globalThis.Date=class extends NativeDate{
    constructor(...args){super(...(args.length?args:[clock]));}
    static now(){return clock;}
  };
  const DB=database();
  t.after(()=>{DB.raw.close();globalThis.Date=NativeDate;});
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
  const at=time=>{clock=NativeDate.parse(instant(time));};
  const call=async(action,data={})=>(await worker.fetch(new Request('https://daily-badge.fixture.invalid/api',{
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data}),
  }),env)).json();
  const ok=async(action,data={})=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+JSON.stringify(r));return r;};
  const daily=async(name='Synthetic daily picker')=>(await ok('sop_attendance_checkin',{name,agency:'가온'})).record;
  const crew=(...records)=>records.map(r=>({id:r.badgeId,name:r.name}));
  const request=(workers,{code='SYNTHETIC-BADGE-PICK',lead=workers[0].id,client=crypto.randomUUID()}={})=>({
    payload:{action:'v2_pick_job_start_by_docs',pick_doc_nos:[code],client_req_id:client},workers,lead_id:lead,estimated_minutes:30,
  });
  const rows=table=>DB.raw.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
  const segments=job=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=? ORDER BY joined_at,id').all(job);
  const docs=job=>DB.raw.prepare('SELECT * FROM v2_pick_worker_docs WHERE job_id=? ORDER BY rowid').all(job);
  const state=job=>JSON.parse(DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(job).state);
  const snapshot=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','v2_ops_job_pick_docs','v2_pick_worker_docs','v2_ops_job_results','sop_records','sop_events','v2_idempotency_keys'].map(table=>[table,rows(table)]));
  // Model rows written before normalization, without changing the attendance identity.
  // Include cached signatures so retries exercise compatibility with a real old request.
  const historicalLower=(job,badge)=>{
    const lower=badge.toLowerCase();
    for(const table of ['v2_ops_job_workers','v2_pick_worker_docs'])DB.raw.prepare(`UPDATE ${table} SET worker_id=? WHERE job_id=? AND worker_id=?`).run(lower,job,badge);
    DB.raw.prepare('UPDATE v2_ops_jobs SET created_by=? WHERE id=? AND created_by=?').run(lower,job,badge);
    const record=DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(job);
    if(record)DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(record.state.replaceAll(badge,lower),job);
    for(const row of rows('v2_idempotency_keys'))if(row.response_json.includes(badge))DB.raw.prepare('UPDATE v2_idempotency_keys SET response_json=? WHERE idem_key=?').run(row.response_json.replaceAll(badge,lower),row.idem_key);
    return lower;
  };
  return {DB,env,at,call,ok,daily,crew,request,rows,segments,docs,state,snapshot,historicalLower};
}

test('a valid lowercase daily badge without attendance cannot bypass native pick admission',async t=>{
  const s=fixture(t),before=s.snapshot();
  const denied=await s.call('sop_native_start',s.request([{id:'da-20261012-deadbeef',name:'Synthetic unchecked picker'}]));
  assert.equal(denied.ok,false,'a lowercase DA badge must require the same attendance as uppercase');
  assert.match(denied.error,/签到|attendance|출근/i);assert.deepEqual(s.snapshot(),before);
});

for(const prefix of ['daf','emp'])test(`an unregistered lowercase ${prefix}- prefix is managed and cannot bypass attendance`,async t=>{
  const s=fixture(t),before=s.snapshot();
  const denied=await s.call('sop_native_start',s.request([{id:prefix+'-unregistered-synthetic',name:'Synthetic unregistered picker'}]));
  assert.equal(denied.ok,false,'managed prefixes require attendance even when the full spelling stays exact');
  assert.match(denied.error,/签到|登记|attendance|출근|명찰/i);assert.deepEqual(s.snapshot(),before);
});

test('a checked-in lowercase DA request stores one canonical native pick identity and reports its minutes',async t=>{
  const s=fixture(t),p=await s.daily();s.at('09:10');
  const j=await s.ok('sop_native_start',s.request([{id:p.badgeId.toLowerCase(),name:p.name}]));
  assert.equal(s.segments(j.job_id).length,1);assert.equal(s.segments(j.job_id)[0].worker_id,p.badgeId);
  assert.equal(s.docs(j.job_id)[0].worker_id,p.badgeId);assert.equal(s.state(j.job_id).lead_id,p.badgeId);
  assert.deepEqual(s.state(j.job_id).workers,s.crew(p));
  s.at('10:00');const report=await s.ok('sop_attendance_summary',{date:day});
  assert.equal(report.items.length,1);const item=report.items[0];
  assert.equal(item.record.id,p.id);assert.equal(item.status,'working');assert.equal(item.totals.direct_ship,50);
  assert.equal(item.segments.length,1);assert.deepEqual(item.currentJobs.map(x=>x.id),[j.job_id]);
});

test('the same daily badge in two case spellings is rejected as a duplicate native crew member',async t=>{
  const s=fixture(t),p=await s.daily(),before=s.snapshot();
  const denied=await s.call('sop_native_start',s.request([...s.crew(p),{id:p.badgeId.toLowerCase(),name:p.name}]));
  assert.equal(denied.ok,false);assert.deepEqual(s.snapshot(),before);
});

test('historic lowercase pick segments survive crew retention and lead switching, then finish once',async t=>{
  const s=fixture(t),a=await s.daily('Synthetic picker A'),b=await s.daily('Synthetic picker B');s.at('09:10');
  const j=await s.ok('sop_native_start',s.request(s.crew(a,b)));s.historicalLower(j.job_id,a.badgeId);
  const segments=s.segments(j.job_id),docs=s.docs(j.job_id);s.at('09:20');
  const change={job_id:j.job_id,revision:1,workers:s.crew(a,b),lead_id:b.badgeId,client_req_id:'synthetic-case-people'};
  await s.ok('sop_native_people',change);assert.deepEqual(s.segments(j.job_id),segments,'retaining a badge in another case must not close/reopen its segment');
  assert.deepEqual(s.docs(j.job_id),docs);assert.equal(s.state(j.job_id).lead_id,b.badgeId);assert.equal(s.state(j.job_id).workers.length,2);
  await s.ok('sop_native_people',change);assert.deepEqual(s.segments(j.job_id),segments);
  await s.ok('sop_native_people',{job_id:j.job_id,revision:2,workers:s.crew(a,b),lead_id:a.badgeId.toLowerCase()});
  assert.equal(s.state(j.job_id).lead_id,a.badgeId);assert.deepEqual(s.segments(j.job_id),segments);
  s.at('10:00');const finished=await s.ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:a.badgeId});
  assert.equal(finished.worker_count,2);assert.equal(finished.total_minutes,100);assert.equal(s.segments(j.job_id).length,2);
  assert.ok(s.segments(j.job_id).every(x=>x.left_at===instant('10:00')&&x.minutes_worked===50));
  assert.ok(s.docs(j.job_id).every(x=>x.status==='completed'));assert.equal(s.rows('v2_ops_job_results').length,1);
});

test('resuming a paused historical lowercase picker creates one new segment and counts one person',async t=>{
  const s=fixture(t),p=await s.daily();s.at('09:10');
  const j=await s.ok('sop_native_start',s.request(s.crew(p)));s.historicalLower(j.job_id,p.badgeId);
  s.at('09:20');await s.ok('sop_native_pause',{job_id:j.job_id,revision:1,reason:'Synthetic equipment pause'});
  const closed=s.segments(j.job_id)[0];assert.equal(closed.minutes_worked,10);
  s.at('09:30');await s.ok('sop_native_resume',{job_id:j.job_id,revision:2,workers:s.crew(p),lead_id:p.badgeId});
  assert.equal(s.state(j.job_id).workers.length,1);assert.equal(s.segments(j.job_id).length,2);assert.deepEqual(s.segments(j.job_id)[0],closed);
  assert.equal(s.segments(j.job_id)[1].worker_id,p.badgeId);assert.equal(s.segments(j.job_id)[1].left_at,'');
  s.at('10:00');const finished=await s.ok('v2_pick_job_finalize',{job_id:j.job_id,worker_id:p.badgeId});
  assert.equal(finished.worker_count,1,'old lowercase history and new canonical segment are one person');assert.equal(finished.total_minutes,40);
});

for(const historical of [false,true])test(`${historical?'historical lowercase':'canonical uppercase'} active work blocks a case-changed second native task`,async t=>{
  const s=fixture(t),p=await s.daily();s.at('09:10');
  const j=await s.ok('sop_native_start',s.request(s.crew(p)));if(historical)s.historicalLower(j.job_id,p.badgeId);
  const before=s.snapshot(),id=historical?p.badgeId:p.badgeId.toLowerCase();
  const denied=await s.call('sop_native_start',s.request([{id,name:p.name}],{code:'SYNTHETIC-SECOND-PICK'}));
  assert.equal(denied.ok,false,'case changes cannot allocate one person to two active jobs');assert.deepEqual(s.snapshot(),before);
});

test('an old lowercase native request retries with its original key without rewriting work or making another job',async t=>{
  const s=fixture(t),p=await s.daily();s.at('09:10');
  const request=s.request(s.crew(p),{client:'synthetic-old-lower-retry'}),j=await s.ok('sop_native_start',request);
  s.historicalLower(j.job_id,p.badgeId);const before=s.snapshot();
  const retried=await s.ok('sop_native_start',{...request,workers:[{id:p.badgeId.toLowerCase(),name:p.name}],lead_id:p.badgeId.toLowerCase()});
  assert.equal(retried.job_id,j.job_id);assert.deepEqual(s.snapshot(),before);
  const changed=await s.call('sop_native_start',{...request,estimated_minutes:31});
  assert.equal(changed.ok,false,'case compatibility must not erase genuine request changes');assert.deepEqual(s.snapshot(),before);
});

for(const kind of ['DAF','EMP'])test(`${kind} custom badge casing stays exact and cannot borrow another spelling's attendance`,async t=>{
  const s=fixture(t);let person;
  if(kind==='DAF')person=(await s.ok('sop_attendance_register',{badge:'DAF-CaseSensitive',name:'Synthetic fixed case picker',agency:'가온'})).person;
  else person=(await s.ok('sop_attendance_employee_register',{employeeNo:'CASE-SENSITIVE',name:'Synthetic employee case picker',department:'direct_ship'})).person;
  const original=(await s.ok('sop_attendance_checkin',{badge:person.badgeId,agency:'가온'})).record;
  const badge=kind+'-CaseSensitive';
  // Existing custom employee IDs can contain case-significant suffixes. The
  // synthetic fixture preserves the person/profile IDs and changes only spelling.
  if(original.badgeId!==badge){
    s.DB.raw.prepare('UPDATE ck_attendance_people SET badge_id=? WHERE id=?').run(badge,original.personId);
    s.DB.raw.prepare('UPDATE ck_attendance_days SET worker_id=? WHERE id=?').run(badge,original.id);
  }
  s.at('09:10');const before=s.snapshot();
  const denied=await s.call('sop_native_start',s.request([{id:badge.toUpperCase(),name:original.name}]));
  assert.equal(denied.ok,false,'DAF/EMP case variants must not merge');assert.deepEqual(s.snapshot(),before);
  const j=await s.ok('sop_native_start',s.request([{id:badge,name:original.name}]));
  assert.equal(s.segments(j.job_id)[0].worker_id,badge);assert.equal(s.state(j.job_id).lead_id,badge);
  assert.equal(s.docs(j.job_id)[0].worker_id,badge);assert.equal(s.rows('ck_attendance_days').length,1);
});

test('legacy individual scan canonicalizes a daily badge and finishing resolves its historic lowercase segment',async t=>{
  const s=fixture(t),p=await s.daily(),codes=['SYNTHETIC-LEGACY-BADGE'];
  const j=await s.ok('v2_pick_job_start',{pick_doc_nos:codes,worker_id:p.badgeId,worker_name:p.name});
  s.at('09:10');const joined=await s.ok('v2_pick_job_start_by_docs',{pick_doc_nos:codes,worker_id:p.badgeId.toLowerCase(),worker_name:p.name});
  assert.equal(joined.job_id,j.job_id);assert.equal(s.segments(j.job_id).length,1);assert.equal(s.segments(j.job_id)[0].worker_id,p.badgeId);
  s.historicalLower(j.job_id,p.badgeId);const original=s.segments(j.job_id)[0];
  s.at('10:00');await s.ok('v2_pick_job_finish',{job_id:j.job_id,worker_id:p.badgeId});
  const closed=s.segments(j.job_id)[0];assert.equal(closed.id,original.id);assert.equal(closed.worker_id,original.worker_id);
  assert.equal(closed.left_at,instant('10:00'));assert.equal(closed.minutes_worked,50);assert.equal(s.segments(j.job_id).length,1);
  assert.ok(s.docs(j.job_id).every(x=>x.status==='completed'&&x.minutes_worked===50));
  assert.equal(s.rows('v2_ops_job_results').length,1);assert.equal(JSON.parse(s.rows('v2_ops_job_results')[0].result_json).segment_id,original.id);
});

for(const operation of ['retain','switch lead','resume','legacy scan'])test(`historic lowercase work without attendance cannot ${operation} or extend its original segment`,async t=>{
  const s=fixture(t),workers=[
    {id:'da-20261012-deadbeef',name:'Synthetic historical unchecked A'},
    {id:'da-20261012-cafe1234',name:'Synthetic historical unchecked B'},
  ];
  // Reproduce old writes through the actual Worker with attendance integration
  // disabled only for fixture seeding. All tested operations run with it enabled.
  s.env.SOP_ATTENDANCE_ENABLED='false';s.at('09:10');
  const request=s.request(workers),j=await s.ok('sop_native_start',request);
  assert.deepEqual(s.segments(j.job_id).map(x=>x.worker_id).sort(),workers.map(x=>x.id).sort());
  s.env.SOP_ATTENDANCE_ENABLED='true';s.at('10:00');
  if(operation==='resume'){
    // A paused historical remnant must not become eligible merely because its
    // job status permits a resume. Preserve both physical segments unchanged.
    const state=s.state(j.job_id);state.status='paused';
    s.DB.raw.prepare("UPDATE v2_ops_jobs SET status='paused' WHERE id=?").run(j.job_id);
    s.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(state),j.job_id);
  }
  const before=s.snapshot();let denied;
  if(operation==='legacy scan')denied=await s.call('v2_pick_job_start_by_docs',{
    pick_doc_nos:request.payload.pick_doc_nos,worker_id:workers[0].id,worker_name:workers[0].name,
  });
  else denied=await s.call(operation==='resume'?'sop_native_resume':'sop_native_people',{
    job_id:j.job_id,revision:1,workers,lead_id:workers[operation==='switch lead'?1:0].id,
  });
  assert.equal(denied.ok,false,'an old unverified work row is not proof of attendance');
  assert.match(denied.error,/签到|attendance|출근/i);assert.deepEqual(s.snapshot(),before);
});

test('upgraded crew guards lend a historic lowercase picker and return a canonical segment from an old lowercase loan',async t=>{
  const s=fixture(t);
  for(const sql of CREW_BORROW_SCHEMA.filter(sql=>/^CREATE (?:TABLE|(?:UNIQUE )?INDEX)/.test(sql)))s.DB.raw.exec(sql);
  // These are the five deployed guards before daily-badge alias support. The
  // first real Worker request must upgrade them despite their existing names.
  const oldGuards=[
    `CREATE TRIGGER ck_crew_source_guard BEFORE INSERT ON ck_crew_borrows BEGIN
     SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id WHERE w.id=NEW.source_segment_id AND w.worker_id=NEW.worker_id AND w.job_id=NEW.source_job_id AND w.left_at='' AND j.status IN ('working','awaiting_close')) OR EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=NEW.worker_id AND w.left_at='' AND w.id!=NEW.source_segment_id) THEN RAISE(ABORT,'borrow_source_changed') END;
     SELECT CASE WHEN NEW.source_kind!='legacy' AND NOT EXISTS(SELECT 1 FROM sop_records s WHERE s.id=NEW.source_job_id AND s.kind=NEW.source_kind AND s.revision=NEW.source_revision AND (s.kind!='task' OR json_extract(s.state,'$.status')='working')) THEN RAISE(ABORT,'borrow_source_changed') END; END`,
    `CREATE TRIGGER ck_crew_one_clock BEFORE INSERT ON v2_ops_job_workers WHEN NEW.left_at='' AND EXISTS(SELECT 1 FROM ck_crew_borrows b WHERE b.worker_id=NEW.worker_id) AND EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.worker_id=NEW.worker_id AND w.left_at='') BEGIN SELECT RAISE(ABORT,'borrow_worker_busy'); END`,
    `CREATE TRIGGER ck_crew_return_guard BEFORE INSERT ON v2_ops_job_workers WHEN NEW.id LIKE 'WS-RETURN-%' BEGIN
     SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_crew_borrows b JOIN v2_ops_jobs j ON j.id=b.source_job_id WHERE 'WS-RETURN-'||b.id=NEW.id AND b.worker_id=NEW.worker_id AND b.source_job_id=NEW.job_id AND b.status='return_pending' AND j.status IN ('working','awaiting_close') AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=b.destination_job_id AND w.worker_id=b.worker_id AND w.left_at='') AND (b.source_kind='legacy' OR EXISTS(SELECT 1 FROM sop_records s WHERE s.id=b.source_job_id AND s.kind=b.source_kind AND COALESCE(json_extract(s.state,'$.owner_id'),'')=b.source_owner_id AND (s.kind!='task' OR json_extract(s.state,'$.status')='working') AND EXISTS(SELECT 1 FROM json_each(s.state,'$.workers') w WHERE json_extract(w.value,'$.id')=b.worker_id)))) THEN RAISE(ABORT,'borrow_return_changed') END; END`,
    `CREATE TRIGGER ck_crew_source_attendance BEFORE INSERT ON ck_crew_borrows WHEN NEW.worker_id GLOB 'DA-*' OR NEW.worker_id GLOB 'DAF-*' OR NEW.worker_id GLOB 'EMP-*' BEGIN
     SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id JOIN v2_ops_job_workers w ON w.id=NEW.source_segment_id WHERE d.worker_id=NEW.worker_id AND d.day=NEW.source_day AND p.enabled=1 AND d.signed_out='' AND ${activeAttendanceSQL('d')} AND d.signed_in<=w.joined_at AND date(w.joined_at,'+9 hours')=NEW.source_day AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='')) THEN RAISE(ABORT,'borrow_source_attendance_changed') END; END`,
    `CREATE TRIGGER ck_crew_return_attendance BEFORE INSERT ON v2_ops_job_workers WHEN NEW.id LIKE 'WS-RETURN-%' AND (NEW.worker_id GLOB 'DA-*' OR NEW.worker_id GLOB 'DAF-*' OR NEW.worker_id GLOB 'EMP-*') BEGIN
     SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d JOIN ck_attendance_people p ON p.id=d.person_id WHERE d.worker_id=NEW.worker_id AND d.day=date(NEW.joined_at,'+9 hours') AND p.enabled=1 AND d.signed_out='' AND ${activeAttendanceSQL('d')} AND d.signed_in<=NEW.joined_at AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks r WHERE r.attendance_id=d.id AND r.ended_at='')) THEN RAISE(ABORT,'borrow_return_attendance_changed') END; END`,
  ];
  for(const sql of oldGuards)s.DB.raw.exec(sql);
  const a=await s.daily('Synthetic loan picker'),b=await s.daily('Synthetic retained picker');s.at('09:10');
  const source=await s.ok('sop_native_start',s.request(s.crew(a,b)));const lower=s.historicalLower(source.job_id,a.badgeId);
  const original=s.segments(source.job_id).find(x=>x.worker_id===lower),retained=s.segments(source.job_id).find(x=>x.worker_id===b.badgeId),roster=s.state(source.job_id).workers;
  s.at('09:20');const payload={action:'v2_unplanned_unload_start',biz_class:'bulk',cargo_summary:'Synthetic borrowed unload',client_req_id:'synthetic-historic-loan'};
  const available=await s.ok('sop_crew_availability',{worker_id:a.badgeId,payload});
  assert.equal(available.can_borrow,true);assert.equal(available.source.job_id,source.job_id);assert.equal(available.source.segment_id,original.id);
  const target=await s.ok('sop_native_start',{
    payload,workers:s.crew(a),lead_id:a.badgeId,estimated_minutes:15,labor_department:'bulk',
    borrow_confirmations:[{worker_id:lower,source_job_id:source.job_id,source_segment_id:original.id,source_revision:available.source.revision}],
  });
  const closed=s.segments(source.job_id).find(x=>x.id===original.id);
  assert.equal(closed.worker_id,lower);assert.equal(closed.joined_at,original.joined_at);assert.equal(closed.left_at,instant('09:20'));assert.equal(closed.minutes_worked,10);
  assert.deepEqual(s.segments(source.job_id).find(x=>x.id===retained.id),retained);assert.deepEqual(s.state(source.job_id).workers,roster);
  assert.equal(s.segments(target.job_id)[0].worker_id,a.badgeId);
  assert.ok(s.docs(source.job_id).filter(x=>x.segment_id===original.id).every(x=>x.status==='completed'&&x.minutes_worked===10));
  // An existing loan may also predate canonical writes. Preserve that audit row
  // while requiring the newly created return segment and documents to canonicalize.
  const loan=s.rows('ck_crew_borrows')[0];s.DB.raw.prepare('UPDATE ck_crew_borrows SET worker_id=? WHERE id=?').run(lower,loan.id);
  s.at('09:30');await s.ok('v2_unplanned_unload_finish',{job_id:target.job_id,worker_id:a.badgeId,result_lines:[{unit_type:'carton',actual_qty:1}]});
  const returned=s.rows('ck_crew_borrows')[0];assert.equal(returned.status,'returned');assert.equal(returned.worker_id,lower);
  const segment=s.segments(source.job_id).find(x=>x.id===returned.return_segment_id);
  assert.equal(segment.worker_id,a.badgeId);assert.equal(segment.joined_at,instant('09:30'));assert.equal(segment.left_at,'');
  assert.deepEqual(s.segments(source.job_id).find(x=>x.id===original.id),closed);assert.deepEqual(s.segments(source.job_id).find(x=>x.id===retained.id),retained);
  assert.deepEqual(s.state(source.job_id).workers,roster);assert.deepEqual(s.state(source.job_id).borrowed_worker_ids,[]);
  const returnedDocs=s.docs(source.job_id).filter(x=>x.segment_id===segment.id);assert.equal(returnedDocs.length,1);assert.equal(returnedDocs[0].worker_id,a.badgeId);assert.equal(returnedDocs[0].status,'working');
  const before=s.snapshot();await s.ok('sop_crew_return',{job_id:target.job_id});assert.deepEqual(s.snapshot(),before);
});

test('a load trip with an old lowercase crew signature replays either badge spelling without changing business records',async t=>{
  const s=fixture(t),p=await s.daily('Synthetic load trip picker');s.env.SOP_WORK_CHAIN_ENABLED='true';
  // Use the same reviewed inventory-work flow as outbound-load-fixture.mjs;
  // the order is approved by real APIs before the loading request is created.
  const need=await s.ok('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYNTHETIC-LOAD-BADGE-STOCK',title:'Synthetic load work',customer:'Synthetic load customer',instructions:'Synthetic approved stock',planned_quantity:4,planned_unit:'箱'});
  const order=await s.ok('v2_outbound_order_create',{customer:'Synthetic load customer',biz_class:'bulk',outbound_mode:'customer_pickup',expected_ship_at:day,sop_existing_need_id:need.id,sop_need_revision:need.revision,sop_link_quantity:4,po_no:'SYNTHETIC-LOAD-BADGE-PO'});
  await s.ok('v2_outbound_order_update_status',{id:order.id,status:'issued'});
  const task=await s.ok('sop_task_create',{department:'bulk',title:'Synthetic stock preparation',job_type:'bulk_op',need_id:need.id,workers:[{id:'SYNTHETIC-LOAD-PREP',name:'Synthetic preparation worker'}],lead_id:'SYNTHETIC-LOAD-PREP',estimated_minutes:10});
  const revision=async id=>(await s.ok('sop_get',{id})).record.revision;
  await s.ok('sop_task_start',{id:task.id,revision:await revision(task.id)});
  await s.ok('sop_task_complete_review',{id:task.id,revision:await revision(task.id),result:{quantity:4,unit:'箱'},decision:'pass',reason:'Synthetic approval'});
  s.at('09:10');const request={
    payload:{action:'v2_outbound_load_start',order_ids:[order.id],client_req_id:'synthetic-old-load-signature',vehicle_no:'SYNTHETIC-TRUCK',driver_name:'Synthetic driver'},
    workers:s.crew(p),lead_id:p.badgeId,estimated_minutes:30,labor_department:'bulk',
  };
  const started=await s.ok('sop_native_start',request),lower=s.historicalLower(started.job_id,p.badgeId);
  const trip=s.DB.raw.prepare('SELECT * FROM ck_load_trips WHERE job_id=?').get(started.job_id);
  assert.ok(trip.signature_json.includes(p.badgeId));
  s.DB.raw.prepare('UPDATE ck_load_trips SET signature_json=? WHERE job_id=?').run(trip.signature_json.replaceAll(p.badgeId,lower),started.job_id);
  const snapshot=()=>({...s.snapshot(),...Object.fromEntries(['v2_outbound_orders','ck_load_trips','ck_load_order_links','ck_load_order_claims','ck_crew_borrows','ck_crew_requests'].map(table=>[table,s.rows(table)]))});
  const before=snapshot();s.at('09:20');
  for(const id of [p.badgeId,lower]){
    const replay=await s.ok('sop_native_start',{...request,workers:[{id,name:p.name}],lead_id:id});
    assert.equal(replay.job_id,started.job_id);assert.equal(replay.is_new_job,false);assert.deepEqual(snapshot(),before);
  }
  const changed=await s.call('sop_native_start',{...request,estimated_minutes:31});
  assert.equal(changed.ok,false);assert.match(changed.error,/变更|变化/);assert.deepEqual(snapshot(),before);
  assert.equal(s.rows('ck_load_trips').length,1);assert.equal(s.segments(started.job_id).length,1);assert.equal(s.segments(started.job_id)[0].worker_id,lower);
});
