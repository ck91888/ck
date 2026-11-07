import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

test('field issue activity is ordered across the full set before pagination; opening alone does not promote a ticket',async()=>{
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-recency-only')};
 let cookie='';
 const call=async(action,data={})=>{
  const response=await worker.fetch(new Request('https://local.test/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
  if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
  const value=rememberOperation(await response.json(),cookie);assert.equal(value.ok,true,`${action}: ${value.error||value.message||''}`);return value;
 };
 try{
  await call('sop_login',{sop_key:'synthetic-recency-only'});await confirmOperation(call,'Synthetic recency operator');
  const insert=DB.raw.prepare("INSERT INTO v2_issue_tickets(id,biz_class,customer,issue_description,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)");
  const at=n=>new Date(Date.parse('2026-10-01T00:00:00.000Z')+n*60000).toISOString();
  for(let i=1;i<=251;i++)insert.run('ISS-'+String(i).padStart(3,'0'),i%2?'import':'bulk','Synthetic '+i,'Synthetic issue '+i,'pending',at(i),i===200?'invalid':i===250?null:at(i));
  const ticket=id=>DB.raw.prepare('SELECT status,updated_at FROM v2_issue_tickets WHERE id=?').get(id);
  const before=ticket('ISS-002');await call('sop_issue_adopt',{legacy_id:'ISS-002',native:true});assert.deepEqual(ticket('ISS-002'),before);
  DB.raw.prepare("INSERT INTO sop_events(request_id,record_id,expected_revision,action,actor_id,actor_name,before_json,after_json,result_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)").run('ACT-1','ISSUE-ISS-002',1,'sop_issue_adopt','QA','QA','{}','{}','{}','2026-10-15T00:00:00.000Z');
  DB.raw.prepare("UPDATE v2_issue_tickets SET updated_at=? WHERE id='ISS-001'").run('2026-10-14T00:00:00.000Z');
  DB.raw.prepare("UPDATE v2_issue_tickets SET updated_at=? WHERE id IN ('ISS-210','ISS-211')").run('2026-10-13T23:00:00.000Z');
  DB.raw.prepare("INSERT INTO sop_events(request_id,record_id,expected_revision,action,actor_id,actor_name,before_json,after_json,result_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)").run('ACT-2','ISSUE-ISS-003',0,'sop_issue_ack','QA','QA','{}','{}','{}','2026-10-13T22:00:00.000Z');
  DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,created_at) VALUES('ATT-QA','issue_ticket','ISS-004','2026-10-13T21:00:00.000Z')").run();
  DB.raw.prepare("INSERT INTO v2_issue_handle_runs(id,issue_id,job_id,started_at) VALUES('RUN-QA','ISS-005','JOB-QA','2026-10-01T00:00:00.000Z')").run();
  DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,related_doc_type,related_doc_id,updated_at) VALUES('JOB-QA','issue_handle','issue_ticket','ISS-005','2026-10-13T20:00:00.000Z')").run();
  DB.raw.prepare("INSERT INTO v2_issue_rework_requests(id,issue_id,created_at) VALUES('RW-QA','ISS-006','2026-10-13T19:00:00.000Z')").run();
  const ids=[];
  for(let offset=0;offset<251;offset+=50){const r=await call('v2_issue_ops_list',{sort:'latest_activity',status:'pending',limit:50,offset});assert.equal(r.total,251);assert.equal(r.offset,offset);ids.push(...r.items.map(x=>x.id));}
  assert.equal(ids.length,251);assert.equal(new Set(ids).size,251);
  assert.deepEqual(ids.slice(0,7),['ISS-001','ISS-211','ISS-210','ISS-003','ISS-004','ISS-005','ISS-006']);
  assert.ok(ids.indexOf('ISS-002')>100,'read-only SOP adoption must not promote the old ticket');
  assert.ok(ids.indexOf('ISS-251')<ids.indexOf('ISS-200'),'invalid updated_at falls back to created_at');
  const imported=await call('v2_issue_ops_list',{sort:'latest_activity',status:'pending,rework_required',biz_class:'import',limit:50});assert.equal(imported.total,126);assert.ok(imported.items.every(x=>x.biz_class==='import'));
  const old=await call('v2_issue_ops_list',{sort:'oldest_first',status:'pending'});assert.equal(old.items[0].id,'ISS-001','legacy sort remains available to other callers');
 }finally{DB.raw.close();}
});
