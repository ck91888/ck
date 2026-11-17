import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {jobBusinessFilter} from '../worker-v2/document-numbers.js';
import {ensureLoadSchema} from '../worker-v2/outbound-load-trip.js';

test('dashboard contains search finds direct and grouped references using keyed lookups',async()=>{
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true'};
 await ensureLoadSchema(env);
 DB.raw.exec(`
   INSERT INTO v2_ops_jobs(id,display_no,related_doc_id,linked_outbound_order_id) VALUES
   ('job-direct','direct-101','in-direct','out-linked'),('job-unload','','',''),('job-load','','',''),
   ('job-out-direct','','out-direct',''),('job-task','','',''),('job-not-task','','',''),('job-other','','','');
   INSERT INTO v2_inbound_plans(id,display_no,status,updated_at) VALUES
   ('in-direct','inbound-201','pending','v1'),('in-grouped','inbound-202','pending','v1');
   INSERT INTO ck_unload_plan_links(job_id,plan_id,position,plan_version,lines_snapshot)
    VALUES('job-unload','in-grouped',1,'v1','[]');
   INSERT INTO v2_outbound_orders(id,display_no,status,updated_at) VALUES
   ('out-linked','outbound-301','issued','v1'),('out-grouped','outbound-302','issued','v1'),('out-direct','outbound-303','issued','v1');
   INSERT INTO ck_load_order_links(job_id,order_id,position,snapshot_json)
    VALUES('job-load','out-grouped',1,'{"order_version":"v1","revision_no":0,"needs":[]}');
   INSERT INTO sop_records(id,kind,department,state,updated_at) VALUES
   ('need-a','need','bulk','{}','2026-11-01T00:00:00Z'),
   ('job-task','task','bulk','{"need_id":"need-a"}','2026-11-01T00:00:00Z'),
   ('job-not-task','dispatch','bulk','{"need_id":"need-a"}','2026-11-01T00:00:00Z');
 `);
 const needNo=DB.raw.prepare("SELECT display_no FROM sop_document_numbers WHERE record_id='need-a'").get().display_no;
 const find=(pattern,scope=env)=>{
  const f=jobBusinessFilter(scope,pattern),query=`SELECT j.id FROM v2_ops_jobs j WHERE ${f.sql} ORDER BY j.id`;
  return {hits:DB.raw.prepare(query).all(...f.args).map(r=>r.id),plan:DB.raw.prepare('EXPLAIN QUERY PLAN '+query).all(...f.args).map(r=>r.detail).join('\n')};
 };
 for(const [term,expected] of [
  ['%RECT-101%',['job-direct']],['%bound-201%',['job-direct']],['%bound-202%',['job-unload']],
  ['%BOUND-30_%',['job-direct','job-load','job-out-direct']],['%'+needNo+'%',['job-task']],
  ['%no-match%',[]]
 ])assert.deepEqual(find(term).hits,expected,term);
 assert.deepEqual(find('%bound-302%',{...env,SOP_ENVIRONMENT:'production'}).hits,[]);
 const plan=find('%no-match%').plan;
 for(const alias of ['ip','op','dn'])assert.match(plan,new RegExp('SEARCH '+alias+' .*\\('),plan);
 assert.doesNotMatch(plan,/SCAN (?:ip|op|dn)\b/,plan);
});
