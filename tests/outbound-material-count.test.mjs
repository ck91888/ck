import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {workMaterialCountFor} from '../worker-v2/work-chain.js';
import {ensureBatchMaterialState} from '../worker-v2/batch-material-state.js';

async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true'};
 await ensureBatchMaterialState(env);
 for(const id of ['O1','O2','O3','EMPTY'])DB.raw.prepare('INSERT INTO v2_outbound_orders(id) VALUES(?)').run(id);
 const attachment=(id,type,doc,category='work_material')=>DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category) VALUES(?,?,?,?)').run(id,type,doc,category);
 const need=(id,state,kind='need')=>DB.raw.prepare('INSERT INTO sop_records(id,kind,department,state,updated_at) VALUES(?,?,\'bulk\',?,\'2026-10-19\')').run(id,kind,JSON.stringify(state));
 const counts=(hasMaterial)=>DB.raw.prepare('SELECT id,'+workMaterialCountFor(env)+' AS n FROM v2_outbound_orders'+(hasMaterial===undefined?'':' WHERE '+workMaterialCountFor(env)+(hasMaterial?'>0':'=0'))+' ORDER BY id').all().map(r=>({...r}));
 return {DB,env,attachment,need,counts};
}

test('outbound counts retain direct, linked, historical and removed-material rules without duplicate files',async t=>{
 const s=await fixture();t.after(()=>s.DB.raw.close());
 s.attachment('DIRECT','outbound_order','O1','outbound_material');
 s.attachment('WRONG-CATEGORY','outbound_order','O1','arrival_photo');
 s.attachment('OWN','sop_need','N1');s.attachment('WITHDRAWN','sop_need','N1');
 s.attachment('INBOUND','inbound_plan','I1','inbound_material');
 s.attachment('BATCH','inbound_plan','I1','batch_work_material');
 s.attachment('UNRELATED-INBOUND','inbound_plan','I1','arrival_photo');
 s.attachment('SIBLING','outbound_order','O2','outbound_material');
 s.need('N1',{source_type:'inbound',source_id:'I1',links:[{outbound_id:'O1'},{outbound_id:'O2'}],removed_material_ids:['WITHDRAWN']});
 s.need('N2',{source_type:'inbound',source_id:'I1',links:[{outbound_id:'O1'}]});
 s.need('NOT-A-NEED',{source_type:'inbound',source_id:'I1',links:[{outbound_id:'EMPTY'}]},'task');
 s.attachment('SOURCE-OWN','sop_need','N3');
 s.need('N3',{source_type:'outbound',source_id:'O3',links:[]});
 assert.deepEqual(s.counts(),[{id:'EMPTY',n:0},{id:'O1',n:5},{id:'O2',n:5},{id:'O3',n:1}]);
 assert.deepEqual(s.counts(false),[{id:'EMPTY',n:0}]);
 assert.deepEqual(s.counts(true).map(r=>r.id),['O1','O2','O3']);
 s.DB.raw.prepare("INSERT INTO ck_batch_material_state VALUES('BATCH','I1',1,1,'QA','QA','2026-10-19')").run();
 assert.deepEqual(s.counts(),[{id:'EMPTY',n:0},{id:'O1',n:4},{id:'O2',n:4},{id:'O3',n:1}]);
 s.env.SOP_ENVIRONMENT='production';
 assert.equal(s.counts().find(r=>r.id==='O1').n,5,'legacy non-staging expression has no batch-state dependency');
});

test('group allocations and reservations count only frozen selected files, shared files once, and respect batch removal',async t=>{
 const s=await fixture();t.after(()=>s.DB.raw.close());
 for(const id of ['F1','F2','COMMON','UNSELECTED'])s.attachment(id,'sop_need','GROUPED');
 s.attachment('DIRECT','outbound_order','O1','outbound_material');
 s.need('GROUPED',{cargo_groups:{groups:[]},links:[
  {outbound_id:'O1',cargo_allocations:[{documents:[{id:'F1'},{id:'MISSING'}],public_documents:[{id:'COMMON'},{id:'DIRECT'}]},{documents:[{id:'F1'}],public_documents:[{id:'COMMON'}]}]},
  {outbound_id:'O2',cargo_reservation:{groups:[{documents:[{id:'F2'}],public_documents:[{id:'COMMON'}]}]}}
 ],removed_material_ids:['F1']});
 assert.deepEqual(s.counts(),[{id:'EMPTY',n:0},{id:'O1',n:3},{id:'O2',n:2},{id:'O3',n:0}]);
 s.DB.raw.prepare("INSERT INTO ck_batch_material_state VALUES('COMMON','I1',1,1,'QA','QA','2026-10-19')").run();
 assert.deepEqual(s.counts(),[{id:'EMPTY',n:0},{id:'O1',n:2},{id:'O2',n:1},{id:'O3',n:0}]);
});
