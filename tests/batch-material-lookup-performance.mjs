// Compare authenticated Worker APIs on identical local workerd D1 databases.
// CK_MATERIAL_LOOKUP_MODULES is a JSON label -> absolute index.js path mapping.
import assert from 'node:assert/strict';import fs from 'node:fs';import {mock} from 'node:test';import {pathToFileURL} from 'node:url';
import {materialReferenceFixture} from './batch-material-reference-fixture.mjs';
import {operationHeaders} from './office-operator-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {seedMaterialHistory,materialLookupSnapshot,lookupHash,lookupIndex,inboundChildren} from './batch-material-lookup-fixture.mjs';
const modules=JSON.parse(process.env.CK_MATERIAL_LOOKUP_MODULES||JSON.stringify({candidate:new URL('../worker-v2/index.js',import.meta.url).pathname})),samples=[];
const histories=(process.env.CK_MATERIAL_LOOKUP_SIZES||'100,1000,10000').split(',').map(Number);
const stagingFlags=process.env.CK_MATERIAL_LOOKUP_STAGING_FLAGS==='true';
assert.notEqual(process.env.CK_P1_NATIVE_D1,'true','seed uses Node SQLite, measurements use native D1 clones');
try{for(const history of histories){
 const cleanup=[],f=await materialReferenceFixture({mock,after:fn=>cleanup.push(fn)});
 try{
  if(stagingFlags){Object.assign(f.env,{SOP_ATTENDANCE_ENABLED:'true',SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true',SOP_ATTENDANCE_SHIFT_ACCOUNTING:'true',SOP_DISPATCH_SHARE_ENABLED:'true'});await f.ok('sop_identity');}
  await f.ok('sop_batch_work_materials',{id:f.a.id});await seedMaterialHistory(f,history);
  const expected=[];
  for(const [label,path]of Object.entries(modules)){
   const {default:worker}=await import(pathToFileURL(path)),DB=await localD1(f.DB.raw),env={...f.env,DB};
   try{
    const initial=await materialLookupSnapshot(DB),results=[];
    for(let i=0;i<4;i++){
     DB.metrics.length=0;DB.metrics.wall_ms=0;const start=performance.now();
     const response=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:f.cookie,...operationHeaders(f.cookie)},body:JSON.stringify(f.remove({action:i%2?'sop_batch_work_material_restore':'sop_batch_work_material_remove',material_revision:i,client_req_id:'synthetic-perf-request-'+i}))}),env);
     const result=await response.json(),ms=performance.now()-start,dbWall=DB.metrics.wall_ms,metrics=[...DB.metrics];assert.equal(result.ok,true,JSON.stringify(result));
     const lookup=metrics.filter(m=>m.sql.includes('FROM sop_records WHERE '+inboundChildren));
     results.push({result:lookupHash(result),snapshot:await materialLookupSnapshot(DB)});
     if(i>0&&await DB.prepare("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?").bind(lookupIndex).first())assert.ok(lookup.reduce((n,m)=>n+m.rows_read,0)<=50,'indexed six-child validation must not scan unrelated historical needs');
     const sample={label,history,phase:i===0?'cold':'warm',iteration:i,api_ms:+ms.toFixed(3),db_wall_ms:+dbWall.toFixed(3),rows_read:metrics.reduce((n,m)=>n+m.rows_read,0),rows_written:metrics.reduce((n,m)=>n+m.rows_written,0),lookup_rows_read:lookup.reduce((n,m)=>n+m.rows_read,0),index:metrics.filter(m=>m.sql.includes('CREATE INDEX')&&m.sql.includes(lookupIndex)).map(m=>({rows_read:m.rows_read,rows_written:m.rows_written,duration:m.duration})),response_hash:results.at(-1).result,business_snapshot_hash:results.at(-1).snapshot};
     samples.push(sample);console.log(JSON.stringify(sample));
    }
    if(expected.length)assert.deepEqual(results,expected,'exact responses, business rows and audit event order must match before and after optimization');else expected.push(...results);
    assert.equal((await DB.prepare('SELECT COUNT(*) n FROM ck_batch_material_need_set_guards').first()).n,0);
    assert.ok(initial);
   }finally{await DB.close();}
  }
 }finally{for(const fn of cleanup)await fn();mock.timers.reset();}
}}finally{mock.timers.reset();}
if(process.env.CK_MATERIAL_LOOKUP_METRICS)fs.writeFileSync(process.env.CK_MATERIAL_LOOKUP_METRICS,JSON.stringify({synthetic:true,stagingFlags,modules,samples},null,2));
