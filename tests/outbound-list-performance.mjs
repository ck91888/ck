// Local synthetic SQLite benchmark. No online API, D1, credentials or business data.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {execFileSync} from 'node:child_process';
import {database} from './d1-adapter.mjs';
import {workMaterialCountFor} from '../worker-v2/work-chain.js';
import {ensureBatchMaterialState,batchVisibleSql} from '../worker-v2/batch-material-state.js';

const baseline='96dee541bcdad15fed1f2bd78ae4f943044a5f95';
const source=execFileSync('git',['show',baseline+':worker-v2/work-chain.js'],{encoding:'utf8'});
const oldCount=vm.runInNewContext(source.slice(source.indexOf('export const workMaterialCountSql')).replaceAll('export const ','const ')+'\nworkMaterialCountFor',{batchVisibleSql});
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true'};
await ensureBatchMaterialState(env);
for(let i=0;i<50;i++)DB.raw.prepare('INSERT INTO v2_outbound_orders(id) VALUES(?)').run('QA-O-'+i);
for(let i=0;i<200;i++){
 DB.raw.prepare("INSERT INTO sop_records(id,kind,department,state,updated_at) VALUES(?,'need','bulk',?,'2026-10-19')").run('QA-N-'+i,JSON.stringify({source_type:'inbound',source_id:'QA-I-'+i,links:[{outbound_id:i<10?'QA-O-'+i:'OTHER-'+i}]}));
 for(let j=0;j<5;j++)DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category) VALUES(?,'sop_need',?,'work_material')").run('QA-A-'+i+'-'+j,'QA-N-'+i);
}
const results=[];let expected;
for(let iteration=0;iteration<3;iteration++)for(const [version,expression] of [['baseline',oldCount(env)],['candidate',workMaterialCountFor(env)]]){
 const query='SELECT id,'+expression+' AS material_count FROM v2_outbound_orders ORDER BY id LIMIT 50';
 const started=performance.now(),rows=DB.raw.prepare(query).all(),ms=Math.round((performance.now()-started)*10)/10;
 if(!expected)expected=rows;else assert.deepEqual(rows,expected);
 assert.equal(rows.filter(r=>r.material_count===5).length,10);
 results.push({version,iteration,ms});console.log(version,iteration,ms+'ms');
}
const report={synthetic:true,baseline,fixture:{orders:50,needs:200,attachments:1000,linkedOrders:10},identicalResults:true,results};
fs.writeFileSync('/tmp/ck-outbound-material-performance.json',JSON.stringify(report,null,2));
DB.raw.close();
