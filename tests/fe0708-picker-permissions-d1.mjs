// Synthetic native D1 acceptance: picker retains the existing material-list permission boundary.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
const source=database();
source.raw.prepare("INSERT INTO v2_003_materials(id,material_code,name_zh,unit,status) VALUES(?,?,?,?,?)").run('SYN-PERMISSION','SYN-PERMISSION','Synthetic permission material','个','active');
const DB=await localD1(source.raw),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',ADMINKEY:'synthetic-admin-key',VIEWKEY:'synthetic-view-key',OPSKEY:'synthetic-ops-key'},results=[];
const call=async(body)=>{const response=await worker.fetch(new Request('http://localhost/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'v2_003_material_list',...body})}),env);return {status:response.status,body:await response.json()};};
try{
 // Prime migrations before taking the synthetic business snapshot.
 assert.equal((await call({k:env.ADMINKEY})).body.ok,true);
 const snapshot=async()=>JSON.stringify(await Promise.all(['v2_003_materials','v2_003_material_txns','v2_003_purchase_orders'].map(async t=>(await DB.prepare('SELECT * FROM '+t+' ORDER BY rowid').all()).results))),before=await snapshot();
 for(const [name,body,allowed]of [
  ['anonymous',{},false],['invalid key',{k:'synthetic-invalid'},false],
  ['admin key',{k:env.ADMINKEY},true],['view key',{k:env.VIEWKEY},true],['operations key',{k:env.OPSKEY},true],
  ['existing public field identity',{operator_id:'DAF-SYN-PERMISSION',operator_name:'Synthetic operator'},true]
 ]){
  const ordinary=await call(body),picker=await call({...body,purchase_picker:1});
  assert.equal(picker.status,ordinary.status);assert.equal(picker.body.ok,allowed);assert.equal(ordinary.body.ok,allowed);
  if(allowed)assert.deepEqual(picker.body.items.map(x=>x.id),ordinary.body.items.map(x=>x.id));
  else{assert.equal(picker.status,401);const malformed=await call({...body,purchase_picker:1,search_locale:'invalid_locale'});assert.equal(malformed.status,401);assert.equal(malformed.body.error,'unauthorized');}
  results.push({name,status:picker.status,allowed});
 }
 assert.equal(await snapshot(),before);console.log(JSON.stringify({ok:true,nativeD1:true,cases:results.length,unchangedBusinessSnapshot:true,results}));
}finally{await DB.close();source.raw.close();}
