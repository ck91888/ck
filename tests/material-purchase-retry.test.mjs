import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

const source=fs.readFileSync(new URL('../003/app.js',import.meta.url),'utf8');
const submit=source.slice(source.indexOf('async function submitPurchaseRequest('),source.indexOf('async function loadPurchasing('));
const open=source.slice(source.indexOf('async function openPurchaseRequest('),source.indexOf('function renderPurchaseMaterialSelection('));
const requestId=source.split('\n').find(line=>line.startsWith('function reqId('));
const operator=source.split('\n').find(line=>line.startsWith('function operatorPayload('));
async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const actor={operator_id:'FIXTURE-PURCHASER',operator_name:'Fixture manager'};
 const call=async(action,data={})=>{
  const response=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...actor,...data})}),env);
  const result=await response.json();if(!result.ok)throw Error(result.error);return result;
 };
 const material=await call('v2_003_material_save',{name_zh:'Fixture tape',category:'Fixture',unit:'roll',opening_qty:0});
 const fields={prUrgency:'normal',prReason:'Fixture purchase',prNote:''},requests=[],notices=[],closed=[];
 let handler=call;
 const context=vm.createContext({crypto,Date,Number,Object,S003:{busy:false,lang:'zh',operatorId:actor.operator_id,operatorName:actor.operator_name,purchaseRequestLines:[{material_id:material.id,requested_qty:10}],purchaseRequestSubmission:null},
  val:id=>String(fields[id]||'').trim(),T:key=>key,toast:(...args)=>notices.push(args),errorText:x=>x,
  closeModal:id=>closed.push(id),isAdmin:()=>true,openPurchaseDetail:()=>{},loadFieldHome:()=>{},ensurePurchaseMaterials:async()=>{},renderPurchaseMaterialSelection:()=>{},renderPurchaseRequestLines:()=>{},
  E:id=>({get value(){return fields[id]||'';},set value(value){fields[id]=value;},classList:{remove(){}}}),
  api:async(action,payload)=>{requests.push({...payload});return handler(action,payload);}
 });
 vm.runInContext(requestId+'\n'+operator+'\n'+open+'\n'+submit,context);
 return {DB,call,material,context,fields,requests,notices,closed,setHandler(fn){handler=fn;},submit:()=>context.submitPurchaseRequest({preventDefault(){}}),count:()=>DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_purchase_orders').get().n};
}

test('purchase retry after a lost committed response creates only one request through the real worker',async()=>{
 const f=await fixture();let first=true;
 f.setHandler(async(action,payload)=>{const result=await f.call(action,payload);if(first){first=false;throw Error('Fixture response lost after commit');}return result;});
 await f.submit();assert.equal(f.count(),1);assert.equal(f.closed.length,0);assert.equal(f.context.S003.busy,false);assert.equal(f.context.S003.purchaseRequestLines[0].requested_qty,10);
 await f.submit();assert.equal(f.count(),1);assert.equal(f.requests.length,2);assert.equal(f.requests[0].client_req_id,f.requests[1].client_req_id);assert.equal(f.closed.length,1);
});

test('purchase retry before commit preserves the form and commits one request',async()=>{
 const f=await fixture();let first=true;
 f.setHandler(async(action,payload)=>{if(first){first=false;throw Error('Fixture offline before commit');}return f.call(action,payload);});
 await f.submit();assert.equal(f.count(),0);assert.equal(f.closed.length,0);await f.submit();assert.equal(f.count(),1);assert.equal(f.requests[0].client_req_id,f.requests[1].client_req_id);
});

test('editing a failed purchase changes the intent and saves the edited quantities',async()=>{
 const f=await fixture();f.setHandler(async()=>{throw Error('Fixture offline');});await f.submit();
 f.context.S003.purchaseRequestLines[0].requested_qty=20;f.fields.prReason='Edited fixture purchase';f.setHandler(f.call);await f.submit();
 assert.notEqual(f.requests[0].client_req_id,f.requests[1].client_req_id);assert.equal(f.count(),1);assert.equal(f.DB.raw.prepare('SELECT requested_qty FROM v2_003_purchase_order_lines').get().requested_qty,20);
});

test('a successful request or explicit new form can create a distinct identical purchase',async()=>{
 for(const startNew of ['success','reopen']){
  const f=await fixture();if(startNew==='success')await f.submit();else{f.setHandler(async(action,payload)=>{await f.call(action,payload);throw Error('Fixture response lost');});await f.submit();}
  await f.context.openPurchaseRequest();f.context.S003.purchaseRequestLines=[{material_id:f.material.id,requested_qty:10}];f.fields.prReason='Fixture purchase';f.setHandler(f.call);await f.submit();
  assert.equal(f.count(),2,startNew);assert.notEqual(f.requests[0].client_req_id,f.requests[1].client_req_id,startNew);
 }
});

test('repeated clicks during a pending purchase send one request',async()=>{
 const f=await fixture();let release;f.setHandler((action,payload)=>new Promise(resolve=>{release=async()=>resolve(await f.call(action,payload));}));
 const pending=f.submit();await f.submit();assert.equal(f.requests.length,1);await release();await pending;assert.equal(f.count(),1);
});
