import test from 'node:test';import assert from 'node:assert/strict';import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
async function setup(){const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};const api=async(action,data={})=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,operator_id:'FIXTURE-M',operator_name:'Fixture manager',client_req_id:crypto.randomUUID(),...data})}),env)).json();const ok=async(a,d)=>{const r=await api(a,d);assert.equal(r.ok,true,r.error);return r;};return {DB,api,ok};}

test('materials receive, issue, return and stocktake maintain one ledger and reject negative stock',async()=>{
 const {DB,api,ok}=await setup(),m=await ok('v2_003_material_save',{name_zh:'Fixture tape',category:'包材',unit:'卷',opening_qty:10});
 const txn=(txn_type,extra={})=>({material_id:m.id,txn_type,qty:3,department:'大货',recipient_name:'Fixture crew',...extra});
 const payload=txn('issue',{client_req_id:'fixture-issue-once'});let r=await ok('v2_003_material_txn',payload);assert.equal(r.qty_after,7);assert.equal((await ok('v2_003_material_txn',payload)).id,r.id);
 assert.equal((await api('v2_003_material_txn',txn('use',{qty:8}))).ok,false);
 await ok('v2_003_material_txn',txn('return',{qty:1}));await ok('v2_003_material_txn',txn('inbound',{qty:5}));await ok('v2_003_material_txn',txn('stocktake',{counted_qty:12}));
 assert.equal(DB.raw.prepare('SELECT current_qty FROM v2_003_materials WHERE id=?').get(m.id).current_qty,12);
 const logs=DB.raw.prepare('SELECT * FROM v2_003_material_txns WHERE material_id=? ORDER BY rowid').all(m.id);assert.equal(logs.length,5);for(let i=1;i<logs.length;i++)assert.equal(logs[i].qty_before,logs[i-1].qty_after);
});

test('racing material and asset changes retry without a phantom transaction',async()=>{
 for(const kind of ['material','asset']){const {DB,ok}=await setup();const material=kind==='material',item=await ok('v2_003_'+kind+'_save',{name_zh:'Fixture item',category:'Fixture',unit:'个',opening_qty:10});const table='v2_003_'+(material?'materials':'assets'),version=material?'stock_version':'asset_version',txns=material?'material_txns':'asset_txns';
  const batch=DB.batch.bind(DB);let injected=false;DB.batch=async items=>{if(!injected&&items.some(x=>x.sql.includes('INSERT INTO v2_003_'+txns))){injected=true;DB.raw.prepare('UPDATE '+table+' SET '+version+'='+version+'+1'+(material?',current_qty=20':",location_code='OTHER'")+' WHERE id=?').run(item.id);}return batch(items);};
  const action=material?'v2_003_material_txn':'v2_003_asset_action',body=material?{material_id:item.id,txn_type:'issue',qty:3,department:'大货',recipient_name:'Fixture'}:{asset_id:item.id,action_type:'assign',department:'大货',to_keeper_id:'FIXTURE-W',to_keeper_name:'Fixture'};
  const result=await ok(action,body);assert.equal(injected,true);const logs=DB.raw.prepare('SELECT * FROM v2_003_'+txns+' WHERE '+kind+'_id=? ORDER BY rowid').all(item.id);assert.equal(logs.length,2,'creation + one committed operation only');if(material){assert.equal(result.qty_before,20);assert.equal(result.qty_after,17);assert.equal(logs[1].qty_before,20);}else assert.equal(logs[1].from_location,'OTHER');
 }
});

test('assets cannot be assigned twice and move through return, repair and retirement',async()=>{
 const {DB,api,ok}=await setup(),a=await ok('v2_003_asset_save',{name_zh:'Fixture scanner',category:'设备'});const body={asset_id:a.id,department:'大货',to_keeper_name:'Fixture worker',to_keeper_id:'FIXTURE-W'};
 for(const action_type of ['assign','return','repair_start','repair_done','assign','retire']){await ok('v2_003_asset_action',{...body,action_type});if(action_type==='assign')assert.equal((await api('v2_003_asset_action',{...body,action_type})).ok,false);}
 assert.equal((await api('v2_003_asset_action',{...body,action_type:'assign'})).ok,false);assert.equal(DB.raw.prepare('SELECT status FROM v2_003_assets WHERE id=?').get(a.id).status,'retired');
});
