// Real authenticated Worker API, synthetic SQLite only. Never contacts a live site.
import assert from 'node:assert/strict';
import {mock} from 'node:test';
import {performance} from 'node:perf_hooks';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import fs from 'node:fs';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {confirmOperation,operationHeaders,rememberOperation} from './office-operator-fixture.mjs';
import {factsFixture} from './work-plan-performance-fixture.mjs';
mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
const url=process.env.CK_WORK_PLAN_API_MODULE?pathToFileURL(process.env.CK_WORK_PLAN_API_MODULE).href:new URL('../worker-v2/index.js',import.meta.url).href;
const {default:worker}=await import(url),samples=[];
const hash=x=>createHash('sha256').update(x).digest('hex');
try{for(const size of (process.env.CK_WORK_PLAN_SIZES||'100,1000,10000').split(',').map(Number)){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-work-plan-performance')};let cookie='';
 try{
  const call=async(action,body={})=>{const response=await worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);const next=response.headers.getSetCookie().find(s=>s.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];const text=await response.text(),r=rememberOperation(JSON.parse(text),cookie);assert.equal(r.ok,true,r.error||r.message);return {r,text};};
  await call('sop_login',{sop_key:'synthetic-work-plan-performance'});await confirmOperation(async(a,b)=>(await call(a,b)).r,'合成性能测试员');await call('sop_need_groups',{limit:50});
  const f=factsFixture(size),at='2026-11-03T00:00:00.000Z';DB.raw.exec('BEGIN');
  const plan=DB.raw.prepare('INSERT INTO v2_inbound_plans(id,display_no,customer,biz_class,biz_classes_json,status,expected_arrival,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)');
  for(const [i,p]of f.plans.entries())plan.run(p.id,p.display_no,'合成客户'+i,'bulk',p.biz_classes_json,'pending',p.expected_arrival,at,at);
  const order=DB.raw.prepare('INSERT INTO v2_outbound_orders(id,display_no,customer,status,expected_ship_at,source_inbound_plan_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)'),seen=new Set();
  for(const o of f.orders)if(!seen.has(o.id)){seen.add(o.id);order.run(o.id,o.display_no,'合成客户',o.status,o.expected_ship_at,o.source_inbound_plan_id,at,at);}
  const need=DB.raw.prepare("INSERT INTO sop_records(id,kind,department,revision,state,updated_at) VALUES(?,'need','bulk',1,?,?)");
  for(const [i,g]of f.groups.entries())need.run('PERF-NEED-'+i,JSON.stringify({...g.items[0],id:"PERF-NEED-"+i,source_type:g.source_type,source_id:g.source_id,customer:g.customer,supply_chain_no:'SYN-STOCK-'+i,title:'合成作业'+i,instructions:'合成性能验证',status:'pending',created_at:at}),at);
  DB.raw.exec('COMMIT');
  const snapshot=()=>hash(JSON.stringify(['sop_records','v2_inbound_plans','v2_outbound_orders','v2_attachments'].map(table=>DB.raw.prepare('SELECT * FROM '+table+' ORDER BY id').all()))),before=snapshot();
  for(const [scenario,body]of [['first-page',{limit:50}],...(size===10000?[['late-filter',{customer:'合成客户9999',reference:'PERF-OB-9999',limit:1,offset:9999}]]:[])]){
   const start=performance.now(),{r,text}=await call('sop_need_groups',body),ms=performance.now()-start;
   assert.equal(r.total,scenario==='first-page'?size:1);assert.equal(r.items.length,scenario==='first-page'?Math.min(50,size):1);assert.equal(r.offset,0);
   const sample={size,scenario,api_ms:Number(ms.toFixed(3)),payload_bytes:Buffer.byteLength(text),payload_sha256:hash(text),total:r.total,returned:r.items.length};samples.push(sample);console.log(JSON.stringify(sample));
  }
  assert.equal(snapshot(),before,'read API must preserve original business data');
 }finally{DB.raw.close();}
}}finally{mock.timers.reset();}
if(process.env.CK_WORK_PLAN_METRICS)fs.writeFileSync(process.env.CK_WORK_PLAN_METRICS,JSON.stringify({module:url,samples},null,2));
