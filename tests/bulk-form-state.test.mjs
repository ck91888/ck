import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
const source=fs.readFileSync(new URL('../001/app.js',import.meta.url),'utf8');
const bulk=source.slice(source.indexOf('// ===== Bulk Op (大货操作) ====='),source.indexOf('// ===== Generic Job —'));
function fixture(){
 const elements=new Map(),requests=[],alerts=[],renders=[];let handler;
 const node=id=>{if(!elements.has(id))elements.set(id,{id,value:'',textContent:'',innerHTML:'',style:{},disabled:false,readOnly:false,checked:false,focus(){},querySelectorAll(){return [...elements.values()].filter(e=>/^bulk(Customer$|Packed|Carton|Repaired|Reboxed|Label|Total|Pallet|Forklift|UsedForklift|Remark|ResultNote)/.test(e.id));}});return elements.get(id);};
 const job=id=>({id,job_type:'bulk_op',status:'working',related_doc_id:'EXT-'+id,customer:'',created_at:'2026-09-29T01:00:00Z',active_worker_count:2});
 handler=async b=>({ok:true,job:job(b.job_id),workers:[]});
 const c={document:{getElementById:node},_activeJobId:null,_currentPage:'bulk_op',console,setInterval:()=>1,clearInterval(){},api:async b=>{requests.push(b);return handler(b);},renderWorkers:(id,workers)=>renders.push({id,workers}),startJobPoll(){},confirm:()=>true,alert:m=>alerts.push(m),getWorkerId:()=> 'EMP-FIXTURE',getWorkerName:()=> 'Fixture',hasOtherActiveJob:()=>false,acceptDocumentCode:()=>true,withActionLock:(_key,_button,_label,fn)=>fn(),saveActiveJob(id){c._activeJobId=id;},clearActiveJob(){c._activeJobId=null;},goPage(page){c._currentPage=page;},isAlreadyCompletedResponse:r=>!!r?.already_completed,handleFinishSuccessAndExit(_m,fn){c.clearActiveJob();fn();}};
 c.window=c;vm.runInNewContext(bulk,c);
 function enter(id,details={}){c._activeJobId=id;c._currentPage='bulk_op';c._bulkEnterWorkingState('EXT-'+id,{...job(id),...details});}
 return {c,node,job,requests,alerts,renders,enter,setHandler:fn=>handler=fn};
}
test('complete one external order then open another: values and submitting lock cannot cross job IDs',async()=>{
 const f=fixture();f.enter('A');f.node('bulkCustomer').value='Fixture A';f.node('bulkPalletCount').value='2';f.node('bulkRemark').value='Only A';f.node('bulkUsedForklift').checked=true;
 f.setHandler(async()=>({ok:true}));await f.c.finishBulkJob();
 assert.equal(f.requests[0].job_id,'A');assert.equal(f.requests[0].pallet_count,2);assert.equal(f.c._activeJobId,null);
 f.c._activeJobId='B';f.c._currentPage='bulk_op';f.setHandler(async b=>({ok:true,job:f.job(b.job_id),workers:[]}));await f.c.initBulkOp();
 assert.equal(f.node('bulkActiveOrderNo').textContent,'EXT-B');
 for(const id of ['bulkCustomer','bulkRemark','bulkResultNote'])assert.equal(f.node(id).value,'');
 for(const id of f.c._bulkOutputFieldIds){assert.equal(f.node(id).value,'0');assert.equal(f.node(id).disabled,false);}
 assert.equal(f.node('bulkCustomer').readOnly,false);assert.equal(f.node('bulkCustomer').disabled,false);assert.equal(f.node('bulkUsedForklift').checked,false);assert.equal(f.node('bulkFinishBtn').disabled,false);
 f.node('bulkCustomer').value='Fixture B';f.node('bulkPalletCount').value='3';f.setHandler(async()=>({ok:true}));await f.c.finishBulkJob();
 const finish=f.requests.filter(x=>x.action==='v2_bulk_op_job_finish');assert.equal(finish.length,2);assert.equal(finish[1].job_id,'B');assert.equal(finish[1].customer,'Fixture B');assert.equal(finish[1].pallet_count,3);assert.equal(finish[1].remark,'');
});
test('new external start clears a previous linked customer and restores editable mandatory customer',async()=>{
 const f=fixture();f.enter('A',{customer:'Fixture linked',linked_outbound_order_id:'OB-A'});f.node('bulkPalletCount').value='2';f.c._bulkSetSubmitting(true);
 f.c._activeJobId=null;f.node('bulkOrderInput').value='EXT-B';f.setHandler(async b=>b.action==='v2_bulk_op_job_start'?{ok:true,job_id:'B'}:{ok:true,job:f.job(b.job_id),workers:[]});
 await f.c.startBulkJob();assert.equal(f.node('bulkActiveOrderNo').textContent,'EXT-B');assert.equal(f.node('bulkCustomer').value,'');assert.equal(f.node('bulkCustomer').readOnly,false);assert.equal(f.node('bulkCustomerRequired').style.display,'');assert.equal(f.node('bulkPalletCount').value,'0');assert.equal(f.node('bulkCustomer').disabled,false);
});
test('same-job resume and polling retain unsaved output; rejected or failed submissions unlock without losing it',async()=>{
 const f=fixture();f.enter('A');f.node('bulkCustomer').value='Unsaved A';f.node('bulkPalletCount').value='4';
 await f.c.initBulkOp();await f.c.refreshBulkWorkers();assert.equal(f.node('bulkCustomer').value,'Unsaved A');assert.equal(f.node('bulkPalletCount').value,'4');
 for(const response of [async()=>({ok:false,error:'Fixture validation'}),async()=>{throw Error('Fixture offline');}]){
  f.setHandler(response);await f.c.finishBulkJob();assert.equal(f.node('bulkCustomer').value,'Unsaved A');assert.equal(f.node('bulkPalletCount').value,'4');assert.equal(f.node('bulkCustomer').disabled,false);assert.equal(f.node('bulkFinishBtn').disabled,false);assert.equal(f.c._activeJobId,'A');
 }
});
test('late detail, worker refresh and completion from A cannot overwrite or navigate away from B',async()=>{
 for(const request of ['detail','workers','finish']){
  const f=fixture();f.enter('A');f.node('bulkCustomer').value='Fixture A';f.node('bulkPalletCount').value='2';let release;
  f.setHandler(()=>new Promise(resolve=>release=resolve));
  const pending=request==='detail'?f.c.initBulkOp():request==='workers'?f.c.refreshBulkWorkers():f.c.finishBulkJob();
  f.enter('B',{customer:'Fixture B',linked_outbound_order_id:'OB-B'});f.node('bulkPalletCount').value='7';
  const before=f.renders.length;release({ok:true,job:{...f.job('A'),customer:'OLD'},workers:[{worker_name:'Old crew'}]});await pending;
  assert.equal(f.c._activeJobId,'B',request);assert.equal(f.c._currentPage,'bulk_op',request);assert.equal(f.node('bulkActiveOrderNo').textContent,'EXT-B',request);assert.equal(f.node('bulkCustomer').value,'Fixture B',request);assert.equal(f.node('bulkPalletCount').value,'7',request);assert.equal(f.renders.length,before,request);assert.equal(f.node('bulkCustomer').disabled,false,request);
 }
});
test('two sequential UI completions persist separate customers, outputs and crew time through the real worker',async()=>{
 const f=fixture(),DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const api=async b=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({client_req_id:crypto.randomUUID(),...b})}),env)).json();
 const start=async no=>{const r=await api({action:'sop_native_start',payload:{action:'v2_bulk_op_job_start',work_order_no:no,client_req_id:crypto.randomUUID()},workers:[{id:'EMP-FIXTURE',name:'Fixture'}],lead_id:'EMP-FIXTURE',estimated_minutes:10,labor_department:'bulk'});assert.equal(r.ok,true,r.error);return r.job_id;};
 f.setHandler(api);
 const a=await start('FIXTURE-A');f.c._activeJobId=a;await f.c.initBulkOp();f.node('bulkCustomer').value='Fixture A';f.node('bulkPalletCount').value='2';f.node('bulkRemark').value='Only A';await f.c.finishBulkJob();
 assert.equal(DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(a).status,'completed');
 const b=await start('FIXTURE-B');f.c._activeJobId=b;f.c._currentPage='bulk_op';await f.c.initBulkOp();assert.equal(f.node('bulkCustomer').value,'');assert.equal(f.node('bulkPalletCount').value,'0');assert.equal(f.node('bulkCustomer').disabled,false);
 f.node('bulkCustomer').value='Fixture B';f.node('bulkPalletCount').value='3';await f.c.finishBulkJob();
 for(const [id,customer,pallets,remark] of [[a,'Fixture A',2,'Only A'],[b,'Fixture B',3,'']]){
  const job=DB.raw.prepare('SELECT status,customer FROM v2_ops_jobs WHERE id=?').get(id);assert.equal(job.status,'completed');assert.equal(job.customer,customer);
  const results=DB.raw.prepare('SELECT result_json,remark FROM v2_ops_job_results WHERE job_id=?').all(id);assert.equal(results.length,1);assert.equal(JSON.parse(results[0].result_json).pallet_count,pallets);assert.equal(results[0].remark,remark);
  assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at='' ").get(id).n,0);
 }
});
