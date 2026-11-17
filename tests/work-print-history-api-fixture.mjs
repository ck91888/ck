// Real authenticated Worker requests with entirely synthetic, isolated storage.
import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {database} from './d1-adapter.mjs';import {localD1} from './d1-local-runtime.mjs';
import {digest} from '../worker-v2/access-control.js';import {workNeedDocumentKey} from '../worker-v2/work-need-print.js';
import {confirmOperation,operationHeaders,rememberOperation} from './office-operator-fixture.mjs';
export const historyHash=x=>createHash('sha256').update(typeof x==='string'?x:JSON.stringify(x)).digest('hex');
export async function historyApiFixture(worker,size,{native=false}={}){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-print-history-only')};let cookie='',runtime;
 const close=async()=>{await runtime?.close();DB.raw.close();};
 try{
  const call=async(action,body={})=>{
   const response=await worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
   const next=response.headers.getSetCookie().find(s=>s.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];
   const text=await response.text(),r=rememberOperation(JSON.parse(text),cookie);assert.equal(r.ok,true,action+': '+(r.error||r.message));return {r,text};
  };
  await call('sop_login',{sop_key:'synthetic-print-history-only'});await confirmOperation(async(a,b)=>(await call(a,b)).r,'合成历史性能核验员');await call('sop_need_groups',{limit:1});
  const states=[],at='2026-11-03T00:00:00.000Z',need=DB.raw.prepare("INSERT INTO sop_records(id,kind,department,revision,state,updated_at) VALUES(?,'need','bulk',1,?,?)");
  DB.raw.exec('BEGIN');
  for(let i=0;i<size;i++){
   const x={id:'SYN-PRINT-'+i,kind:'need',source_type:'inventory',source_id:'',customer:'合成客户'+i,supply_chain_no:'SYN-GROUP-'+i,title:'合成作业'+i,instructions:'合成原作业要求',status:['pending','assigned','working','closed','cancelled'][i%5],created_at:at};
   if(i%3)x.requirement_version=2;states.push(x);need.run(x.id,JSON.stringify(x),at);
  }
  DB.raw.exec('COMMIT');
  const numbers=new Map(DB.raw.prepare('SELECT record_id,display_no FROM sop_document_numbers').all().map(x=>[x.record_id,x.display_no])),events=[];
  for(const [i,x]of states.entries()){
   if(i%11===0)continue;
   const key=await workNeedDocumentKey({...x,display_no:numbers.get(x.id),source_display_no:''});
   for(let j=0;j<10;j++){
    const payload=j%5===0?'{broken':j%5===1?'':j%5===2?'[]':JSON.stringify({document_key:i%3===0?'old-version':key,...(j%2?{requirement_version:j+1}:{})});
    events.push(['SYN-REQUEST-'+String(i).padStart(6,'0')+'-'+String(j).padStart(3,'0'),x.id,1,'sop_need_print_request','synthetic:historical','合成打印员'+j,'{}',payload,'{}','2026-11-03T00:00:'+String(j).padStart(2,'0')+'.000Z']);
   }
  }
  const insert=DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)');DB.raw.exec('BEGIN');for(const e of events)insert.run(...e);DB.raw.exec('COMMIT');
  if(native){runtime=await localD1(DB.raw);env.DB=runtime;await call('sop_need_groups',{source_id:'SYN-NONEXISTENT',limit:1});}
  const snapshot=async()=>historyHash(await Promise.all(['sop_records','sop_document_numbers','sop_events','v2_ops_jobs','v2_ops_job_results','v2_ops_job_workers','v2_inbound_plans','v2_outbound_orders','v2_attachments'].map(async table=>(await env.DB.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()).results)));
  let capturing;const prepare=env.DB.prepare.bind(env.DB);
  env.DB.prepare=sql=>{const s=prepare(sql);if(capturing){const c={sql,args:[]};capturing.push(c);const bind=s.bind.bind(s);s.bind=(...a)=>{c.args=a;return bind(...a);};}return s;};
  const measure=async(action,body)=>{capturing=[];try{const start=performance.now(),result=await call(action,body),ms=performance.now()-start;return {...result,ms,queries:capturing};}finally{capturing=null;}};
  return {DB,env,call,measure,snapshot,close,eventCount:events.length};
 }catch(error){await close();throw error;}
}
