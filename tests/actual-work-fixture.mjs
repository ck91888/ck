import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
const dates={date_basis:'actual_work',start_date:'2026-10-12',end_date:'2026-10-18',as_of:'2026-10-19T00:00:00Z'};
export async function reportFixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const call=async(action='v2_dashboard_order_export',data={})=>(await worker.fetch(new Request('http://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...dates,...data})}),env)).json();
 const ok=async(a,d)=>{const r=await call(a,d);assert.equal(r.ok,true,JSON.stringify(r));return r;};
 await ok('sop_identity');
 const job=(id,type='pack_direct',created='2026-10-12T00:00:00Z',finish='2026-10-13T00:00:00Z',biz='direct_ship')=>DB.raw.prepare('INSERT INTO v2_ops_jobs(id,job_type,status,created_at,updated_at,finished_at,biz_class,display_no) VALUES(?,?,?,?,?,?,?,?)').run(id,type,finish?'completed':'working',created,finish||created,finish,biz,id);
 const segment=(id,jobId,workerId,start,end,mins=999)=>DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(id,jobId,workerId,'合成职员 '+workerId,start,end,mins);
 const result=(id,jobId,p,outer=[],extra={})=>DB.raw.prepare('INSERT INTO v2_ops_job_results(id,job_id,result_json,result_lines_json,created_at,source,previous_result_id) VALUES(?,?,?,?,?,?,?)').run(id,jobId,JSON.stringify(p),JSON.stringify(outer),extra.at||'2026-10-13T00:00:00Z',extra.source||'',extra.previous_result_id||'');
 return {DB,env,call,ok,job,segment,result};
}
