// Synthetic ordered histories. No remote data, names or business documents.
import {workNeedDocumentKey} from '../worker-v2/work-need-print.js';
export const PRINT_HISTORY_SQL="SELECT record_id,actor_name,created_at,after_json FROM sop_events WHERE action='sop_need_print_request' AND record_id IN (SELECT value FROM json_each(?)) ORDER BY created_at DESC,request_id DESC";
const parse=s=>{try{return JSON.parse(s||'{}');}catch{return{};}};
// Frozen published deb5f249 algorithm, used only as a differential oracle.
export async function publishedPrintFacts(env,items){
 const needs=items.filter(x=>x.kind==='need');if(!needs.length)return items;
 const events=(await env.DB.prepare(PRINT_HISTORY_SQL).bind(JSON.stringify(needs.map(x=>x.id))).all()).results||[];
 for(const x of needs){const document_key=await workNeedDocumentKey(x),own=events.filter(p=>p.record_id===x.id),last=own[0],current=own.filter(p=>parse(p.after_json).document_key===document_key);x.print_status={state:!last?'unknown':current.length?'requested':'reprint',document_key,request_count:own.length,current_count:current.length,last_at:last?.created_at||'',last_by:last?.actor_name||'',last_requirement_version:last?parse(last.after_json).requirement_version:null};}
 return items;
}
export function historyEnvironment(events,{missingResults=false}={}){
 const reads=[];const env={DB:{prepare(sql){return{bind(...args){return{async all(){reads.push({sql,args});return missingResults?{}:{results:events};}};}};}}};
 return {env,reads};
}
export async function historyFixture(size,histories=10){
 const items=[],events=[];
 for(let i=0;i<size;i++){
  const x={id:'SYN-PRINT-'+i,kind:'need',display_no:'ZY-SYN-'+i,source_type:['inventory','inbound','outbound',''][i%4],source_display_no:'SYN-SOURCE-'+i,customer:'合成客户'+i,title:'合成作业'+i,instructions:'合成原作业要求',status:['pending','assigned','working','closed','cancelled'][i%5]};
  if(i%3)x.requirement_version=2;
  if(i%7===0)x.cargo_groups={total_range:'SYN1-SYN2',box_count:2,groups:[{name:'合成组',range:'SYN1-SYN2',box_count:2,documents:[{file_name:'合成资料.pdf'}]}]};
  items.push(x);const key=await workNeedDocumentKey(x);
  if(i%11===0)continue; // Existing records with no known print history.
  for(let j=0;j<histories;j++){
   const payload=j%5===0?'{broken':j%5===1?'':j%5===2?'[]':JSON.stringify({document_key:i%3===0?'old-version':key,...(j%2?{requirement_version:j+1}:{})});
   events.push({record_id:x.id,request_id:'SYN-REQUEST-'+String(i).padStart(6,'0')+'-'+String(j).padStart(3,'0'),created_at:'2026-11-03T00:00:'+String(j%60).padStart(2,'0')+'.000Z',actor_name:'合成打印员'+j,after_json:payload});
  }
 }
 events.sort((a,b)=>b.created_at.localeCompare(a.created_at)||b.request_id.localeCompare(a.request_id));
 return {items,events};
}
