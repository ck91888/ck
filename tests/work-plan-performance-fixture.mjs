// Entirely fictional relationship graph. Deliberately shuffled order positions,
// repeated links, unrelated orders, partial receipts, corrections and missing sources.
export function factsFixture(size){
 const plans=[],orders=[],results=[],tasks=[],groups=[];
 for(let i=0;i<size;i++){
  const p='PERF-IB-'+i,o='PERF-OB-'+i,at='2026-11-03T00:00:00.000Z';
  plans.push({id:p,display_no:p,external_inbound_no:'SYN-EXT-'+i,biz_classes_json:JSON.stringify(i%3?['bulk_putaway']:['bulk','return']),status:i%5?'partially_completed':'completed',expected_arrival:'2026-11-03',unload_completed_at:i%7?'':at,manual_completed_at:i%11?'':at,is_deleted:i%17===0?1:0});
  orders.push({id:o,display_no:o,status:['issued','pending_issue','shipped','cancelled','completed'][i%5],expected_ship_at:i%4?'2026-11-05':'',source_inbound_plan_id:p,is_deleted:i%13===0?1:0});
  results.push({plan_id:p,job_id:'PERF-J-'+i,job_type:'unload',result_id:'PERF-R-'+i,created_at:at,result_lines_json:JSON.stringify([{actual_qty:2}]),source:i%19?'':'manual_correction',inbound_external_no:'SYN-J-REF-'+i});
  if(i%3)results.push({plan_id:p,job_id:'PERF-PJ-'+i,job_type:'inbound_bulk',result_id:'PERF-PR-'+i,created_at:'2026-11-03T16:00:00.000Z',result_lines_json:JSON.stringify([{putaway_qty:1}])});
  tasks.push({plan_id:p,status:'completed',completion_source:i%23?'':'manual_force',completed_by:i%29?'合成操作员':'(legacy)'});
  const type=['inbound','outbound','inventory','standalone'][i%4],source=type==='inbound'?p:type==='outbound'?o:type==='inventory'?'SYN-STOCK-'+i:'';
  groups.push({key:'SYN-GROUP-'+i,source_type:type,source_id:source,customer:'合成客户'+i,display_no:'SYN-G-'+i,status:['pending','working','completed'][i%3],updated_at:at,count:2,completed:0,items:[{id:'SYN-NEED-'+i,title:'合成作业'+i,files:[{id:'SYN-FILE-'+i,file_key:'synthetic/'+i}],links:[{outbound_id:'PERF-OB-'+((i+7)%size)},{outbound_id:o},{outbound_id:o}]}]});
 }
 // A repeated order ID is legal input to this read helper. Preserve all rows,
 // while excluding the same array position matched via both source and a link.
 if(size>3)orders.splice(2,0,{...orders[0],display_no:'SYN-REPEATED-ID',expected_ship_at:'2026-11-02'});
 orders.reverse();results.reverse();tasks.reverse();
 const knownPlans=plans.filter((_,i)=>i%2===0),reads=[];
 const rowsFor=s=>{const ids=new Set(JSON.parse(s.args[0]));return s.sql.includes('v2_inbound_plan_biz_tasks')?tasks.filter(t=>ids.has(t.plan_id)):s.sql.includes('v2_inbound_plan_jobs')?results.filter(r=>ids.has(r.plan_id)):plans.filter(p=>ids.has(p.id));};
 const prepare=sql=>{const s={sql,args:[],bind(...args){s.args=args;return s;},async all(){reads.push({sql,args:s.args});return {results:rowsFor(s)};}};return s;};
 const env={DB:{prepare,async batch(ss){return Promise.all(ss.map(s=>s.all()));}}};
 return {env,groups,knownPlans,orders,plans,results,tasks,reads};
}
