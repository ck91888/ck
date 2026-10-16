// Read-only business evidence for an authorized work-plan group. Never infer
// receipt dates from creation/update timestamps or match unrelated customers.
const q=(e,s,...a)=>e.DB.prepare(s).bind(...a),parse=(s,f=[])=>{try{return JSON.parse(s||'null')??f;}catch{return f;}};
export const workPlanListEnabled=(e,u)=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_WORK_CHAIN_ENABLED==='true'&&u.scope!=='field';
export function workPlanDay(value){
 const s=String(value||'').trim();if(!s)return '';
 let day;if(/^\d{4}[/-]\d{2}[/-]\d{2}$/.test(s))day=s.replaceAll('/','-');
 else if(/^\d{4}-\d{2}-\d{2}[T ]/.test(s)&&!/[zZ]$|[+-]\d{2}:?\d{2}$/.test(s.slice(11)))day=s.slice(0,10);
 else{const d=new Date(s);if(!Number.isFinite(d.getTime()))return '';day=new Date(d.getTime()+9*3600000).toISOString().slice(0,10);}
 return /^\d{4}-\d{2}-\d{2}$/.test(day)&&Number.isFinite(Date.parse(day))&&new Date(day).toISOString().slice(0,10)===day?day:'';
}
const positive=(lines,key)=>Array.isArray(lines)&&lines.some(x=>Number(x[key])>0);
const days=values=>[...new Set(values.filter(Boolean))].sort();
const add=(list,event)=>{if(event.day&&!list.some(x=>x.key===event.key))list.push(event);};
export async function workPlanFacts(e,groups,knownPlans,orders){
 const plans=[...knownPlans],ids=new Set(plans.map(p=>p.id));
 const missing=[...new Set(groups.filter(g=>g.source_type==='outbound').flatMap(g=>orders.filter(o=>o.id===g.source_id).map(o=>o.source_inbound_plan_id)).filter(id=>id&&!ids.has(id)))];
 if(missing.length)plans.push(...(await q(e,'SELECT * FROM v2_inbound_plans WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(missing)).all()).results);
 const planIds=JSON.stringify(plans.map(p=>p.id));
 const [results,tasks]=await e.DB.batch([
  q(e,`SELECT j.plan_id,j.id AS job_id,j.job_type,j.inbound_external_no,j.display_no AS job_no,r.id AS result_id,r.source,r.created_at,r.result_lines_json,
    l.plan_id AS linked_plan_id,l.result_json AS linked_result,t.finished_at
    FROM v2_inbound_plan_jobs j LEFT JOIN v2_ops_job_results r ON r.job_id=j.id
    LEFT JOIN ck_unload_plan_links l ON l.job_id=j.id AND l.plan_id=j.plan_id LEFT JOIN ck_unload_trips t ON t.job_id=j.id
    WHERE j.plan_id IN (SELECT value FROM json_each(?)) AND j.status='completed' AND j.job_type IN ('unload','inbound_direct','inbound_bulk')`,planIds),
  q(e,'SELECT plan_id,biz_class,status,completed_at,completion_source,completed_by,job_id FROM v2_inbound_plan_biz_tasks WHERE plan_id IN (SELECT value FROM json_each(?))',planIds)
 ]);
 const facts=new Map();for(const p of plans){
  const receipt=[],putaway=[],rows=results.results.filter(r=>r.plan_id===p.id);
  for(const r of rows){
   if(r.source==='manual_correction')continue; // A later correction is not a new arrival.
   const own=parse(r.linked_result,{}),lines=r.linked_plan_id?(own.result_lines||[]):parse(r.result_lines_json);
   const at=r.job_type==='unload'&&r.finished_at?r.finished_at:r.created_at,day=workPlanDay(at);
   const event={key:r.job_type==='unload'?r.job_id:r.result_id,job_id:r.job_id,result_id:r.result_id||'',plan_id:p.id,plan_no:p.display_no||p.id,at:at||'',day,reference:r.inbound_external_no||r.job_no||'',kind:r.job_type==='unload'?'unload_result':'putaway_result'};
   if(r.job_type==='unload'&&positive(lines,'actual_qty'))add(receipt,event);
   if(r.job_type!=='unload'&&positive(lines,'putaway_qty'))add(putaway,event);
  }
  if(!receipt.length&&workPlanDay(p.unload_completed_at))add(receipt,{key:'plan:'+p.id,plan_id:p.id,plan_no:p.display_no||p.id,at:p.unload_completed_at,day:workPlanDay(p.unload_completed_at),kind:'plan_unload',reference:''});
  const parsedClasses=parse(p.biz_classes_json),classes=Array.isArray(parsedClasses)&&parsedClasses.length?parsedClasses:p.biz_class?[p.biz_class]:[],requires=classes.some(c=>['direct_ship','bulk_putaway'].includes(c))||putaway.length>0;
  const legacy=tasks.results.some(t=>t.plan_id===p.id&&t.status==='completed'&&(t.completion_source==='manual_force'||t.completed_by==='(legacy)'))||!!p.manual_completed_at||!!p.force_completed_at;
  facts.set(p.id,{id:p.id,display_no:p.display_no||p.id,external_no:p.external_inbound_no||'',status:p.status,is_deleted:!!p.is_deleted,expected:workPlanDay(p.expected_arrival),receipt:receipt.sort((a,b)=>a.at.localeCompare(b.at)),putaway:putaway.sort((a,b)=>a.at.localeCompare(b.at)),requires_putaway:requires,legacy_completion:legacy,completion_recorded:p.status==='completed'});
 }
 for(const g of groups){
  const linked=new Set(g.items.flatMap(n=>(n.links||[]).map(l=>l.outbound_id)));if(g.source_type==='outbound')linked.add(g.source_id);
  const shipmentOrders=orders.filter(o=>linked.has(o.id)||g.source_type==='inbound'&&o.source_inbound_plan_id===g.source_id);
  const origins=g.source_type==='inbound'?[g.source_id]:g.source_type==='outbound'?shipmentOrders.filter(o=>o.id===g.source_id).map(o=>o.source_inbound_plan_id):[];
  const originFacts=[...new Set(origins.filter(Boolean))].map(id=>facts.get(id)).filter(Boolean);
  const shipments=shipmentOrders.map(o=>({id:o.id,display_no:o.display_no||o.id,status:o.status,day:workPlanDay(o.expected_ship_at),source_inbound_plan_id:o.source_inbound_plan_id||'',deleted:!!o.is_deleted}));
  const pending=shipments.filter(o=>!o.deleted&&!['shipped','completed','cancelled'].includes(o.status));
  g.timeline={plans:originFacts,missing_source:!!origins.filter(Boolean).length&&!originFacts.length,
   receipt_days:days(originFacts.flatMap(p=>p.receipt.map(x=>x.day))),putaway_days:days(originFacts.flatMap(p=>p.putaway.map(x=>x.day))),expected_days:days(originFacts.map(p=>p.expected)),
   shipments,pending_count:pending.length,undated_count:pending.filter(o=>!o.day).length,earliest_ship:days(pending.map(o=>o.day))[0]||'',
   shipment_state:!pending.length?'none':pending.some(o=>o.day)?'scheduled':'undated'};
 }
 return groups;
}
export function filterWorkPlans(groups,b){
 const ranges={};for(const name of ['receipt','putaway','expected','ship']){
  const from=String(b[name+'_from']||'').trim(),to=String(b[name+'_to']||'').trim();
  if([from,to].some(s=>s&&(!/^\d{4}-\d{2}-\d{2}$/.test(s)||workPlanDay(s)!==s))||from&&to&&from>to)throw Error('日期范围无效，请核实起止日期 / 날짜 범위를 확인하세요');ranges[name]={from,to};
 }
 const customer=String(b.customer||'').trim().toLowerCase(),reference=String(b.reference||'').trim().toLowerCase(),status=String(b.status||''),shipping=String(b.shipping||'');
 if(status&&!['pending','working','completed','cancelled'].includes(status)||shipping&&!['none','scheduled','undated'].includes(shipping))throw Error('筛选状态无效，请刷新 / 필터 상태를 확인하세요');
 return groups.filter(g=>{
  const t=g.timeline;if(customer&&!String(g.customer||'').toLowerCase().includes(customer)||status&&g.status!==status)return false;
  const refs=[g.display_no,g.source_id,...g.items.flatMap(n=>[n.display_no,n.source_display_no,n.supply_chain_no]),...t.plans.flatMap(p=>[p.display_no,p.external_no,...p.putaway.map(x=>x.reference)]),...t.shipments.map(o=>o.display_no)].filter(Boolean).join(' ').toLowerCase();
  if(reference&&!refs.includes(reference))return false;
  if(shipping==='none'&&t.pending_count||shipping==='scheduled'&&!t.earliest_ship||shipping==='undated'&&!t.undated_count)return false;
  return Object.entries(ranges).every(([name,{from,to}])=>!from&&!to||(name==='ship'?[t.earliest_ship]:t[name+'_days']).some(d=>d&&(!from||d>=from)&&(!to||d<=to)));
 });
}
