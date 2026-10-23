import {employeeDepartments} from './employee-attendance.js';
import {jobCustomers} from './employee-workday.js';
import {dashboardRange} from './dashboard-time.js';
import {canonicalDailyBadge} from '../shared/badge-identity.js';
import {parseOpsResultForExport} from './report-results.js';
import {jobNumbers,jobBusinessFilter} from './document-numbers.js';
import {executionDepartment} from './work-execution.js';
const DAY=86400000,MAX=20000,MAX_JOBS=5000;
const rows=async(env,sql,...args)=>(await env.DB.prepare(sql).bind(...args).all()).results||[];
const parse=x=>{try{return JSON.parse(x||'{}');}catch{return {};}};
const iso=n=>Number.isFinite(n)?new Date(n).toISOString():'';
export const kstDate=n=>iso((typeof n==='number'?n:Date.parse(n))+9*3600000).slice(0,10);
const round=n=>Math.round(n*100)/100;
const sumSeconds=(list,key='seconds')=>list.reduce((n,s)=>n+Math.round(s[key]*1000),0)/1000;
const limitRows=(xs,max,label)=>{if(xs.length>max)throw Error(label+'超过安全上限 '+max+' 行，请缩小日期或筛选范围；未返回部分统计');return xs;};
const idOf=r=>canonicalDailyBadge(String(r.worker_id||'').trim())||'UNIDENTIFIED:'+r.id;
const inside=(x,range,cutoff)=>Number.isFinite(Date.parse(x))&&Date.parse(x)>=range.start&&Date.parse(x)<range.end&&Date.parse(x)<=cutoff;

// Keep milliseconds until final presentation; same-person overlap is reported, not silently allocated.
export function measureSegments(source,range,cutoff){
 const measured=source.map(r=>{
  const forced=String(r.leave_reason||'').startsWith('system_cutoff:'),a=Date.parse(r.joined_at),z=Date.parse(r.left_at),open=!r.left_at,invalid=!Number.isFinite(a)||!open&&(!Number.isFinite(z)||z<a);
  const start=Math.max(a,range.start),end=Math.min(z,range.end,cutoff),ms=forced||invalid||open?0:Math.max(0,end-start);
  const reasons=[];if(forced)reasons.push('系统04:00强制结束，工时待核实，排除有效工时与人效');if(invalid)reasons.push('时间缺失/无效/倒序，不能归属');if(open)reasons.push('未结束时段排除，不估算');if(!r.worker_id)reasons.push('人员身份未记录，无法核对同人重叠');
  if(!invalid&&!open&&(z-a)>=12*3600000)reasons.push('单段≥12小时，待核实');
  if(!invalid&&!open&&z>cutoff)reasons.push('结束晚于截止时间，仅计截止前部分（暂算）');
  return {...r,worker_id:idOf(r),source_worker_id:r.worker_id||'',clipped_start:iso(start),clipped_end:ms>0?iso(end):'',seconds:ms/1000,minutes:ms/60000,raw_minutes:invalid||open?null:(z-a)/60000,active:open?1:0,provisional:forced||!open&&z>cutoff?1:0,overlap_seconds:0,anomaly_reason:reasons.join('；'),anomaly:reasons.length?1:0,long_segment:ms>=240*60000?1:0,_a:start,_z:end};
 });
 // Sweep-line: mark every intersecting clock, including other tasks hidden by later filters.
 const groups=new Map();for(const s of measured)if(s.seconds>0){const list=groups.get(s.worker_id)||[];list.push(s);groups.set(s.worker_id,list);}
 for(const list of groups.values()){
  const events=[];for(const s of list){events.push({at:s._a,s,delta:1},{at:s._z,s,delta:-1});}
  events.sort((a,b)=>a.at-b.at||a.delta-b.delta);const active=new Set();let last=events[0]?.at;
  for(const e of events){if(e.at>last&&active.size>1)for(const s of active)s.overlap_seconds+=(e.at-last)/1000;if(e.delta>0)active.add(e.s);else active.delete(e.s);last=e.at;}
 }
 for(const s of measured){if(s.overlap_seconds){s.anomaly=1;s.anomaly_reason+=(s.anomaly_reason?'；':'')+'同一人员时段重叠（含其他筛选外任务），未自动分摊';}delete s._a;delete s._z;}
 return measured;
}
function groupTotals(list,key){
 const groups=new Map();for(const s of list){const k=s[key]||'other',g=groups.get(k)||{[key]:k,worker_name:s.worker_name,total_seconds:0,overlap_seconds:0,job_ids:new Set(),worker_ids:new Set(),max_segment_minutes:0};g.total_seconds=Math.round((g.total_seconds+s.seconds)*1000)/1000;g.overlap_seconds=Math.round((g.overlap_seconds+s.overlap_seconds)*1000)/1000;g.job_ids.add(s.job_id);g.worker_ids.add(s.worker_id);g.max_segment_minutes=Math.max(g.max_segment_minutes,s.minutes);groups.set(k,g);}
 return [...groups.values()].map(g=>{const {job_ids,worker_ids,...rest}=g;return {...rest,total_minutes:round(g.total_seconds/60),total_hours:round(g.total_seconds/3600),job_count:job_ids.size,worker_count:worker_ids.size,avg_minutes_per_job:round(g.total_seconds/60/job_ids.size),avg_minutes_per_worker:round(g.total_seconds/60/worker_ids.size)};}).sort((a,b)=>b.total_seconds-a.total_seconds);
}
function taskGroupTotals(segments,tasks,key){
 const groups=new Map(groupTotals(segments,key).map(g=>[g[key],g]));
 for(const task of tasks)if(!groups.has(task[key]))groups.set(task[key],{[key]:task[key],total_seconds:0,total_minutes:0,total_hours:0,overlap_seconds:0,worker_count:0,max_segment_minutes:0});
 return [...groups.values()].map(g=>{const own=tasks.filter(t=>t[key]===g[key]);return {...g,job_count:own.length,gap_task_count:own.filter(t=>t.data_gaps.length).length,missing_segment_task_count:own.filter(t=>t.hour_state==='unavailable').length};}).sort((a,b)=>b.total_seconds-a.total_seconds);
}
function dailySegments(segs){
 const out=[];for(const s of segs){if(!s.seconds){out.push({...s,work_date:kstDate(s.joined_at)});continue;}let a=Date.parse(s.clipped_start),z=Date.parse(s.clipped_end);while(a<z){const end=Math.min(z,Date.parse(kstDate(a)+'T00:00:00+09:00')+DAY);out.push({...s,work_date:kstDate(a),clipped_start:iso(a),clipped_end:iso(end),seconds:(end-a)/1000,minutes:(end-a)/60000,overlap_seconds:undefined,overlap_note:s.overlap_seconds?'原始时段存在同人重叠，请核查':'',segment_overlap_seconds:s.overlap_seconds});a=end;}}
 return out;
}
async function digest(value){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))].map(x=>x.toString(16).padStart(2,'0')).join('');}
export async function actualWorkReport(body,env){
 const range=dashboardRange(body.start_date,body.end_date);
 if(!Number.isFinite(range.start)||!Number.isFinite(range.end)||range.end-range.start>31*DAY)throw Error('实际作业口径需填写开始和结束日期，单次最多31天（KST）；请分期查询');
 const cutoff=body.as_of?Date.parse(body.as_of):Date.now();if(!Number.isFinite(cutoff)||cutoff>Date.now()+1000)throw Error('截止时间无效或晚于当前时间');
 const start=iso(range.start),end=iso(Math.min(range.end,cutoff));
 const source=limitRows(await rows(env,`SELECT w.*,j.job_type,j.status,j.flow_stage,j.biz_class FROM v2_ops_job_workers w JOIN v2_ops_jobs j ON j.id=w.job_id
 WHERE (julianday(w.joined_at)<julianday(?) AND (COALESCE(w.left_at,'')='' OR julianday(w.left_at)>julianday(?)))
 OR ((julianday(w.joined_at) IS NULL OR (COALESCE(w.left_at,'')<>'' AND (julianday(w.left_at) IS NULL OR julianday(w.left_at)<julianday(w.joined_at)))) AND julianday(j.created_at)>=julianday(?) AND julianday(j.created_at)<julianday(?))
 ORDER BY w.job_id,w.joined_at,w.id LIMIT ${MAX+1}`,end,start,start,end),MAX,'区间人员时段');
 const measured=measureSegments(source,range,cutoff),ids=JSON.stringify([...new Set(source.map(s=>s.job_id))]);
 let filter='';const args=[ids,start,end,start,end];
 if(body.job_id){filter+=' AND j.id=?';args.push(String(body.job_id));}
 for(const key of ['flow_stage','job_type','status'])if(body[key]){filter+=' AND j.'+key+'=?';args.push(String(body[key]));}
 if(body.doc_no){const f=jobBusinessFilter(env,'%'+String(body.doc_no)+'%');filter+=' AND '+f.sql;args.push(...f.args);}
 let jobs=limitRows(await rows(env,`SELECT j.*,s.state AS report_dispatch_state,s.department AS report_saved_department,s.kind AS report_record_kind FROM v2_ops_jobs j LEFT JOIN sop_records s ON s.id=j.id AND s.kind IN ('task','dispatch')
 WHERE (j.id IN (SELECT value FROM json_each(?)) OR (julianday(j.finished_at)>=julianday(?) AND julianday(j.finished_at)<julianday(?)) OR (NOT EXISTS(SELECT 1 FROM v2_ops_job_workers w WHERE w.job_id=j.id) AND julianday(COALESCE(NULLIF(j.finished_at,''),NULLIF(j.updated_at,''),j.created_at))>=julianday(?) AND julianday(COALESCE(NULLIF(j.finished_at,''),NULLIF(j.updated_at,''),j.created_at))<julianday(?))) ${filter} ORDER BY j.created_at DESC,j.id LIMIT ${MAX_JOBS+1}`,...args),MAX_JOBS,'区间任务');
 const department=j=>{const s=parse(j.report_dispatch_state),record=j.report_saved_department?executionDepartment({state:j.report_dispatch_state,department:j.report_saved_department,kind:j.report_record_kind}):'';const d=s.labor_department||record||j.biz_class;return (j.job_type==='office_order_processing'?Object.hasOwn(employeeDepartments,d):['bulk','direct_ship','import'].includes(d))?d:d==='return'?'direct_ship':'other';};
 jobs=jobs.map(j=>({...j,department:department(j)})).filter(j=>!body.department||j.department===body.department);
 const name=String(body.worker_name||'').toLowerCase();
 if(name)jobs=jobs.filter(j=>measured.some(s=>s.job_id===j.id&&String(s.worker_name||'').toLowerCase().includes(name)));
 const jobIds=JSON.stringify(jobs.map(j=>j.id)),jobMap=new Map(jobs.map(j=>[j.id,j]));
 const selected=measured.filter(s=>jobMap.has(s.job_id)&&(!name||String(s.worker_name||'').toLowerCase().includes(name))).map(s=>({...s,department:jobMap.get(s.job_id).department}));
 const [allWorkers,resultRows]=await Promise.all([
 rows(env,`SELECT * FROM v2_ops_job_workers WHERE job_id IN (SELECT value FROM json_each(?)) ORDER BY job_id,joined_at,id LIMIT ${MAX+1}`,jobIds),
 rows(env,`SELECT * FROM v2_ops_job_results WHERE job_id IN (SELECT value FROM json_each(?)) ORDER BY job_id,created_at,id LIMIT ${MAX+1}`,jobIds)]);
 limitRows(allWorkers,MAX,'相关任务累计人员时段');limitRows(resultRows,MAX,'相关任务结果');
 const byId=(list)=>{const m=new Map();for(const r of list){const a=m.get(r.job_id)||[];a.push(r);m.set(r.job_id,a);}return m;};
 const workers=byId(allWorkers),results=byId(resultRows),period=byId(selected),quantities=new Map();
 const tables=new Set((await rows(env,"SELECT name FROM sqlite_master WHERE type='table'")).map(x=>x.name));
 const customers=await jobCustomers(env,jobs,tables);
 const tasks=jobs.map(j=>{
  const ws=workers.get(j.id)||[],ss=period.get(j.id)||[],rs=results.get(j.id)||[],p=parseOpsResultForExport(j.job_type,rs,j),gaps=[];
  const seconds=sumSeconds(ss),overlap=sumSeconds(ss,'overlap_seconds');
  if(!ws.length)gaps.push('缺人员分段，历史工时不可归属；候选按完成/更新时间列出');
  if(ws.some(w=>!w.left_at))gaps.push('有未结束时段，已排除');
  if(ws.some(w=>!Number.isFinite(Date.parse(w.joined_at))||w.left_at&&(!Number.isFinite(Date.parse(w.left_at))||Date.parse(w.left_at)<Date.parse(w.joined_at))))gaps.push('有缺失/无效时段');
  if(!j.finished_at&&j.status==='completed')gaps.push('完成时间未记录');
  if(overlap)gaps.push('同人重叠待核，工时含重复部分');
  if(ss.some(s=>s.anomaly))gaps.push('人员时段异常待核');
  if(p.quantity_state==='missing')gaps.push('数量未记录，不等于零产出');
  if(p.quantity_warnings.length)gaps.push(...p.quantity_warnings);
  if(ws.some(w=>String(w.leave_reason||'').startsWith('system_cutoff:')))gaps.push('系统04:00强制结束工时待核实，排除人效');
  const completed= j.status==='completed'&&inside(j.finished_at,range,cutoff);
  const wholly=ws.length>0&&ws.every(w=>Number.isFinite(Date.parse(w.joined_at))&&Date.parse(w.joined_at)>=range.start&&w.left_at&&Date.parse(w.left_at)>=Date.parse(w.joined_at)&&Date.parse(w.left_at)<=Math.min(range.end,cutoff));
  if(rs.some(r=>r.source!=='manual_correction'&&!inside(r.created_at,range,cutoff)))gaps.push('结果记录跨期，无法与本期工时精确配对');
  const hasComparableMetric=p.quantity_metrics.some(m=>m.phase==='finish'&&m.unit!=='单位未记录');
  const comparable=completed&&wholly&&!gaps.length&&!name&&seconds>0&&hasComparableMetric;
  const efficiency_reason=p.quantity_state==='time_only'?'仅记录工时，不采集数量或计算效率':comparable?'完整同类任务样本；仅结束实绩可比':name?'按人员筛选，任务总产出不可配给个人':!wholly?'跨期或缺时段，不计算效率':!completed?'非本期明确完成，不计算效率':!hasComparableMetric?'仅开始快照或无明确单位的结束数量，不计算效率':gaps.join('；');
  for(const m of p.quantity_metrics){const k=[j.department,j.job_type,p.inventory_type||'',m.metric,m.unit,m.phase].join('|');const g=quantities.get(k)||{department:j.department,job_type:j.job_type,inventory_type:p.inventory_type||'',metric:m.metric,label:m.label,unit:m.unit,phase:m.phase,task_cumulative_quantity:0,completed_in_period_quantity:0,comparable_quantity:0,comparable_seconds:0,sample_count:0,recorded_task_count:0};g.task_cumulative_quantity+=m.quantity;g.recorded_task_count++;if(completed)g.completed_in_period_quantity+=m.quantity;if(comparable&&m.phase==='finish'&&m.unit!=='单位未记录'){g.comparable_quantity+=m.quantity;g.comparable_seconds+=seconds;g.sample_count++;}quantities.set(k,g);}
  return {id:j.id,job_id:j.id,customers:customers.get(j.id)||[],customer:(customers.get(j.id)||[]).join('、'),hour_state:!ws.length?'unavailable':gaps.length?'needs_review':'recorded',display_no:j.display_no,created_at:j.created_at,finished_at:j.finished_at,job_type:j.job_type,flow_stage:j.flow_stage,biz_class:j.biz_class,status:j.status,department:j.department,worker_count:new Set(ss.map(s=>s.worker_id)).size,worker_names:[...new Set(ss.map(s=>s.worker_name).filter(Boolean))].join('、'),total_seconds:seconds,total_minutes:round(seconds/60),total_hours:round(seconds/3600),overlap_seconds:overlap,started_at:ws[0]?.joined_at||'',ended_at:ws.reduce((z,w)=>w.left_at>z?w.left_at:z,''),recorded_lifetime_minutes:ws.reduce((n,w)=>n+(Number(w.minutes_worked)||0),0),...p,quantity_scope:p.quantity_state==='time_only'?'仅记录工时':'任务累计；非周内产量',completed_in_period:completed,comparable,efficiency_reason,data_gaps:gaps,result_count:rs.length};
 });
 const total_seconds=sumSeconds(selected),overlap_seconds=sumSeconds(selected,'overlap_seconds'),snapshot=await digest(JSON.stringify([source,jobs,allWorkers,resultRows,[...customers]]));
 if(body.snapshot&&body.snapshot!==snapshot)throw Error('查询期间数据已变化，请重新查询并导出；未生成部分文件');
 const metadata={basis:'actual_work',basis_label:'按实际作业日期（人员时段与区间交集）',timezone:'Asia/Seoul (UTC+09:00)',start_inclusive:start,end_exclusive:iso(range.end),as_of:iso(cutoff),snapshot,open_policy:'未结束时段排除，不估算',overlap_policy:'同一人员重叠显式告警，记录工时含重复部分；多人并行分别计人时',quantity_policy:'任务累计产出及本期完成任务产出均非周内产量；不按工时比例分配。开始快照不算完成实绩。',gap_policy:'仅有累计或缺分段的历史记录不可归属；未归属部门不表示闲置。候选范围外完全无日期证据的缺失无法检索。',consistency:'固定计算截止时间；源记录变化时拒绝跨页混合，历史状态不回填'};
 const summary={total_seconds,total_minutes:round(total_seconds/60),total_hours:round(total_seconds/3600),overlap_seconds,worker_count:new Set(selected.map(s=>s.worker_id)).size,job_count:tasks.length,avg_minutes_per_worker:round(total_seconds/60/(new Set(selected.map(s=>s.worker_id)).size||1)),max_segment_minutes:Math.max(0,...selected.map(s=>s.minutes)),anomaly_count:selected.filter(s=>s.anomaly).length,long_segment_count:selected.filter(s=>s.long_segment).length,excluded_open_count:selected.filter(s=>s.active).length,missing_quantity_tasks:tasks.filter(t=>t.quantity_state==='missing').length,gap_task_count:tasks.filter(t=>t.data_gaps.length).length};
 const offset=body.job_id?0:Math.max(0,Math.trunc(Number(body.offset)||0)),limit=Math.max(1,Math.min(200,Math.trunc(Number(body.limit)||200))),kind=body.report_kind||'tasks';
 const quantityRows=[...quantities.values()].map(g=>({...g,qty_per_hour:g.sample_count?round(g.comparable_quantity/(g.comparable_seconds/3600)):null}));
 const detail=kind==='segments'?dailySegments(selected):kind==='quantities'?quantityRows:tasks,total=detail.length;
 let page=detail.slice(offset,offset+limit);
 // Only decorate a page; never hydrate every historical task in the browser.
 if(kind!=='quantities')page=await jobNumbers(env,page,kind==='segments'?'job_id':'id');
 const response={ok:true,metadata,summary,by_worker:groupTotals(selected,'worker_id'),by_job_type:taskGroupTotals(selected,tasks,'job_type'),by_department:taskGroupTotals(selected,tasks,'department'),quantities:[...quantities.values()].map(g=>({...g,qty_per_hour:g.sample_count?round(g.comparable_quantity/(g.comparable_seconds/3600)):null})),items:page,rows:page,segments:kind==='segments'?page:[],total,offset,limit,has_more:offset+page.length<total,truncated:false};
 if(JSON.stringify(response).length>8000000)throw Error('本页明细超过8MB安全上限，请减小每页数量；未截断内容');
 return response;
}
