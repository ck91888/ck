import {syncGroupReservations} from './cargo-reservations.js';
import {taskGroups,taskProcess,allProcessesApproved} from './cargo-processes.js';
import {chainNeed,chainEvent,workChainEnabled} from './work-chain.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args),text=v=>String(v??'').trim();
const actions=new Set(['sop_cargo_group_complete','sop_cargo_group_review','sop_cargo_job_finish','sop_cargo_groups_ack']);
const fields=['label_count','packed_box_count','total_operated_box_count','pallet_count','used_carton_large_count','used_carton_small_count','packed_sku_count','repaired_box_count','reboxed_count','forklift_location_count'];
export async function cargoTask(env,row,u,requested=''){
 const id=requested||row.data.task_id;if(requested&&!new Set([row.data.task_id,...(row.data.task_ids||[])]).has(requested))throw Error('任务不属于此计划');
 const task=id?await q(env,"SELECT * FROM sop_records WHERE id=? AND kind='task'",id).first():null;
 if(task){task.data=JSON.parse(task.state);if(task.data.need_id!==row.id)throw Error('任务不属于此计划');}
 const owner=!!task&&(u.role==='manager'||task.data.owner_id===u.id||(task.data.delegates||[]).includes(u.id));
 return {task,can_complete:owner&&['manager','dispatcher','reviewer'].includes(u.role),can_review:!!task&&(['manager','reviewer'].includes(u.role)||u.role==='dispatcher'&&owner)};
}
export async function cargoExecution(b,env,u){
 if(!actions.has(b.action))return null;
 if(!workChainEnabled(env))throw Error('资料分组未启用');
 const row=await chainNeed(env,b.id,u),data=structuredClone(row.data),cargo=data.cargo_groups;
 if(!cargo)throw Error('此计划未设置资料分组，请沿用整单操作');
 const {task,can_complete,can_review}=await cargoTask(env,row,u,b.task_id),request=text(b.client_req_id);
 if(!request)throw Error('缺少请求编号');
 const fingerprint=JSON.stringify({id:row.id,task_id:b.task_id||'',revision:b.revision,group_id:b.group_id||'',group_ids:b.group_ids||[],quantity:b.quantity,unit:b.unit,description:b.description||'',location:b.location||'',decision:b.decision||'',reason:b.reason||'',counts:b.counts||{}});
 const previous=async()=>{const p=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(!p)return null;const result=JSON.parse(p.result_json);if(p.actor_id!==u.id||p.record_id!==row.id||p.action!==b.action||result.fingerprint!==fingerprint)throw Error('请求编号冲突');return result;};
 const prior=await previous();if(prior)return prior;
 if(row.revision!==Number(b.revision))throw Error('分组已更新，请刷新后核对');
 if(!task||(task.data.status==='cancelled'||task.data.status==='completed'&&!['sop_cargo_group_review','sop_cargo_groups_ack'].includes(b.action))||['cancelled','closed'].includes(data.status))throw Error('请先派工并开始有效任务');
 if(b.action==='sop_cargo_group_review'?!can_review:!can_complete)throw Error('仅本任务派审员或获授权人员可以操作');
 const t=new Date().toISOString(),extra=[],selectedGroups=taskGroups(cargo,task),group=selectedGroups.find(g=>g.id===b.group_id);
 const processFor=g=>taskProcess(g,task),resultFor=g=>processFor(g)?.result||g.outputs?.find(o=>o.id===g.current_output_id);
 if(b.action==='sop_cargo_group_complete'&&!group)throw Error('资料组不属于此计划');
 if(b.action==='sop_cargo_groups_ack'){
  if(!Array.isArray(b.group_ids)||!b.group_ids.length||new Set(b.group_ids).size!==b.group_ids.length)throw Error('请选择待确认组');
  for(const id of b.group_ids){const g=selectedGroups.find(x=>x.id===id);if(!g)throw Error('资料组不属于此计划');g.ack_version=g.mapping_version||1;g.ack={by:u.name,actor_id:u.id,at:t};}
 }else if(b.action==='sop_cargo_group_complete'){
  if(task.data.status!=='working')throw Error('任务暂停或尚未开工，不能提交完成');
  if(group.ack_version!==(group.mapping_version||1))throw Error('请先现场确认此组最新资料版本');
  const process=processFor(group),current=process?process.result:resultFor(group);if(group.processes&&!process)throw Error('本任务未分配此组工序');if(current&&current.status!=='returned')throw Error('此组已提交成果；不得重复登记货物');
  const quantity=Number(b.quantity),unit=text(b.unit);if(!Number.isSafeInteger(quantity)||quantity<1||!['箱','托'].includes(unit))throw Error('请填写实际成果数量与单位，箱数不能直接当托数');
  const output={id:'CGOUT-'+crypto.randomUUID(),version:process?(process.result_history?.length||0)+1:(group.outputs?.length||0)+1,status:'awaiting_review',quantity,unit,input_box_count:group.box_count,range:group.range,marks:group.marks,identifier:group.identifier,mapping_version:group.mapping_version||1,documents:group.documents,public_documents:group.public_documents||cargo.public_documents,description:text(b.description).slice(0,4000),location:text(b.location).slice(0,500),completed_by:u.name,completed_actor_id:u.id,completed_at:t};
  const finalProcess=!process||task.data.cargo_process_index===group.processes.length-1;
  if(process){process.result=output;process.result_history=[...(process.result_history||[]),output];process.status='awaiting_review';}
  if(finalProcess){group.outputs=[...(group.outputs||[]),output];group.current_output_id=output.id;group.status='awaiting_review';}else group.status='processing';
 }else if(b.action==='sop_cargo_group_review'){
  const ids=b.group_ids||[b.group_id];if(!Array.isArray(ids)||!ids.length||new Set(ids).size!==ids.length)throw Error('请选择待审核资料组');
  if(!['pass','return'].includes(b.decision)||!text(b.reason))throw Error('请填写审核结论与说明');
  for(const id of ids){const selected=selectedGroups.find(g=>g.id===id);if(!selected)throw Error('资料组不属于此计划');
   const process=processFor(selected),output=process?process.result:resultFor(selected);if(!output||output.status!=='awaiting_review')throw Error('此组没有待审核成果');
   if(output.mapping_version!==(selected.mapping_version||1)||selected.ack_version!==output.mapping_version)throw Error('资料版本已更新，请重新核对');
   output.status=b.decision==='pass'?'approved':'returned';output.review={decision:b.decision,reason:text(b.reason).slice(0,4000),actor_id:u.id,by:u.name,at:t};if(process){process.status=output.status;const historic=process.result_history?.find(o=>o.id===output.id);if(historic)Object.assign(historic,output);}const final=selected.outputs?.find(o=>o.id===output.id);if(final)Object.assign(final,output);selected.status=allProcessesApproved(selected)?'approved':process&&task.data.cargo_process_index<selected.processes.length-1?'pending':output.status;
  }
 }else{
  if(!['working','paused'].includes(task.data.status))throw Error('任务当前不能收尾');
  const outputs=selectedGroups.map(g=>resultFor(g));if(outputs.some(o=>o?.status!=='approved'))throw Error('仍有资料组未审核，请继续操作或暂停任务');
  const counts={};for(const f of fields){const n=Number(b.counts?.[f]||0);if(!Number.isSafeInteger(n)||n<0)throw Error('工耗须为非负整数');counts[f]=n;}
  if(!text(b.reason))throw Error('请填写收尾审核说明');
  const finalOutputs=selectedGroups.filter(g=>!g.processes||task.data.cargo_process_index===g.processes.length-1).map(g=>resultFor(g));
  const result={...counts,grouped:true,process_name:task.data.cargo_process_name||'作业',process_results:outputs.map(o=>({id:o.id,version:o.version,quantity:o.quantity,unit:o.unit,input_box_count:o.input_box_count})),group_outputs:finalOutputs.map(o=>({id:o.id,version:o.version,quantity:o.quantity,unit:o.unit,input_box_count:o.input_box_count})),customer:data.customer||'',description:text(b.reason).slice(0,4000),finished_at:t,by:u.name,used_forklift:b.counts?.used_forklift===true};
  task.data.result=result;task.data.status='completed';task.data.review={decision:'pass',reason:result.description,by:u.name,actor_id:u.id,at:t};const otherIds=(data.task_ids||[]).filter(id=>id!==task.id),others=otherIds.length?(await q(env,'SELECT state FROM sop_records WHERE id IN (SELECT value FROM json_each(?))',JSON.stringify(otherIds)).all()).results:[];const allDone=cargo.groups.every(g=>g.status==='approved')&&others.every(r=>JSON.parse(r.state).status==='completed');data.status=allDone?'waiting_customer':'assigned';if(allDone)data.cargo_job_finished_at=t;
  extra.push(q(env,"UPDATE v2_ops_job_workers SET left_at=?,minutes_worked=MAX(0,ROUND((julianday(?)-julianday(joined_at))*1440,1)),leave_reason='grouped_job_finished' WHERE job_id=? AND left_at=''",t,t,task.id));
  extra.push(q(env,"UPDATE v2_ops_jobs SET status='completed',active_worker_count=0,finished_at=?,updated_at=?,shared_result_json=?,result_summary=? WHERE id=?",t,t,JSON.stringify(result),JSON.stringify(result),task.id));
  extra.push(q(env,'INSERT INTO v2_ops_job_results(id,job_id,box_count,pallet_count,remark,result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)','RES-'+crypto.randomUUID(),task.id,finalOutputs.filter(o=>o.unit==='箱').reduce((n,o)=>n+o.quantity,0),finalOutputs.filter(o=>o.unit==='托').reduce((n,o)=>n+o.quantity,0),result.description,JSON.stringify({...result,job_type:task.data.job_type}),u.name,t));
 }
 if(b.action==='sop_cargo_group_review')extra.push(...await syncGroupReservations(env,data,t,u,{needId:row.id}));
 // Both plan and task revisions are claimed in the same batch, including pause races.
 task.data.cargo_last_action={action:b.action,group_id:group?.id||'',by:u.name,at:t};
 const result={ok:true,id:row.id,revision:row.revision+1,fingerprint};
 try{await env.DB.batch([...chainEvent(env,row,data,u,request,b.action,t,result),...chainEvent(env,task,task.data,u,request+'-task',b.action,t),...extra]);}catch(error){const committed=await previous();if(committed)return committed;throw error;}
 return result;
}
