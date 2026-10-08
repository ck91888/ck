import {chainNeed,chainEvent,workChainEnabled} from './work-chain.js';
import {cargoAvailability} from './cargo-allocation.js';
import {normalizeCargoGroups} from './cargo-groups.js';
import {cargoTask} from './cargo-execution.js';
const q=(env,sql,...a)=>env.DB.prepare(sql).bind(...a),text=v=>String(v??'').trim();
export async function cargoSplit(b,env,u){
 if(b.action!=='sop_cargo_group_split')return null;
 if(!workChainEnabled(env))throw Error('资料分组未启用');
 const row=await chainNeed(env,b.id,u),data=structuredClone(row.data),cargo=data.cargo_groups;
 if(!cargo||u.scope==='field'||!['manager','service'].includes(u.role))throw Error('请由办公室客服按实际范围拆组');
 const request=text(b.client_req_id),fingerprint=JSON.stringify({id:row.id,revision:b.revision,group_id:b.group_id,children:b.children,reason:b.reason});if(!request)throw Error('缺少请求编号');
 const previous=async()=>{const p=await q(env,'SELECT * FROM sop_events WHERE request_id=?',request).first();if(!p)return null;const r=JSON.parse(p.result_json);if(p.actor_id!==u.id||p.record_id!==row.id||p.action!==b.action||r.fingerprint!==fingerprint)throw Error('拆组请求编号冲突');return r;};const prior=await previous();if(prior)return prior;
 if(Number(b.revision)!==row.revision)throw Error('资料组已更新，请刷新核对');
 if(['closed','cancelled'].includes(data.status)||!text(b.reason))throw Error('请核对计划状态并填写拆组原因');
 const parent=cargo.groups.find(g=>g.id===b.group_id);if(!parent)throw Error('资料组不属于此计划');
 const available=await cargoAvailability(env,data);if(available.groups.find(g=>g.id===parent.id).allocated>0)throw Error('已预约或已发货组不能拆分；未发货请先取消原预约');
 if(parent.processes?.some(p=>p.status==='awaiting_review'))throw Error('待审核工序须先退回或审核，再拆组');
 const output=parent.outputs?.find(o=>o.id===parent.current_output_id);if(output?.status==='awaiting_review')throw Error('待审核成果请先退回核对，再拆分');
 if(!Array.isArray(b.children)||b.children.length<2||b.children.length>50)throw Error('请填写2至50个可识别的实际货物范围');
 const t=new Date().toISOString();let children=b.children.map(c=>({name:text(c.name),range:c.range,box_count:c.box_count,identifier:c.identifier,one_mark_one_box:c.one_mark_one_box===undefined?parent.one_mark_one_box:c.one_mark_one_box,process_names:parent.process_names,instructions:parent.instructions,attachment_ids:parent.attachment_ids,no_documents_reason:parent.no_documents_reason}));
 const normalized=normalizeCargoGroups({total_range:parent.range,groups:children});children=normalized.groups;
 if(normalized.box_count!==parent.box_count||normalized.coverage.unassigned.length)throw Error('拆组范围和输入箱数必须完整覆盖原组，不得遗漏或重复实物');
 if(output?.status==='approved'){
  const quantities=b.children.map(c=>Number(c.quantity));if(quantities.some(n=>!Number.isSafeInteger(n)||n<1)||quantities.reduce((a,b)=>a+b,0)!==output.quantity||b.children.some(c=>c.unit&&c.unit!==output.unit))throw Error('逐组填写实际成果数量，合计和单位须与原审核成果一致');
  children=children.map((g,i)=>{const o={...structuredClone(output),id:'CGOUT-'+crypto.randomUUID(),version:1,status:'awaiting_review',quantity:quantities[i],input_box_count:g.box_count,range:g.range,marks:g.marks,identifier:g.identifier,mapping_version:1,review:null,parent_output:{id:output.id,version:output.version,review:output.review},split_by:u.name,split_at:t};return {...g,outputs:[o],current_output_id:o.id,status:'awaiting_review'};});
 }
 const processTasks=[];for(const id of [...new Set((parent.processes||[]).map(p=>p.task_id).filter(Boolean))]){const task=await q(env,"SELECT * FROM sop_records WHERE id=? AND kind='task'",id).first();if(task){task.data=JSON.parse(task.state);processTasks.push(task);}}
 for(let i=0;i<(parent.processes?.length||0)-1;i++){const p=parent.processes[i];if(p.status==='approved'&&processTasks.find(t=>t.id===p.task_id)?.data.status!=='completed')throw Error('拆组前请先收尾已审核的前序真实任务，避免工耗重复');}
 children=children.map(g=>({...g,...(parent.processes?{processes:parent.processes.map((p,i)=>{if(i===parent.processes.length-1&&g.outputs?.length)return {...p,status:'awaiting_review',result:g.outputs[0],result_history:[g.outputs[0]]};if(p.status==='approved')return {...p,result:null,result_history:[],inherited_parent_review:{group_id:parent.id,output_id:p.result?.id,review:p.result?.review}};return {...p,result:null,result_history:[]};})}:{}),parent_group_id:parent.id,mapping_version:1,ack_version:data.task_id?0:undefined,documents:parent.documents,public_documents:parent.public_documents||cargo.public_documents,split_by:u.name,split_at:t}));
 cargo.groups=cargo.groups.flatMap(g=>g.id===parent.id?children:[g]);normalizeCargoGroups({total_range:cargo.total_range,groups:cargo.groups});
 cargo.archived_groups=[...(cargo.archived_groups||[]),{...parent,split_at:t,split_by:u.name,reason:text(b.reason),child_ids:children.map(g=>g.id)}];cargo.version++;cargo.by=u.name;cargo.actor_id=u.id;cargo.at=t;cargo.change_reason=text(b.reason);
 const fallback=(await cargoTask(env,row,u)).task,extra=[];const affected=processTasks.length?processTasks.filter(task=>task.data.status!=='completed'||task.id===parent.processes.at(-1).task_id):fallback?[fallback]:[];for(const task of affected){task.data.cargo_pending_change_at=t;if(task.data.cargo_group_ids)task.data.cargo_group_ids=task.data.cargo_group_ids.flatMap(id=>id===parent.id?children.map(g=>g.id):[id]);extra.push(...chainEvent(env,task,task.data,u,request+'-'+task.id,b.action,t));}
 const result={ok:true,id:row.id,revision:row.revision+1,fingerprint,group_ids:children.map(g=>g.id)};
 try{await env.DB.batch([...chainEvent(env,row,data,u,request,b.action,t,result),...extra]);}catch(error){const old=await previous();if(old)return old;throw error;}return result;
}
