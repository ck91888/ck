// Mapping stays on the need; each real process task references groups without copying their ranges/files.
export function processNames(value){const names=value===undefined?['作业']:value;if(!Array.isArray(names)||!names.length||names.length>10)throw Error('每组请填写1至10个实际工序');const result=names.map(x=>String(x??'').trim());if(result.some(n=>!n||n.length>80))throw Error('工序名称须为1至80字');return result;}
export function taskGroups(cargo,task){return task?.data.cargo_group_ids?cargo.groups.filter(g=>task.data.cargo_group_ids.includes(g.id)):cargo.groups;}
export function taskProcess(group,task){if(!group.processes)return null;return group.processes.find((p,i)=>i===(task.data.cargo_process_index||0)&&p.task_id===task.id)||null;}
export function allProcessesApproved(group){return !group.processes||group.processes.every(p=>p.status==='approved');}
export function assignCargoProcess(need,task,body,user,time){
 const cargo=need.cargo_groups;if(!cargo)return;
 const initial=!(need.task_ids?.length||need.task_id),ids=body.cargo_group_ids||(initial?cargo.groups.map(g=>g.id):null),index=Number(body.cargo_process_index||0);
 if(!Array.isArray(ids)||!ids.length||new Set(ids).size!==ids.length||!Number.isSafeInteger(index)||index<0)throw Error('请选择本次真实工序及对应资料组');
 let name='';
 for(const id of ids){const g=cargo.groups.find(g=>g.id===id);if(!g)throw Error('资料组不属于此计划');if(!g.processes)g.processes=processNames(g.process_names).map(name=>({name,status:'pending',task_id:''}));
  const process=g.processes[index];if(!process||process.task_id||process.status!=='pending')throw Error('所选工序已派工或不存在');if(g.processes.slice(0,index).some(p=>p.status!=='approved'))throw Error('前一道工序尚未审核，不能开始后续工序');
  if(name&&name!==process.name)throw Error('一次真实任务只能选择相同工序，请分别派工');name=process.name;
  process.task_id=task.id;process.status='assigned';g.mapping_version=g.mapping_version||1;g.ack_version=g.mapping_version;g.ack={by:user.name,actor_id:user.id,at:time};g.public_documents=g.public_documents||cargo.public_documents;
 }
 task.data.cargo_group_ids=ids;task.data.cargo_process_index=index;task.data.cargo_process_name=name;
 need.task_ids=[...(need.task_ids||[]),...(!need.task_ids?.length&&need.task_id?[need.task_id]:[]),task.id];need.task_id=task.id;
}
