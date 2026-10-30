// Collaboration plans use bulk execution. Stored departments remain source/business
// evidence; native direct-ship/import dispatches retain their original permissions.
export function executionDepartment(row){
 const data=row.data|| (row.state?JSON.parse(row.state):row);
 return row.kind==='need'||row.kind==='task'&&(!!data.need_id||data.job_type==='bulk_op')?'bulk':row.department;
}
export function executionDepartmentSQL(alias=''){
 const p=alias?alias+'.':'';
 return `(CASE WHEN ${p}kind='need' OR (${p}kind='task' AND (COALESCE(json_extract(${p}state,'$.need_id'),'')<>'' OR json_extract(${p}state,'$.job_type')='bulk_op')) THEN 'bulk' ELSE ${p}department END)`;
}
