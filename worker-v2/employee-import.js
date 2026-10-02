// A preview is read-only; all imported profiles and audit events commit together.
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
export async function employeeImport(b,env,h,departments){
 const {access,fail,nameOf,commit,u,t}=h;access(env,['manager']);
 if(!Array.isArray(b.rows)||!b.rows.length||b.rows.length>200)fail('每次上传 1–200 名职员 / 한 번에 1–200명');
 const existing=(await q(env,'SELECT p.*,e.employee_no,e.department,e.version FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id').all()).results;
 const byNo=new Map(existing.map(p=>[p.employee_no,p]));
 const active=new Set((await q(env,"SELECT worker_id FROM ck_attendance_days WHERE signed_out='' UNION SELECT worker_id FROM v2_ops_job_workers WHERE left_at='' AND worker_id LIKE 'EMP-%'").all()).results.map(x=>x.worker_id));
 const seen=new Map(),plans=b.rows.map((raw,i)=>{
  const errors=[],row=Number.isInteger(raw?.row)&&raw.row>=2?raw.row:i+2;
  let name='';try{name=nameOf(raw?.name);}catch(e){errors.push(e.message);}
  const employeeNo=String(raw?.employeeNo??'').trim().toUpperCase(),department=String(raw?.department||'');
  if(!/^[A-Z0-9][A-Z0-9_-]{0,31}$/.test(employeeNo))errors.push('工号必填，限1–32位字母、数字、短横线或下划线 / 사번 형식 오류');
  if(!Object.hasOwn(departments,department))errors.push('部门不正确 / 부서 오류');
  const before=byNo.get(employeeNo),blank=raw?.enabled==null||raw.enabled==='';
  if(!blank&&typeof raw.enabled!=='boolean')errors.push('状态只能填写在职或停用 / 재직 또는 비활성');
  const enabled=blank?(before?!!before.enabled:true):raw.enabled;
  if(before&&!enabled&&active.has(before.badge_id))errors.push('尚未签退或仍在任务中，不能停用 / 출퇴근·작업 종료 필요');
  if(!seen.has(employeeNo))seen.set(employeeNo,[]);seen.get(employeeNo).push(i);
  const action=!before?'create':before.name===name&&before.department===department&&!!before.enabled===enabled?'unchanged':'update';
  return {row,name,employeeNo,department,enabled,action,errors,before:before||null};
 });
 for(const indices of seen.values())if(indices.length>1)for(const i of indices)plans[i].errors.push('文件内工号重复 / 파일 내 사번 중복');
 const items=plans.map(p=>({...p,before:p.before?{name:p.before.name,department:p.before.department,enabled:!!p.before.enabled}:null}));
 const counts={create:0,update:0,unchanged:0,error:0};for(const p of plans)counts[p.errors.length?'error':p.action]++;
 const hash=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify(plans)));
 const previewToken=Array.from(new Uint8Array(hash),x=>x.toString(16).padStart(2,'0')).join('');
 if(b.action==='sop_attendance_employee_import_preview')return {ok:true,valid:!counts.error,items,counts,previewToken};
 if(b.action!=='sop_attendance_employee_import')fail('未知批量登记操作');
 if(counts.error)fail('表格有错误，请重新核对，尚未导入任何资料 / 오류를 수정하세요');
 if(b.previewToken!==previewToken)fail('资料已变化，请重新预览后导入 / 미리보기를 새로 확인하세요');
 const statements=[],changes=[];
 for(const [i,p] of plans.entries()){
  if(p.action==='unchanged')continue;
  const before=p.before,after=before?{...before,name:p.name,agency:p.department,department:p.department,enabled:p.enabled?1:0,version:before.version+1}:{id:'ATP-'+crypto.randomUUID(),badge_id:'EMP-'+p.employeeNo,name:p.name,agency:p.department,department:p.department,employee_no:p.employeeNo,kind:'permanent',enabled:p.enabled?1:0,created_at:t,version:1};
  // Unique person/version events protect against concurrent ordinary profile edits.
  statements.push(q(env,'INSERT INTO ck_attendance_events VALUES(?,?,?,?,?,?,?,?,?,?,?)','ATE-'+crypto.randomUUID(),b.client_req_id+'-row-'+i,after.id,after.version,b.action,u.id,t,JSON.stringify(before||{}),JSON.stringify(after),JSON.stringify({ok:true}),JSON.stringify({batch:b.client_req_id,row:p.row})));
  if(before){
   // Fail the transaction if the person clocks in after validation but before saving.
   statements.push(q(env,"UPDATE ck_employee_profiles SET department=?,version=CASE WHEN version=? AND (?=1 OR (NOT EXISTS(SELECT 1 FROM ck_attendance_days WHERE person_id=? AND signed_out='') AND NOT EXISTS(SELECT 1 FROM v2_ops_job_workers WHERE worker_id=? AND left_at=''))) THEN ? ELSE NULL END WHERE person_id=?",after.department,before.version,after.enabled,before.id,before.badge_id,after.version,before.id));
   statements.push(q(env,'UPDATE ck_attendance_people SET name=?,agency=?,enabled=? WHERE id=?',after.name,after.agency,after.enabled,after.id));
  }else statements.push(q(env,'INSERT INTO ck_attendance_people VALUES(?,?,?,?,?,?,?)',after.id,after.badge_id,after.name,after.agency,'permanent',after.enabled,t),q(env,'INSERT INTO ck_employee_profiles VALUES(?,?,?,1)',after.id,p.employeeNo,after.department));
  changes.push({row:p.row,employeeNo:p.employeeNo,id:after.id,action:p.action});
 }
 return commit(env,b,u,null,{id:'EMPIMPORT-'+b.client_req_id,version:1,changes,counts},statements,{ok:true,counts});
}
