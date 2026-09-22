export const employeeDepartments={bulk:'大货 / 대량',direct_ship:'代发 / 직배송',import:'进口 / 수입',order_processing:'订单处理 / 주문 처리',customer_service:'客服 / 고객 서비스',office:'办公室 / 사무실',management:'管理 / 관리',other:'其他 / 기타'};
export const EMPLOYEE_SCHEMA=[
 `CREATE TABLE IF NOT EXISTS ck_employee_profiles(person_id TEXT PRIMARY KEY,employee_no TEXT NOT NULL UNIQUE,department TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 1)`,
 `CREATE TRIGGER IF NOT EXISTS ck_employee_guard_join BEFORE INSERT ON v2_ops_job_workers
 WHEN NEW.worker_id GLOB 'EMP-*' AND NEW.left_at=''
 BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_days d WHERE d.worker_id=NEW.worker_id AND d.day=date(NEW.joined_at,'+9 hours') AND d.signed_out='' AND d.signed_in<=NEW.joined_at AND NOT EXISTS(SELECT 1 FROM ck_attendance_breaks b WHERE b.attendance_id=d.id AND b.ended_at='')) THEN RAISE(ABORT,'Employee attendance required') END; END`
];
export const isEmployee=badge=>String(badge||'').startsWith('EMP-');
const q=(env,sql,...a)=>env.DB.prepare(sql).bind(...a),rows=async(env,sql,...a)=>(await q(env,sql,...a).all()).results;
const select='SELECT p.*,e.employee_no,e.department,e.version FROM ck_attendance_people p JOIN ck_employee_profiles e ON e.person_id=p.id';
const person=p=>({id:p.id,badgeId:p.badge_id,name:p.name,employeeNo:p.employee_no,department:p.department,agency:p.department,badgeType:'permanent',personType:'employee',enabled:!!p.enabled,version:p.version});
import { employeeImport } from './employee-import.js';
export async function employeeAction(b,env,h){
 if(b.action==='sop_attendance_employee_import_preview'||b.action==='sop_attendance_employee_import')return employeeImport(b,env,h,employeeDepartments);
 const {access,fail,nameOf,text,commit,u,t}=h;
 if(b.action==='sop_attendance_employee_people'){access(env,['manager']);return {ok:true,items:(await rows(env,select+' ORDER BY p.enabled DESC,p.name,e.employee_no')).map(person)};}
 if(b.action==='sop_attendance_employee_search'){
  const term=text(b.term,80);if(!term)fail('请输入姓名、工号或扫描工牌 / 이름·사번을 입력하세요');
  const code=term.split('|')[0].toUpperCase();
  const found=await rows(env,select+' WHERE p.enabled=1 AND (p.name=? OR p.badge_id=? OR e.employee_no=?) ORDER BY e.employee_no LIMIT 51',term,code,code);
  if(found.length>50)fail('同名人员过多，请使用工号 / 사번으로 조회하세요');
  return {ok:true,items:found.map(person)};
 }
 access(env,['manager']);
 const department=String(b.department||'');if(!Object.hasOwn(employeeDepartments,department))fail('请选择所属部门 / 부서를 선택하세요');
 const name=nameOf(b.name);
 if(b.action==='sop_attendance_employee_register'){
  const no=(text(b.employeeNo,34)||'E'+crypto.randomUUID().slice(0,8)).toUpperCase();
  if(!/^[A-Z0-9][A-Z0-9_-]{0,31}$/.test(no))fail('工号限1–32位字母、数字、短横线或下划线 / 사번 형식을 확인하세요');
  if(await q(env,'SELECT person_id FROM ck_employee_profiles WHERE employee_no=?',no).first())fail('工号已登记，请勿重复创建 / 이미 등록된 사번입니다');
  const p={id:'ATP-'+crypto.randomUUID(),badge_id:'EMP-'+no,name,agency:department,department,employee_no:no,kind:'permanent',enabled:1,created_at:t,version:1};
  return commit(env,b,u,null,p,[q(env,'INSERT INTO ck_attendance_people VALUES(?,?,?,?,?,1,?)',p.id,p.badge_id,name,department,'permanent',t),q(env,'INSERT INTO ck_employee_profiles VALUES(?,?,?,1)',p.id,no,department)],{ok:true,person:person(p)});
 }
 if(b.action!=='sop_attendance_employee_update')fail('未知职员操作');
 const before=await q(env,select+' WHERE p.id=?',String(b.id||'')).first();if(!before)fail('职员不存在 / 직원 정보가 없습니다');
 if(Number(b.version)!==before.version)fail('资料已更新，请刷新后重试 / 새로고침 후 다시 시도하세요');
 if(typeof b.enabled!=='boolean')fail('在职状态无效');
 if(!b.enabled&&(await q(env,"SELECT id FROM ck_attendance_days WHERE person_id=? AND signed_out='' LIMIT 1",before.id).first()||await q(env,"SELECT id FROM v2_ops_job_workers WHERE worker_id=? AND left_at='' LIMIT 1",before.badge_id).first()))fail('该职员尚未签退或仍在任务中，请先核实结束 / 출퇴근·작업 상태를 먼저 확인하세요');
 const after={...before,name,agency:department,department,enabled:b.enabled?1:0,version:before.version+1};
 return commit(env,b,u,before,after,[q(env,'UPDATE ck_attendance_people SET name=?,agency=?,enabled=? WHERE id=?',name,department,after.enabled,before.id),q(env,'UPDATE ck_employee_profiles SET department=?,version=? WHERE person_id=? AND version=?',department,after.version,before.id,before.version)],{ok:true,person:person(after)});
}
