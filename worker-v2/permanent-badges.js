import {ensureSchema} from './schema-ready.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
// Keep the original people table's layout: older attendance and employee writers
// still use positional INSERTs. Existing registrations already have audit v1.
export const permanentPeopleSelect=`SELECT p.*,v.version FROM ck_attendance_people p JOIN ck_permanent_badge_profiles v ON v.person_id=p.id WHERE p.kind='permanent' AND p.badge_id GLOB 'DAF-*'`;
export const PERMANENT_BADGE_SCHEMA=[
  'CREATE TABLE IF NOT EXISTS ck_permanent_badge_profiles(person_id TEXT PRIMARY KEY,version INTEGER NOT NULL DEFAULT 1)',
  `INSERT OR IGNORE INTO ck_permanent_badge_profiles(person_id,version) SELECT p.id,MAX(1,COALESCE((SELECT MAX(e.version) FROM ck_attendance_events e WHERE e.record_id=p.id),1)) FROM ck_attendance_people p WHERE p.kind='permanent' AND p.badge_id GLOB 'DAF-*'`,
  `CREATE TRIGGER IF NOT EXISTS ck_permanent_enabled_checkin BEFORE INSERT ON ck_attendance_days WHEN NEW.worker_id GLOB 'DAF-*' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_people p WHERE p.id=NEW.person_id AND p.badge_id=NEW.worker_id AND p.kind='permanent' AND p.enabled=1) THEN RAISE(ABORT,'Permanent badge disabled or unregistered') END; END`,
  `CREATE TRIGGER IF NOT EXISTS ck_permanent_enabled_join BEFORE INSERT ON v2_ops_job_workers WHEN NEW.worker_id GLOB 'DAF-*' AND NEW.left_at='' BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_people p WHERE p.badge_id=NEW.worker_id AND p.kind='permanent' AND p.enabled=1) THEN RAISE(ABORT,'Permanent badge disabled or unregistered') END; END`,
  // Closing an existing segment remains possible after disable. Reopening or
  // moving a segment to another job is a new assignment and must revalidate.
  `CREATE TRIGGER IF NOT EXISTS ck_permanent_enabled_rejoin BEFORE UPDATE ON v2_ops_job_workers WHEN NEW.worker_id GLOB 'DAF-*' AND NEW.left_at='' AND (OLD.left_at!='' OR OLD.worker_id!=NEW.worker_id OR OLD.job_id!=NEW.job_id) BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM ck_attendance_people p WHERE p.badge_id=NEW.worker_id AND p.kind='permanent' AND p.enabled=1) THEN RAISE(ABORT,'Permanent badge disabled or unregistered') END; END`
];
export async function ensurePermanentBadges(env){
 await ensureSchema(env.DB,'permanent-badges-v1',()=>env.DB.batch(PERMANENT_BADGE_SCHEMA.map(sql=>q(env,sql))));
}
export async function updatePermanentBadge(b,env,{access,fail,nameOf,agencyOf,commit,u,publicPerson}){
 access(env,['manager']);
 const before=await q(env,permanentPeopleSelect+' AND p.id=?',String(b.id||'')).first();
 if(!before)fail('长期日当工牌不存在 / 고정 일용직 명찰을 찾을 수 없습니다');
 if(!Number.isSafeInteger(b.version)||b.version!==before.version)fail('资料已更新，请重新打开工牌管理 / 정보가 변경되었습니다. 다시 열어 주세요');
 const reason=typeof b.reason==='string'?b.reason.normalize('NFC').trim():'';
 if(!reason||reason.length>500||/[\p{Cc}\p{Cf}]/u.test(reason))fail('请填写1–500字核实原因 / 확인 사유를 1–500자로 입력하세요');
 if(typeof b.enabled!=='boolean')fail('工牌状态无效 / 명찰 상태를 확인하세요');
 const after={...before,name:nameOf(b.name),agency:agencyOf(b.agency),enabled:b.enabled?1:0,version:before.version+1,change_reason:reason};
 return commit(env,b,u,before,after,[
  q(env,'UPDATE ck_permanent_badge_profiles SET version=CASE WHEN version=? THEN ? ELSE NULL END WHERE person_id=?',before.version,after.version,before.id),
  q(env,'UPDATE ck_attendance_people SET name=?,agency=?,enabled=? WHERE id=?',after.name,after.agency,after.enabled,before.id)
 ],{ok:true,person:publicPerson(after)});
}
