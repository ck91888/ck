// Show the exact submitted identity without silently normalizing or substituting it.
// Generated badge IDs are ASCII; make hidden/non-ASCII input differences visible.
export function attendanceIdentityLabel(value) {
 const id=String(value??'');
 return id.slice(0,120).replace(/[^\x21-\x7e]/gu,c=>Array.from(c).map(x=>'\\u{'+x.codePointAt(0).toString(16).toUpperCase()+'}').join(''))+(id.length>120?'…':'');
}
export function missingAttendanceMessage(worker,day) {
 return (worker.name||worker.id)+' 未找到当天有效签到 / 유효한 당일 출근 기록 없음'+
  '\n工牌 / 명찰: '+attendanceIdentityLabel(worker.id)+
  '\n核验日期 / 확인 날짜 (KST): '+day+
  '\n请核对工牌编号与签到记录 / 명찰 번호와 출근 기록을 확인하세요';
}
