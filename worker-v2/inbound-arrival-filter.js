// expected_arrival is a Korean business date, independently of creation/unloading.
const value="trim(COALESCE(expected_arrival,''))";
const arrivalDay=`CASE
 WHEN ${value}='' THEN NULL
 WHEN ${value} GLOB '????/??/??' THEN date(replace(${value},'/','-'))
 WHEN ${value} NOT GLOB '*[Zz+]*' AND instr(substr(${value},12),'-')=0 THEN date(${value})
 ELSE date(${value},'+9 hours') END`;
// Match the filter/display business day; SQLite DESC puts NULL (undated) last.
// Keep the previous date/creation precedence for ties, then make paging stable by ID.
export const inboundArrivalOrder=`(${arrivalDay}) DESC, plan_date DESC, created_at DESC, id DESC`;
function calendarDay(raw){
 const s=String(raw??'').trim();if(!s)return '';
 if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||!Number.isFinite(Date.parse(s+'T00:00:00Z'))||new Date(s+'T00:00:00Z').toISOString().slice(0,10)!==s)throw Error('预计到达日期无效，请使用有效日历日期 / 입고 예정일을 확인하세요');
 return s;
}
export function inboundArrivalFilter(body){
 const from=calendarDay(body.expected_arrival_from),to=calendarDay(body.expected_arrival_to);
 if(from&&to&&from>to)throw Error('预计到达起始日期不能晚于结束日期 / 예정일 시작은 종료일보다 늦을 수 없습니다');
 let sql='';const binds=[];
 if(from){sql+=` AND (${arrivalDay}) >= ?`;binds.push(from);}
 if(to){sql+=` AND (${arrivalDay}) <= ?`;binds.push(to);}
 return {sql,binds};
}
