// Reports select intersecting segments and count only their portion in the Korean date range.
export function dashboardRange(start='',end=''){
 const parse=value=>{if(!value)return null;if(!/^\d{4}-\d{2}-\d{2}$/.test(value)||new Date(value+'T00:00:00Z').toISOString().slice(0,10)!==value)throw Error('日期无效 / 날짜를 확인하세요');return Date.parse(value+'T00:00:00+09:00');};
 const a=parse(start),z=parse(end);if(a!=null&&z!=null&&a>z)throw Error('开始日期不能晚于结束日期 / 날짜 범위를 확인하세요');
 return {start:a??-Infinity,end:z==null?Infinity:z+86400000};
}
export function segmentMinutes(row,range,current=Date.now()){
 const a=Date.parse(row.joined_at),z=row.left_at?Date.parse(row.left_at):current;
 const invalid=!Number.isFinite(a)||!Number.isFinite(z)||z<a;
 const raw=invalid?0:Math.max(0,(Math.min(z,current)-a)/60000);
 return {minutes:invalid?0:Math.round(Math.max(0,Math.min(z,current,range.end)-Math.max(a,range.start))/6000)/10,raw_minutes:Math.round(raw*10)/10,invalid};
}
export const reportWorkerId=row=>String(row.worker_id||'').trim()||'UNIDENTIFIED:'+row.id;
