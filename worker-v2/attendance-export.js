import {employeeWorkdayAccess} from './employee-workday.js';
import {exportRange,exportDate,nextExportDate,financePersonDay,LABOR_EXPORT_LIMITS} from '../shared/labor-export-data.js';
import {kstDay} from './attendance-time.js';
const error=message=>{throw Error(message);};
export async function attendanceExportPage(b,e,now,overview){
 employeeWorkdayAccess(e); // Historical daily labor has the same office-only read boundary.
 const {start,end,days}=exportRange(b.start_date,b.end_date);if(end>kstDay(now))error('结束日期不能晚于今天（韩国时间）');
 const agency=String(b.agency||''),keyword=String(b.keyword||'').normalize('NFC').trim();if(agency.length>80||keyword.length>80)error('筛选内容过长');
 const date=exportDate(b.cursor?.date||start),offset=b.cursor?.offset??0,limit=b.page_size??LABOR_EXPORT_LIMITS.page;
 if(date<start||date>end||!Number.isSafeInteger(offset)||offset<0||!Number.isInteger(limit)||limit<1||limit>LABOR_EXPORT_LIMITS.page)error('导出分页无效，请重新导出');
 const asOf=b.as_of||now;if(!Number.isFinite(Date.parse(asOf))||Date.parse(asOf)>Date.parse(now)||Date.parse(now)-Date.parse(asOf)>3600000)error('导出已过期，请重新导出');
 const report=await overview(e,date,asOf,'daily'),rows=report.items.filter(x=>!x.record.voided&&x.record.personType!=='employee'&&!/^EMP-/i.test(x.record.badgeId||'')).filter(x=>(!agency||x.record.agency===agency)&&(!keyword||(x.record.name+' '+x.record.badgeId).toLocaleLowerCase().includes(keyword.toLocaleLowerCase()))).map(x=>financePersonDay(x,date,asOf)).sort((a,b)=>a.name.localeCompare(b.name,'zh-Hans')||a.badge.localeCompare(b.badge)||a.personKey.localeCompare(b.personKey));
 if(rows.length>LABOR_EXPORT_LIMITS.rows)error('单日超过20000个人日，请按公司或姓名工牌分段导出');
 const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify({start,end,agency,keyword,asOf,rows}))))),snapshot=digest.map(v=>v.toString(16).padStart(2,'0')).join('');
 if(offset>rows.length||offset>0&&(!b.snapshot||b.snapshot!==snapshot))error('当天记录已变化或分页校验失效，请重新导出，未生成部分文件');
 const items=rows.slice(offset,offset+limit),next=offset+items.length<rows.length?{date,offset:offset+items.length}:date<end?{date:nextExportDate(date),offset:0}:null;
 return {ok:true,start,end,days,date,asOf,filters:{agency,keyword},items,total:rows.length,snapshot,next,limits:LABOR_EXPORT_LIMITS};
}
