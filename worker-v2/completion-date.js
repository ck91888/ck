export function completionDay(value){
 const raw=String(value??'').trim();if(!raw)return '';
 const day=raw.slice(0,10);if(!/^\d{4}-\d{2}-\d{2}$/.test(day)||!Number.isFinite(Date.parse(day))||new Date(day).toISOString().slice(0,10)!==day)throw Error('请填写有效的要求完成日期');
 if(raw.length===10)return day;
 if(!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?(?:Z|[+-]\d{2}:\d{2})?$/.test(raw)||!Number.isFinite(Date.parse(raw)))throw Error('请填写有效的要求完成日期');
 return /(?:Z|[+-]\d{2}:\d{2})$/.test(raw)?new Date(Date.parse(raw)+9*3600000).toISOString().slice(0,10):day;
}
export function completionDate(value,previous=''){
 if(previous&&String(value??'').trim()===String(previous))return String(previous);
 const day=completionDay(value);
 // Saving other instructions must not rewrite an old deadline's raw timestamp.
 if(previous&&day===completionDay(previous))return String(previous);
 return day;
}
