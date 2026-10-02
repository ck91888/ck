// Shared by document inputs and the API: a staff badge is never an order number.
export function isStaffBadge(value){
 const code=String(value??'').normalize('NFKC').trim();
 return /^(?:EMP-|DAF-|DA-\d{6,8}-)/i.test(code)||/^[^|]+\|[^|]+$/.test(code);
}
export function documentCodeError(value){
 const code=String(value??'').normalize('NFKC').trim();
 if(isStaffBadge(code))return '这是人员工牌，请在“分配人员”中扫描；未添加为单号 / 명찰은 인원 배정에서 스캔하세요';
 if(!code||code.length>200||/[\u0000-\u001f]/.test(code))return '单号格式无效，请重新扫描 / 번호를 다시 스캔하세요';
 return '';
}
