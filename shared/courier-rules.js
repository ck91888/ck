// Source: 仓库入库记录.xlsx, Sheet1 C1:J1 and populated tracking cells.
// Ownership is a destination, never a carrier inferred from a number prefix.
export const courierOwners = [
 ['8-1','8-1'],['8-2','8-2'],['8-3','8-3'],['8-4','8-4'],
 ['tent','천막동（篷布仓）'],['supplies','仓库耗材 / 창고 소모품'],
 ['purchase','采购物品 / 구매 물품'],['unknown','无法识别件 / 미확인']
];
export function trackingNumber(value) {
 const raw=String(value??'').normalize('NFKC').trim().toUpperCase();
 if(raw.length>80||/[\r\n\u0000-\u001f]/.test(raw))throw Error('请扫描一张快递单号 / 송장번호 하나를 스캔하세요');
 // Spaces and hyphens are display separators only. Never extract substrings from QR URLs or notes.
 const code=raw.replace(/[ -]/g,'');
 if(!/^(?:\d{11,14}|(?:LP|EZ)\d{9}CN|JJD\d{18})$/.test(code))throw Error('未识别为快递单号，请对准面单上的运单条码 / 송장 바코드를 다시 스캔하세요');
 if(/^(\d)\1+$/.test(code))throw Error('快递单号无效 / 송장번호 오류');
 return code;
}
export function trackingNumbers(value) {
 const input=Array.isArray(value)?value:String(value??'').split(/[\r\n,;，；]+/);
 const codes=[...new Set(input.map(x=>String(x).trim()).filter(Boolean).map(trackingNumber))];
 if(codes.length>200)throw Error('每张计划最多登记200个快递单号 / 최대 200개');
 return codes;
}
