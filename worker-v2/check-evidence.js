// Complete scan evidence aggregates; pagination limits only the raw log viewer.
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
export async function checkScanLogs(env,batchId,{offset=0,limit=100,barcode='',pallet=''}={}){
 offset=Number(offset);limit=Number(limit);if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>200)throw Error('扫描记录分页参数无效');
 const clauses=['batch_id=?'],args=[batchId];if(barcode){clauses.push('barcode=?');args.push(String(barcode));}if(pallet){clauses.push('pallet_no=?');args.push(String(pallet));}
 const where=clauses.join(' AND '),count=await q(env,'SELECT COUNT(*) AS total FROM v2_verify_scan_logs WHERE '+where,...args).first();
 const logs=(await q(env,'SELECT * FROM v2_verify_scan_logs WHERE '+where+' ORDER BY scanned_at DESC,id DESC LIMIT ? OFFSET ?',...args,limit,offset).all()).results||[];
 return {total:Number(count?.total||0),logs,offset,page_size:limit,more:offset+logs.length<Number(count?.total||0)};
}
export async function checkScanEvidence(env,batchId){
 const grouped=(await q(env,`SELECT barcode,pallet_no,COUNT(*) AS total,SUM(scan_result='ok') AS ok,SUM(scan_result='overflow') AS overflow,SUM(scan_result='not_found') AS not_found,SUM(scan_result='duplicate') AS duplicate,MAX(scanned_at) AS last_at FROM v2_verify_scan_logs WHERE batch_id=? GROUP BY barcode,pallet_no`,batchId).all()).results||[];
 const barcodes=new Map(),pallets=new Map();let total=0;
 for(const row of grouped){
  const values={total:Number(row.total||0),ok:Number(row.ok||0),overflow:Number(row.overflow||0),not_found:Number(row.not_found||0),duplicate:Number(row.duplicate||0)},key=String(row.pallet_no||'');total+=values.total;
  let b=barcodes.get(row.barcode);if(!b){b={barcode:row.barcode,total:0,ok:0,overflow:0,not_found:0,duplicate:0,last_at:'',pallets:[]};barcodes.set(row.barcode,b);}for(const k of Object.keys(values))b[k]+=values[k];if(row.last_at>b.last_at)b.last_at=row.last_at;b.pallets.push({pallet_no:key,...values,last_at:row.last_at||''});
  let p=pallets.get(key);if(!p){p={pallet_no:key,total:0,ok:0,abnormal:0,last_at:''};pallets.set(key,p);}p.total+=values.total;p.ok+=values.ok;p.abnormal+=values.total-values.ok;if(row.last_at>p.last_at)p.last_at=row.last_at;
 }
 for(const b of barcodes.values())b.pallets.sort((a,c)=>a.pallet_no.localeCompare(c.pallet_no));
 return {...await checkScanLogs(env,batchId),total,complete:true,barcodes:[...barcodes.values()].sort((a,b)=>String(a.barcode).localeCompare(String(b.barcode))),pallets:[...pallets.values()].sort((a,b)=>b.abnormal-a.abnormal||b.total-a.total||a.pallet_no.localeCompare(b.pallet_no))};
}
