// Validate the bytes, not the browser's MIME claim. Downloads still use nosniff and
// attachment disposition for documents; this is format validation, not an AV scanner.
export const issueFileTypes={pdf:'application/pdf',xlsx:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',xls:'application/vnd.ms-excel',csv:'text/csv',jpg:'image/jpeg',jpeg:'image/jpeg',png:'image/png',webp:'image/webp'};
export function validateIssueFile(bytes,ext,mime=''){
 const b=new Uint8Array(bytes),view=new DataView(bytes),text=(start,end)=>new TextDecoder().decode(b.subarray(start,end));
 const starts=values=>values.every((n,i)=>b[i]===n);
 const allowed=[issueFileTypes[ext],'','application/octet-stream'];
 if(ext==='csv')allowed.push('text/plain','application/vnd.ms-excel');
 if(!allowed.includes(mime.toLowerCase()))throw Error('文件类型与扩展名不一致 / 파일 형식과 확장자가 일치하지 않습니다');
 let valid=false;
 if(ext==='png')valid=b.length>=45&&starts([137,80,78,71,13,10,26,10])&&view.getUint32(8)===13&&text(12,16)==='IHDR'&&view.getUint32(16)>0&&view.getUint32(20)>0&&text(b.length-8,b.length-4)==='IEND';
 if(ext==='jpg'||ext==='jpeg')valid=b.length>4&&starts([255,216,255])&&b[b.length-2]===255&&b[b.length-1]===217;
 if(ext==='webp')valid=b.length>=20&&text(0,4)==='RIFF'&&view.getUint32(4,true)+8===b.length&&text(8,12)==='WEBP'&&['VP8 ','VP8L','VP8X'].includes(text(12,16));
 if(ext==='pdf')valid=/^%PDF-\d\.\d/.test(text(0,8))&&/%%EOF\s*$/.test(text(Math.max(0,b.length-1024),b.length));
 if(ext==='xls')valid=b.length>=512&&starts([208,207,17,224,161,177,26,225])&&view.getUint16(28,true)===0xfffe&&[9,12].includes(view.getUint16(30,true))&&/W\x00o\x00r\x00k\x00b\x00o\x00o\x00k\x00|B\x00o\x00o\x00k\x00/.test(text(0,b.length));
 if(ext==='xlsx')valid=validWorkbookZip(b,view);
 if(ext==='csv'){
  let decoded;
  if(starts([255,254]))decoded=new TextDecoder('utf-16le').decode(b.subarray(2));
  else if(starts([254,255]))decoded=new TextDecoder('utf-16be').decode(b.subarray(2));
  else decoded=new TextDecoder().decode(b);
  valid=!!decoded.trim()&&!/[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(decoded)&&!/^\s*<(?:!doctype|html|script|svg|iframe|\?xml)\b/i.test(decoded)&&!starts([80,75,3,4]);
 }
 if(!valid)throw Error('文件内容与格式不符或文件损坏，请重新选择 / 파일 내용·형식을 확인하고 다시 선택하세요');
}
function validWorkbookZip(b,v){
 // Inspect the bounded central directory without expanding user-controlled ZIP data.
 if(b.length<22||v.getUint32(0,true)!==0x04034b50)return false;
 let end=-1;
 for(let i=b.length-22;i>=Math.max(0,b.length-65557);i--)if(v.getUint32(i,true)===0x06054b50&&i+22+v.getUint16(i+20,true)===b.length){end=i;break;}
 if(end<0||v.getUint16(end+4,true)||v.getUint16(end+6,true))return false;
 const count=v.getUint16(end+10,true),size=v.getUint32(end+12,true),offset=v.getUint32(end+16,true);
 if(!count||count>4096||count!==v.getUint16(end+8,true)||offset+size!==end)return false;
 const names=new Set();let pos=offset,total=0;
 for(let i=0;i<count;i++){
  if(pos+46>end||v.getUint32(pos,true)!==0x02014b50||v.getUint16(pos+8,true)&1)return false;
  const len=v.getUint16(pos+28,true),extra=v.getUint16(pos+30,true),comment=v.getUint16(pos+32,true),local=v.getUint32(pos+42,true),compressed=v.getUint32(pos+20,true);
  const next=pos+46+len+extra+comment;
  if(next>end||local+30>offset||v.getUint32(local,true)!==0x04034b50||![0,8].includes(v.getUint16(pos+10,true)))return false;
  const name=new TextDecoder().decode(b.subarray(pos+46,pos+46+len)),localLength=v.getUint16(local+26,true),localExtra=v.getUint16(local+28,true);
  if(names.has(name)||name.startsWith('/')||name.split('/').includes('..')||local+30+localLength+localExtra+compressed>offset||new TextDecoder().decode(b.subarray(local+30,local+30+localLength))!==name)return false;
  names.add(name);total+=v.getUint32(pos+24,true);if(total>100*1024*1024)return false;pos=next;
 }
 return pos===end&&names.has('[Content_Types].xml')&&names.has('xl/workbook.xml')&&[...names].some(n=>/^xl\/worksheets\/sheet[^/]+\.xml$/.test(n))&&![...names].some(n=>/vbaProject\.bin$/i.test(n));
}
