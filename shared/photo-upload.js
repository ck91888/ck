// Dedicated photo inputs only. Documents and historical objects never enter here.
(function(){
'use strict';
const hint='照片上传前自动压缩，不另存拍摄原图。请确认细字、条码清晰。 / 사진은 압축하여 업로드하며 촬영 원본은 별도 보관하지 않습니다. 작은 글자·바코드를 확인하세요.';
const cache=new WeakMap();let tail=Promise.resolve(),active=null;
const fail=message=>{throw Error(message);};
const cancelled=signal=>{if(signal?.aborted)throw new DOMException('照片处理已取消 / 사진 처리 취소','AbortError');};
const ascii=(b,p,n)=>String.fromCharCode(...b.subarray(p,p+n));
async function inspect(file){
 if(!file?.size||file.size>20*1048576)fail('请选择20MB以内的照片 / 20MB 이하 사진을 선택하세요');
 const b=new Uint8Array(await file.arrayBuffer()),v=new DataView(b.buffer);
 let width=0,height=0,type='',alpha=false,orientation=1;
 if(b[0]===255&&b[1]===216){
  type='image/jpeg';let p=2;
  while(p+4<=b.length){
   if(b[p++]!==255)break;while(b[p]===255)p++;const marker=b[p++];
   if(marker===218||marker===217)break;if(marker===1||marker>=208&&marker<=215)continue;
   if(p+2>b.length)break;const len=v.getUint16(p);if(len<2||p+len>b.length)break;
   if(marker===225&&ascii(b,p+2,6)==='Exif\0\0'){
    const t=p+8,end=p+len,le=ascii(b,t,2)==='II';
    if(t+8<=end&&['II','MM'].includes(ascii(b,t,2))&&v.getUint16(t+2,le)===42){
     const d=t+v.getUint32(t+4,le);
     if(d>=t&&d+2<=end){const n=Math.min(v.getUint16(d,le),512);for(let i=0;i<n;i++){const a=d+2+i*12;if(a+12>end)break;if(v.getUint16(a,le)===274&&v.getUint16(a+2,le)===3&&v.getUint32(a+4,le)===1)orientation=v.getUint16(a+8,le);}}
    }
   }
   if([192,193,194,195,197,198,199,201,202,203,205,206,207].includes(marker)&&len>=8){height=v.getUint16(p+3);width=v.getUint16(p+5);}
   p+=len;
  }
 }else if(b.length>=33&&[137,80,78,71,13,10,26,10].every((x,i)=>b[i]===x)){
  type='image/png';width=v.getUint32(16);height=v.getUint32(20);alpha=[4,6].includes(b[25]);
  for(let p=8;p+12<=b.length;){const n=v.getUint32(p),tag=ascii(b,p+4,4);if(p+12+n>b.length)fail('照片文件损坏 / 손상된 사진');if(tag==='acTL')fail('动图请改选静态照片 / 움직이는 사진 대신 정지 사진을 선택하세요');if(tag==='tRNS')alpha=true;p+=12+n;}
 }else if(b.length>=30&&ascii(b,0,4)==='RIFF'&&ascii(b,8,4)==='WEBP'){
  type='image/webp';
  for(let p=12;p+8<=b.length;){const n=v.getUint32(p+4,true),tag=ascii(b,p,4),d=p+8;if(d+n>b.length)fail('照片文件损坏 / 손상된 사진');
   if(tag==='ANIM'||tag==='ANMF'||tag==='VP8X'&&(b[d]&2))fail('动图请改选静态照片 / 움직이는 사진 대신 정지 사진을 선택하세요');
   if(tag==='VP8X'&&n>=10){alpha=!!(b[d]&16);width=1+b[d+4]+b[d+5]*256+b[d+6]*65536;height=1+b[d+7]+b[d+8]*256+b[d+9]*65536;}
   if(tag==='ALPH')alpha=true;
   if(tag==='VP8 '&&n>=10&&!width){width=v.getUint16(d+6,true)&16383;height=v.getUint16(d+8,true)&16383;}
   if(tag==='VP8L'&&n>=5){const bits=v.getUint32(d+1,true);if(!width){width=(bits&16383)+1;height=((bits>>>14)&16383)+1;}alpha=alpha||!!(bits&0x10000000);}
   p=d+n+(n%2);
  }
 }
 if(!width||!height||!type)fail('无法处理此照片，请改选JPG、PNG或静态WebP / JPG·PNG·정지 WebP를 선택하세요');
 const low=navigator.deviceMemory&&navigator.deviceMemory<=2;
 if(width*height>(low?16000000:40000000)||Math.max(width,height)>16000)fail('照片像素过大，请在相册缩小后重新选择 / 사진 해상도를 줄여 다시 선택하세요');
 if(orientation>=5&&orientation<=8)[width,height]=[height,width];
 return {width,height,type,alpha};
}
async function compress(file,signal){
 cancelled(signal);const meta=await inspect(file);cancelled(signal);
 const scale=Math.min(1,2048/Math.max(meta.width,meta.height)),width=Math.max(1,Math.round(meta.width*scale)),height=Math.max(1,Math.round(meta.height*scale));
 let image,canvas,url='';
 try{
  if(typeof createImageBitmap==='function')image=await createImageBitmap(file,{imageOrientation:'from-image',resizeWidth:width,resizeHeight:height,resizeQuality:'high'});
  else{
   if(meta.width*meta.height>16000000)fail('此浏览器无法安全处理大照片，请缩小照片或更新浏览器 / 사진을 줄이거나 브라우저를 업데이트하세요');
   image=new Image();url=URL.createObjectURL(file);
   await new Promise((resolve,reject)=>{image.onload=resolve;image.onerror=()=>reject(Error('照片无法解码，请重新选择 / 사진을 다시 선택하세요'));image.src=url;});
  }
  cancelled(signal);canvas=document.createElement('canvas');canvas.width=width;canvas.height=height;
  const ctx=canvas.getContext('2d');if(!ctx)fail('照片处理内存不足，请减少照片后重试 / 사진 수를 줄여 다시 시도하세요');
  ctx.drawImage(image,0,0,width,height);
  const type=meta.alpha?'image/png':'image/jpeg';
  const blob=await new Promise((resolve,reject)=>{canvas.toBlob(b=>b?resolve(b):reject(Error('照片压缩失败，请重试 / 사진 압축 실패. 다시 시도하세요')),type,0.86);});
  cancelled(signal);if(blob.type!==type)fail('浏览器不支持照片编码，请更新浏览器 / 브라우저를 업데이트하세요');
  // No byte target/quality loop: preserve detail even when the result exceeds a few hundred KB.
  const chosen=blob.size<file.size?blob:file,chosenType=chosen===file?meta.type:type;
  const ext={'image/jpeg':'jpg','image/png':'png','image/webp':'webp'}[chosenType];
  const name=(file.name.replace(/\.[^.]*$/,'')||'photo')+'.'+ext;
  return new File([chosen],name,{type:chosenType,lastModified:file.lastModified});
 }catch(error){if(error.name==='AbortError'||/[\u3400-\u9fff]/.test(error.message))throw error;throw Error('照片压缩失败，请缩小照片或重新选择 / 사진 해상도를 줄이거나 다시 선택하세요');}
 finally{image?.close?.();if(url)URL.revokeObjectURL(url);if(canvas){canvas.width=0;canvas.height=0;}}
}
function prepare(file,{signal}={}){
 cancelled(signal);let result=cache.get(file);
 if(!result){result=tail.catch(()=>{}).then(()=>compress(file,signal));tail=result.catch(()=>{});cache.set(file,result);result.catch(()=>{if(cache.get(file)===result)cache.delete(file);});}
 if(!signal)return result;
 // Stop the UI promptly; keep the decoder in the serial tail until its resources
 // are released. Cancelling cannot start a second large decode concurrently.
 return new Promise((resolve,reject)=>{const abort=()=>{if(cache.get(file)===result)cache.delete(file);reject(new DOMException('照片处理已取消 / 사진 처리 취소','AbortError'));};signal.addEventListener('abort',abort,{once:true});result.then(value=>{signal.removeEventListener('abort',abort);if(signal.aborted)abort();else resolve(value);},e=>{signal.removeEventListener('abort',abort);reject(e);});});
}
// One visible recoverable batch for legacy photo upload entrances. Ownership is captured
// by the caller; a retry reuses these File objects and never recodes uploaded bytes.
function upload(files,options){
 if(active){active.host.scrollIntoView({block:'nearest'});return Promise.resolve(false);}
 document.querySelectorAll('[data-photo-complete]').forEach(host=>host.remove());
 const unique=files.filter((f,i)=>files.findIndex(a=>a.name===f.name&&a.size===f.size&&a.lastModified===f.lastModified)===i);
 const host=document.createElement('section');host.className='ck-photo-transfer card';host.setAttribute('aria-label','照片上传 / 사진 업로드');
 host.innerHTML='<p data-status role="status" aria-live="polite"></p><p>'+hint+'</p><button type="button" data-stop>暂停后续照片 / 나머지 일시정지</button> <button type="button" data-retry hidden>重试未上传照片 / 업로드 재시도</button> <button type="button" data-discard hidden>移除待上传照片 / 대기 사진 제외</button>';
 const place=()=>{const target=[...document.querySelectorAll('dialog[open]')].at(-1)||document.body;if(host.parentNode!==target)target.append(host);};place();
 // Native dialogs live in the top layer. Keep recovery controls visible even if
 // feedback is closed/reopened or its form is rerendered during an upload.
 const observer=new MutationObserver(place);observer.observe(document.body,{childList:true,subtree:true,attributes:true,attributeFilter:['open']});
 const batch={host,items:unique.map(original=>({original,file:null,request:crypto.randomUUID()})),running:false,stop:false,controller:null};active=batch;
 const status=host.querySelector('[data-status]'),stop=host.querySelector('[data-stop]'),retry=host.querySelector('[data-retry]'),discard=host.querySelector('[data-discard]');
 const clear=()=>{observer.disconnect();host.remove();if(active===batch)active=null;};
 stop.onclick=()=>{batch.stop=true;batch.controller?.abort();stop.disabled=true;};
 discard.onclick=()=>{options.onDiscard?.();clear();};
 async function run(){
  if(batch.running)return;batch.running=true;batch.stop=false;stop.disabled=false;stop.hidden=false;retry.hidden=true;discard.hidden=true;options.onBusy?.(true);
  let error='';
  try{while(batch.items.length&&!batch.stop){const item=batch.items[0];batch.controller=new AbortController();status.textContent='正在压缩照片… / 사진 압축 중…';item.file= item.file||await prepare(item.original,{signal:batch.controller.signal});batch.controller=null;if(batch.stop)break;
    status.textContent='正在上传照片… / 사진 업로드 중…';const result=await options.send(item.file,item);
    if(!result?.ok)throw Error(result?.network_error?'上传结果未确认，请先核对已保存照片，再重试未保存的照片。 / 저장된 사진을 확인한 후 재시도하세요.':result?.error||'照片上传失败 / 사진 업로드 실패');
    batch.items.shift();options.onUploaded?.(result,item.file);
   }}catch(e){error=e.name==='AbortError'?'已暂停，可重试或移除 / 일시정지. 재시도 또는 제외':e.message;}
  finally{batch.controller=null;batch.running=false;options.onBusy?.(false);stop.hidden=true;retry.hidden=!batch.items.length;discard.hidden=!batch.items.length;status.textContent=error|| (batch.items.length?'已暂停，可重试或移除 / 일시정지. 재시도 또는 제외':'照片已保存 / 사진 저장 완료');options.onDone?.({pending:batch.items.length});if(!batch.items.length){observer.disconnect();active=null;host.setAttribute('data-photo-complete','');setTimeout(()=>host.remove(),2500);}}
  return !batch.items.length;
 }
 retry.onclick=()=>run();return run();
}
window.addEventListener('beforeunload',e=>{if(active?.items.length){e.preventDefault();e.returnValue='';}});
window.CKPhotoUpload={prepare,inspect,upload,hint,busy:()=>!!active};
})();
