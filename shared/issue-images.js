// Private originals remain unchanged. Small visible photos load two at a time;
// large/unknown files wait for a click. Blob URLs live only in this page/view.
(function(){
 'use strict';
 const entries=new Map(),queue=[];let active=0,current=null,modal=null,observer=null,scheduled=false,openedView=null;
 const t=(zh,ko)=>window.getLang?.()==='ko'?ko:zh;
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const size=n=>n>=1048576?(n/1048576).toFixed(1)+' MB':Math.ceil(n/1024)+' KB';
 const visible=el=>el.isConnected&&el.getClientRects().length>0;
 function entry(url,name='',bytes=0){let e=entries.get(url);if(!e){e={url,name,bytes,cells:new Set(),state:'idle',blob:'',controller:null,used:Date.now()};entries.set(url,e);}return e;}
 function status(e){if(e.state==='ready')return t('图片已加载','사진 로딩 완료');if(e.state==='loading')return t('原图加载中','원본 로딩 중')+' · '+size(e.loaded||0)+(e.bytes?' / '+size(e.bytes):'');if(e.state==='queued')return t('等待加载','로딩 대기');if(e.error)return e.error;return (e.bytes?size(e.bytes)+' · ':'')+t('点击查看原图','클릭하여 원본 보기');}
 function paint(e){
  for(const cell of e.cells){const img=cell.querySelector('img'),note=cell.querySelector('[data-image-status]'),button=cell.querySelector('[data-image-open]');if(e.blob&&img.src!==e.blob)img.src=e.blob;else if(!e.blob)img.removeAttribute('src');img.hidden=!e.blob;note.textContent=status(e);button.textContent=e.state==='error'||e.state==='cancelled'?t('重试图片','사진 다시 시도'):t('查看图片','사진 보기');}
  if(current===e&&modal){modal.querySelector('[data-view-status]').textContent=status(e);modal.querySelector('[data-view-retry]').hidden=!['error','cancelled'].includes(e.state);const img=modal.querySelector('img');if(e.blob&&img.src!==e.blob)img.src=e.blob;else if(!e.blob)img.removeAttribute('src');img.hidden=!e.blob;}
 }
 function release(e){e.controller?.abort();e.cancelled=true;e.generation=(e.generation||0)+1;if(e.blob)URL.revokeObjectURL(e.blob);e.blob='';e.state='idle';e.error='';paint(e);entries.delete(e.url);}
 function trim(){const ready=[...entries.values()].filter(e=>e.blob&&e!==current&&e.bytes>262144).sort((a,b)=>b.used-a.used);let pixels=current?.pixels||0,bytes=current?.bytes||0;for(const [index,e] of ready.entries()){pixels+=e.pixels||0;bytes+=e.bytes||0;if(index>=3||pixels>24000000||bytes>24*1048576)release(e);}}
 function ensureModal(){
  if(modal)return;
  modal=document.getElementById('lightbox');modal.removeAttribute('onclick');modal.classList.add('ck-image-viewer');modal.setAttribute('role','dialog');modal.setAttribute('aria-modal','true');modal.setAttribute('aria-label','图片原图 / 원본 사진');
  modal.innerHTML='<div class="ck-image-toolbar"><span data-view-name></span><a data-view-download target="_blank" rel="noopener">下载原图 / 원본 다운로드</a><button type="button" data-view-close>关闭 / 닫기</button></div><div class="ck-image-stage"><img id="lightboxImg" alt="原始图片 / 원본 사진" hidden></div><div class="ck-image-status"><span role="status" aria-live="polite" data-view-status></span><button type="button" data-view-retry hidden>重试图片 / 사진 다시 시도</button></div>';
  modal.querySelector('[data-view-close]').onclick=close;modal.querySelector('[data-view-retry]').onclick=()=>load(current,true);
  modal.onclick=event=>{if(event.target===modal||event.target.classList.contains('ck-image-stage'))close();};
  modal.onkeydown=event=>{if(event.key==='Escape'){event.preventDefault();close();}if(event.key==='Tab'){const buttons=[...modal.querySelectorAll('a,button')].filter(visible),first=buttons[0],last=buttons.at(-1);if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}}};
 }
 let returnFocus=null;
 function close(){if(!modal||!current)return;const e=current;current=null;modal.classList.remove('show');modal.querySelector('img').removeAttribute('src');if(e&&['queued','loading'].includes(e.state)){e.cancelled=true;e.controller?.abort();e.state='cancelled';e.error=t('加载已取消，可重试','로딩 취소됨. 다시 시도하세요.');paint(e);}if(returnFocus?.isConnected)returnFocus.focus();trim();}
 function open(url,name='',bytes=0){ensureModal();openedView=window._currentView;if(current&&current.url!==url)close();returnFocus=document.activeElement;current=entry(url,name,bytes);current.used=Date.now();modal.querySelector('[data-view-name]').textContent=current.name||t('原始图片','원본 사진');const link=modal.querySelector('[data-view-download]');link.href=url;link.download=current.name;modal.classList.add('show');modal.querySelector('[data-view-close]').focus();paint(current);load(current,true);}
 function load(e,priority=false){if(!e||['ready','loading','queued'].includes(e.state))return;e.state='queued';e.error='';e.cancelled=false;e.generation=(e.generation||0)+1;paint(e);const item={e,generation:e.generation};if(priority)queue.unshift(item);else queue.push(item);pump();}
 function pump(){while(active<2&&queue.length){const {e,generation}=queue.shift();if(e.cancelled||entries.get(e.url)!==e||e.generation!==generation)continue;active++;run(e,generation).finally(()=>{active--;pump();});}}
 async function run(e,generation){
  const controller=e.controller=new AbortController();let timedOut=false,url='',timer=setTimeout(()=>{timedOut=true;controller.abort();},120000);e.state='loading';e.loaded=0;paint(e);
  try{
   const response=await fetch(e.url,{credentials:'same-origin',signal:controller.signal});
   if(!response.ok){const message=response.status===401||response.status===403?t('登录已过期或无此附件权限，请确认登录','로그인 또는 첨부 권한을 확인하세요'):response.status===404?t('原始文件不存在，请联系提交人','원본 파일이 없습니다. 제출자에게 문의하세요'):t('图片服务暂不可用','사진 서비스 일시 오류');throw Error(message+' (HTTP '+response.status+')');}
   const contentType=(response.headers.get('Content-Type')||'').split(';')[0].toLowerCase();
   if(contentType&&contentType!=='application/octet-stream'&&!contentType.startsWith('image/'))throw Error(t('服务返回的不是图片，请重试或下载原图','이미지 응답이 아닙니다. 다시 시도하거나 원본을 다운로드하세요'));
   let blob;
   if(response.body?.getReader){const reader=response.body.getReader(),chunks=[];while(true){const {value,done}=await reader.read();if(done)break;chunks.push(value);e.loaded+=value.length;if(e.loaded>24*1048576){await reader.cancel();throw Error(t('图片过大，请下载原图查看','사진이 너무 큽니다. 원본을 다운로드하세요'));}paint(e);}blob=new Blob(chunks,{type:contentType});}else blob=await response.blob();
   if(controller.signal.aborted)throw Error('cancelled');
   // Historical uploads sometimes have an empty/octet-stream MIME. Identify only
   // supported raster signatures; never render HTML/SVG as a trusted preview.
   const bytes=new Uint8Array(await blob.slice(0,16).arrayBuffer());let type='';
   if(bytes[0]===137&&bytes[1]===80&&bytes[2]===78&&bytes[3]===71)type='image/png';
   else if(bytes[0]===255&&bytes[1]===216&&bytes[2]===255)type='image/jpeg';
   else if(String.fromCharCode(...bytes.slice(0,4))==='RIFF'&&String.fromCharCode(...bytes.slice(8,12))==='WEBP')type='image/webp';
   else if(String.fromCharCode(...bytes.slice(0,3))==='GIF')type='image/gif';
   if(!type)throw Error(t('图片格式无法预览，请下载原图核对','미리 볼 수 없는 형식입니다. 원본을 다운로드하세요'));
   if(blob.size>24*1048576)throw Error(t('图片过大，请下载原图查看','사진이 너무 큽니다. 원본을 다운로드하세요'));
   url=URL.createObjectURL(blob.slice(0,blob.size,type));
   const img=new Image();await new Promise((resolve,reject)=>{const abort=()=>reject(Error('cancelled'));controller.signal.addEventListener('abort',abort,{once:true});img.onload=()=>{controller.signal.removeEventListener('abort',abort);resolve();};img.onerror=()=>{controller.signal.removeEventListener('abort',abort);reject(Error(t('图片无法解码，请重试或下载原图','이미지를 열 수 없습니다. 다시 시도하거나 원본을 다운로드하세요')));};img.src=url;if(controller.signal.aborted)abort();});
   if(controller.signal.aborted||entries.get(e.url)!==e)throw Error('cancelled');
   e.blob=url;e.pixels=img.naturalWidth*img.naturalHeight;url='';e.state='ready';e.used=Date.now();paint(e);trim();
  }catch(error){if(entries.get(e.url)===e&&e.generation===generation){e.state=controller.signal.aborted&&!timedOut?'cancelled':'error';e.error=timedOut?t('图片加载超时，可重试或下载原图','사진 로딩 시간 초과. 다시 시도하거나 원본을 다운로드하세요'):controller.signal.aborted?t('加载已取消，可重试','로딩 취소됨. 다시 시도하세요.'):error.name==='TypeError'||/network error|load failed|failed to fetch/i.test(error.message)?t('未收到完整图片，请重试或下载原图','전체 사진을 받지 못했습니다. 다시 시도하거나 원본을 다운로드하세요'):error.message;paint(e);}}
  finally{clearTimeout(timer);if(url)URL.revokeObjectURL(url);if(e.controller===controller)e.controller=null;}
 }
 function scan(){scheduled=false;for(const e of entries.values()){for(const cell of e.cells)if(!visible(cell)){observer?.unobserve(cell);e.cells.delete(cell);}if(!e.cells.size&&e!==current)release(e);}
  if(current&&openedView!==window._currentView)close();
  document.querySelectorAll('[data-issue-image]').forEach(cell=>{if(!visible(cell))return;const e=entry(cell.dataset.imageUrl,cell.dataset.imageName,Number(cell.dataset.imageSize));if(e.cells.has(cell))return;e.cells.add(cell);paint(e);if(e.bytes>0&&e.bytes<=262144&&observer)observer.observe(cell);});
 }
 function schedule(){if(!scheduled){scheduled=true;requestAnimationFrame(scan);}}
 window.CKIssueImages={html(att,url){return '<div class="att-cell issue-photo-cell" data-issue-image data-image-url="'+esc(url)+'" data-image-name="'+esc(att.file_name||'')+'" data-image-size="'+Number(att.file_size||0)+'"><img class="att-thumb issue-photo-thumb" alt="'+esc(att.file_name||'照片 / 사진')+'" hidden><div class="issue-photo-name">'+esc(att.file_name||'照片 / 사진')+'</div><div class="issue-image-note">'+esc(att.uploaded_by||'')+(att.created_at?' · '+esc(window.fmtTime?fmtTime(att.created_at):att.created_at):'')+'</div><div class="issue-image-note" data-image-status></div><button type="button" data-image-open>'+t('查看图片','사진 보기')+'</button><a href="'+esc(url)+'" download="'+esc(att.file_name||'')+'" target="_blank" rel="noopener">'+t('下载原图','원본 다운로드')+'</a></div>';},open,close};
 document.addEventListener('DOMContentLoaded',()=>{if('IntersectionObserver'in window)observer=new IntersectionObserver(items=>{for(const {target,isIntersecting}of items)if(isIntersecting){observer.unobserve(target);load(entries.get(target.dataset.imageUrl));}},{rootMargin:'0px'});
  document.addEventListener('click',event=>{const cell=event.target.closest('[data-issue-image]');if(cell&&(event.target.closest('[data-image-open]')||event.target.tagName==='IMG'))open(cell.dataset.imageUrl,cell.dataset.imageName,Number(cell.dataset.imageSize));});
  new MutationObserver(schedule).observe(document.body,{childList:true,subtree:true,attributes:true,attributeFilter:['class','style','hidden']});schedule();
 });
 window.addEventListener('pagehide',()=>{close();for(const e of entries.values())release(e);});
})();
