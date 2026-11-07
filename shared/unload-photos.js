(function(){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const copy={title:['到货照片','도착 사진'],empty:['暂无到货照片','등록된 도착 사진 없음'],open:['查看照片','사진 보기'],camera:['拍照','사진 촬영'],album:['添加到货图片','도착 사진 추가'],hint:['拍清货物外观、包装和箱唛。最多12张，每张10MB以内。上传前自动压缩，不另存拍摄原图，请确认细字、条码。','화물·포장·박스 표기를 촬영하세요. 최대 12장, 장당 10MB 이하. 압축 업로드, 촬영 원본 별도 보관 안 함. 작은 글자·바코드를 확인하세요.'],saving:['正在保存照片…','사진 저장 중…'],saved:['照片已保存，客服可在协同中心查看','사진 저장 완료. 협업센터에서 확인할 수 있습니다.'],retry:['重试未上传照片','사진 업로드 재시도'],remove:['移除待上传照片','대기 사진 제외'],pending:['待上传','업로드 대기'],loading:['正在读取照片…','사진 불러오는 중…']};
if(window.LANG)for(const [key,[zh,ko]] of Object.entries(copy)){LANG.zh['ck_arrival_'+key]=zh;LANG.ko['ck_arrival_'+key]=ko;}
const text=key=>location.pathname.startsWith('/002/')&&window.L?L('ck_arrival_'+key):copy[key].join(' / ');
const label=key=>'<span data-i18n="ck_arrival_'+key+'">'+esc(text(key))+'</span>';
const url=p=>window.SOP_API+'/file?key='+encodeURIComponent(p.file_key);
const time=t=>t?new Date(t).toLocaleString('zh-CN',{timeZone:'Asia/Seoul',hour12:false}):'';
function thumbnails(photos){return photos.map(p=>'<figure><a href="'+esc(url(p))+'" target="_blank" rel="noopener" aria-label="'+esc(text('open')+' '+p.file_name)+'"><img src="'+esc(url(p))+'" alt="'+esc(p.file_name)+'" loading="lazy"></a><figcaption>'+esc(p.uploaded_by)+'<br>'+esc(time(p.created_at))+'</figcaption></figure>').join('');}
function gallery(photos){return '<section class="card ck-arrival-gallery"><h3>'+label('title')+' <small>'+photos.length+'</small></h3>'+(photos.length?'<div class="ck-arrival-grid">'+thumbnails(photos)+'</div>':'<p class="muted">'+label('empty')+'</p>')+'</section>';}
const instances=new Map(),owners=new WeakMap();let ownerSequence=0;
const idleLimit=16;
const changed=()=>Error('登录或操作身份已变化，请返回原身份处理待上传照片 / 원래 작업자로 돌아와 대기 사진을 확인하세요');
function ownerScope(){
 const session=window.CKSession,explicit=!!session&&'user' in session,user=explicit?session.user:session,field=explicit&&session.scope==='field';
 const number=object=>{if(!object||typeof object!=='object')return 0;if(!owners.has(object))owners.set(object,++ownerSequence);return owners.get(object);};
 const stamp=value=>JSON.stringify(field?[session?.scope,value?.id,value?.attendance_id,value?.badge]:[session?.scope,value?.id,value?.name,value?.role,value?.departments,value?.operation_context]);
 const saved=stamp(user),allowed=!!session&&(!explicit||!!user?.id);
 // The field session replaces user objects on its normal 15-second identity probe.
 // Keep the verified person/badge/workday queue; every retry still uses the server's current authorization.
 return{key:JSON.stringify([number(session),field?0:number(user),saved]),owns:()=>{const current=explicit?session?.user:session;return allowed&&window.CKSession===session&&!!current&&(!explicit||field||current===user)&&stamp(current)===saved;}};
}
function prune(keep){
 const idle=[...instances.entries()].filter(([,p])=>p!==keep&&!p.host.isConnected&&!p.hasPending());
 for(const [key,p]of idle.slice(0,Math.max(0,idle.length-idleLimit))){if(p.dispose())instances.delete(key);}
}
function picker(parent,{jobId,type,target}){
 const scope=ownerScope(),key=JSON.stringify([jobId,type,target,scope.key]),existing=instances.get(key);
 for(const p of instances.values())if(p.host.parentNode===parent&&!p.owned())p.host.remove();
 if(existing){parent.append(existing.host);instances.delete(key);instances.set(key,existing);existing.refresh().catch(()=>{});prune(existing);return existing;}
 const host=document.createElement('section');parent.append(host);
 let photos=[],pending=[],flight=null,reading=null,refreshAfterFlight=null,loadError=null,lastError=null,disposed=false;
 const selections=new Set(),owned=()=>!disposed&&scope.owns(),revoke=url=>{if(url)URL.revokeObjectURL(url);};
 host.classList.add('ck-arrival-picker');
 host.innerHTML='<h4>'+label('title')+' <small data-count></small></h4><p class="ck-trip-help">'+label('hint')+'</p><div class="ck-arrival-actions"><button type="button" data-camera class="btn btn-outline">'+label('camera')+'</button><button type="button" data-album class="btn btn-outline">'+label('album')+'</button><button type="button" data-retry class="btn btn-outline" hidden>'+label('retry')+'</button></div><input data-input-camera type="file" accept="image/jpeg,image/png,image/webp" capture="environment" hidden><input data-input-album type="file" accept="image/jpeg,image/png,image/webp" multiple hidden><p role="status" aria-live="polite"></p><div class="ck-arrival-grid" data-photos></div>';
 const status=host.querySelector('[role=status]'),buttons=[...host.querySelectorAll('button')];
 const hasPending=()=>!!flight||!!pending.length||!!selections.size;
 const render=()=>{if(disposed)return;const active=owned();buttons.forEach(b=>b.disabled=!active||!!flight);host.querySelector('[data-count]').textContent=active?photos.length+' / 12':'';host.querySelector('[data-photos]').innerHTML=active?thumbnails(photos)+pending.map((p,i)=>'<figure>'+(p.preview?'<img src="'+esc(p.preview)+'" alt="'+esc((p.file||p.original).name)+'">':'')+'<figcaption>'+label('pending')+'<button type="button" data-remove="'+i+'" '+(flight?'disabled':'')+'>'+label('remove')+'</button></figcaption></figure>').join(''):'';host.querySelector('[data-retry]').hidden=!active||!pending.length&&!loadError;if(!active)status.textContent=changed().message;};
 function load(){
  if(!owned()){loadError=changed();render();return Promise.reject(loadError);}if(reading)return reading;
  status.textContent=text('loading');
  reading=(async()=>{const r=await CKSession.request('v2_attachment_list',{related_doc_type:type,related_doc_id:target});if(!owned())throw changed();photos=(r.items||[]).filter(p=>p&&p.attachment_category==='unload_photo');loadError=null;render();if(!flight)status.textContent=pending.length?(lastError?.message||text('pending')):'';
  })().catch(e=>{loadError=e;if(!disposed){if(owned()&&[401,403].includes(e.httpStatus))photos=[];status.textContent=owned()?e.message:changed().message;render();}throw e;}).finally(()=>{reading=null;prune();});return reading;
 }
 function refresh(){
  if(flight){if(!refreshAfterFlight)refreshAfterFlight=flight.catch(()=>{}).then(()=>load()).finally(()=>{refreshAfterFlight=null;});return refreshAfterFlight;}return load();
 }
 const ready=load().catch(()=>{});
 function save(){if(flight)return flight;if(!owned())return Promise.reject(changed());lastError=null;
  flight=(async()=>{await ready;if(reading)await reading;if(loadError)await load();if(!owned())throw changed();while(pending.length){
   const item=pending[0];status.textContent='正在压缩照片… / 사진 압축 중…';if(!item.file){if(!window.CKPhotoUpload)throw Error('照片处理组件尚未加载，请刷新后重试 / 새로고침 후 사진을 다시 선택하세요');item.file=await CKPhotoUpload.prepare(item.original);revoke(item.preview);item.preview=URL.createObjectURL(item.file);item.original=null;render();}if(!owned())throw changed();status.textContent=text('saving');
   const form=new FormData();for(const [k,v]of Object.entries({action:'v2_attachment_upload',job_id:jobId,related_doc_type:type,related_doc_id:target,attachment_category:'unload_photo',client_req_id:item.request}))form.set(k,v);form.set('file',item.file);
   const response=await fetch(window.SOP_API,{method:'POST',credentials:'same-origin',body:form}),result=await response.json();if(!owned())throw changed();if(response.ok===false||!result.ok)throw Error(result.error||'照片保存失败 / 사진 저장 실패');if(!result.attachment||!result.id||result.attachment.id!==result.id||!result.attachment.file_key)throw Error('照片保存回执不完整，请重试 / 사진 저장 응답을 확인할 수 없습니다. 재시도하세요');
   if(!photos.some(p=>p.id===result.id))photos.push(result.attachment);pending.shift();revoke(item.preview);render();
  }status.textContent=photos.length?text('saved'):'';
  })().catch(e=>{lastError=e;if(!disposed)status.textContent=owned()?e.message:changed().message;throw e;}).finally(()=>{flight=null;render();prune();});render();return flight;
 }
 for(const source of ['camera','album']){const input=host.querySelector('[data-input-'+source+']');host.querySelector('[data-'+source+']').onclick=()=>{if(owned())input.click();};input.onchange=()=>{
  const files=[...input.files];input.value='';if(!files.length)return Promise.resolve();if(!owned()){render();return Promise.resolve();}
  if(files.some(f=>!['image/jpeg','image/png','image/webp'].includes(f.type)||!f.size||f.size>10*1024*1024)){status.textContent='请选择10MB以内的JPG、PNG或WebP照片 / 10MB 이하 JPG·PNG·WebP 사진';return Promise.resolve();}
  const selection=(async()=>{await ready;if(owned()&&photos.length+pending.length+files.length>12){status.textContent='最多12张照片 / 최대 12장';return;}pending.push(...files.map(file=>({original:file,file:null,request:crypto.randomUUID(),preview:''})));render();if(owned())save().catch(()=>{});})().catch(e=>{lastError=e;if(!disposed)status.textContent=owned()?e.message:changed().message;});selections.add(selection);selection.finally(()=>{selections.delete(selection);render();prune();});return selection;
 };}
 host.querySelector('[data-retry]').onclick=()=>save().catch(()=>{});
 host.querySelector('[data-photos]').onclick=e=>{const b=e.target.closest('[data-remove]');if(!b||flight||!owned())return;const item=pending.splice(Number(b.dataset.remove),1)[0];if(item)revoke(item.preview);render();if(!pending.length){lastError=null;status.textContent=photos.length?text('saved'):'';}prune();};
 const instance={host,owned,refresh,flush:async()=>{await Promise.all([...selections]);if(!owned())throw changed();if(flight)await flight;if(pending.length)await save();},hasPending,getPhotos:()=>owned()?photos:[],dispose:()=>{if(hasPending())return false;disposed=true;photos=[];host.remove();host.replaceChildren();if(instances.get(key)===instance)instances.delete(key);return true;}};instances.set(key,instance);render();prune(instance);return instance;
}
const busy=()=>{prune();return[...instances.values()].some(x=>x.hasPending());};
window.addEventListener('beforeunload',e=>{if(busy()){e.preventDefault();e.returnValue='';}});
window.CKUnloadPhotos={gallery,picker,busy};
})();
