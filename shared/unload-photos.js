(function(){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const copy={title:['到货照片','도착 사진'],empty:['暂无到货照片','등록된 도착 사진 없음'],open:['查看原图','원본 보기'],camera:['拍照','사진 촬영'],album:['添加到货图片','도착 사진 추가'],hint:['拍清货物外观、包装和箱唛。最多12张，每张10MB以内。','화물·포장·박스 표기를 촬영하세요. 최대 12장, 장당 10MB 이하.'],saving:['正在保存照片…','사진 저장 중…'],saved:['照片已保存，客服可在协同中心查看','사진 저장 완료. 협업센터에서 확인할 수 있습니다.'],retry:['重试未上传照片','사진 업로드 재시도'],remove:['移除待上传照片','대기 사진 제외'],pending:['待上传','업로드 대기'],loading:['正在读取照片…','사진 불러오는 중…']};
if(window.LANG)for(const [key,[zh,ko]] of Object.entries(copy)){LANG.zh['ck_arrival_'+key]=zh;LANG.ko['ck_arrival_'+key]=ko;}
const text=key=>location.pathname.startsWith('/002/')&&window.L?L('ck_arrival_'+key):copy[key].join(' / ');
const label=key=>'<span data-i18n="ck_arrival_'+key+'">'+esc(text(key))+'</span>';
const url=p=>window.SOP_API+'/file?key='+encodeURIComponent(p.file_key);
const time=t=>t?new Date(t).toLocaleString('zh-CN',{timeZone:'Asia/Seoul',hour12:false}):'';
function thumbnails(photos){return photos.map(p=>'<figure><a href="'+esc(url(p))+'" target="_blank" rel="noopener" aria-label="'+esc(text('open')+' '+p.file_name)+'"><img src="'+esc(url(p))+'" alt="'+esc(p.file_name)+'" loading="lazy"></a><figcaption>'+esc(p.uploaded_by)+'<br>'+esc(time(p.created_at))+'</figcaption></figure>').join('');}
function gallery(photos){return '<section class="card ck-arrival-gallery"><h3>'+label('title')+' <small>'+photos.length+'</small></h3>'+(photos.length?'<div class="ck-arrival-grid">'+thumbnails(photos)+'</div>':'<p class="muted">'+label('empty')+'</p>')+'</section>';}
const instances=new Map();
function picker(parent,{jobId,type,target}){
 const key=[jobId,type,target].join('|'),existing=instances.get(key);if(existing){parent.append(existing.host);return existing;}
 const host=document.createElement('section');parent.append(host);
 let photos=[],pending=[],flight=null,loadError=null;
 host.classList.add('ck-arrival-picker');
 host.innerHTML='<h4>'+label('title')+' <small data-count></small></h4><p class="ck-trip-help">'+label('hint')+'</p><div class="ck-arrival-actions"><button type="button" data-camera class="btn btn-outline">'+label('camera')+'</button><button type="button" data-album class="btn btn-outline">'+label('album')+'</button><button type="button" data-retry class="btn btn-outline" hidden>'+label('retry')+'</button></div><input data-input-camera type="file" accept="image/jpeg,image/png,image/webp" capture="environment" hidden><input data-input-album type="file" accept="image/jpeg,image/png,image/webp" multiple hidden><p role="status" aria-live="polite"></p><div class="ck-arrival-grid" data-photos></div>';
 const status=host.querySelector('[role=status]'),buttons=[...host.querySelectorAll('button')];
 const render=()=>{host.querySelector('[data-count]').textContent=photos.length+' / 12';host.querySelector('[data-photos]').innerHTML=thumbnails(photos)+pending.map((p,i)=>'<figure><img src="'+esc(p.preview)+'" alt="'+esc(p.file.name)+'"><figcaption>'+label('pending')+'<button type="button" data-remove="'+i+'" '+(flight?'disabled':'')+'>'+label('remove')+'</button></figcaption></figure>').join('');host.querySelector('[data-retry]').hidden=!pending.length&&!loadError;};
 async function load(){const r=await CKSession.request('v2_attachment_list',{related_doc_type:type,related_doc_id:target});photos=(r.items||[]).filter(p=>p.attachment_category==='unload_photo');loadError=null;render();}
 status.textContent=text('loading');const ready=load().then(()=>{status.textContent='';}).catch(e=>{loadError=e;status.textContent=e.message;render();});
 function save(){if(flight)return flight;buttons.forEach(b=>b.disabled=true);
  flight=(async()=>{await ready;if(loadError)await load();while(pending.length){const item=pending[0];status.textContent=text('saving');const form=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',job_id:jobId,related_doc_type:type,related_doc_id:target,attachment_category:'unload_photo',client_req_id:item.request}))form.set(k,v);form.set('file',item.file);const response=await fetch(window.SOP_API,{method:'POST',credentials:'same-origin',body:form}),result=await response.json();if(!result.ok)throw Error(result.error||'照片保存失败 / 사진 저장 실패');if(!photos.some(p=>p.id===result.id))photos.push(result.attachment);pending.shift();URL.revokeObjectURL(item.preview);render();}status.textContent=photos.length?text('saved'):'';})().catch(e=>{status.textContent=e.message;throw e;}).finally(()=>{flight=null;buttons.forEach(b=>b.disabled=false);render();});render();return flight;
 }
 for(const source of ['camera','album']){const input=host.querySelector('[data-input-'+source+']');host.querySelector('[data-'+source+']').onclick=()=>input.click();input.onchange=async()=>{await ready;const files=[...input.files];input.value='';if(!files.length)return;if(photos.length+pending.length+files.length>12){status.textContent='最多12张照片 / 최대 12장';return;}if(files.some(f=>!['image/jpeg','image/png','image/webp'].includes(f.type)||!f.size||f.size>10*1024*1024)){status.textContent='请选择10MB以内的JPG、PNG或WebP照片 / 10MB 이하 JPG·PNG·WebP 사진';return;}pending.push(...files.map(file=>({file,request:crypto.randomUUID(),preview:URL.createObjectURL(file)})));render();save().catch(()=>{});};}
 host.querySelector('[data-retry]').onclick=()=>save().catch(()=>{});
 host.querySelector('[data-photos]').onclick=e=>{const b=e.target.closest('[data-remove]');if(!b||flight)return;const item=pending.splice(Number(b.dataset.remove),1)[0];if(item)URL.revokeObjectURL(item.preview);render();if(!pending.length)status.textContent=photos.length?text('saved'):'';};
 const instance={host,flush:async()=>{if(flight)await flight;if(pending.length)await save();},hasPending:()=>!!flight||!!pending.length,getPhotos:()=>photos};instances.set(key,instance);return instance;
}
const busy=()=>[...instances.values()].some(x=>x.hasPending());
window.addEventListener('beforeunload',e=>{if(busy()){e.preventDefault();e.returnValue='';}});
window.CKUnloadPhotos={gallery,picker,busy};
})();
