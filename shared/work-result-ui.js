(function(){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const photoUrl=p=>window.SOP_API+'/file?key='+encodeURIComponent(p.file_key);
window.CKResultSummary=result=>{if(!result)return '';const fields=[['packed_sku_count','品数'],['packed_count','打包箱数'],['used_carton_large_count','大纸箱'],['used_carton_small_count','小纸箱'],['repaired_box_count','修补箱数'],['reboxed_count','换箱数'],['label_count','标签数'],['operated_box_count','操作总箱数'],['pallet_count','打托数'],['forklift_location_count','叉车货位数']];return '<p><strong>'+esc(result.quantity)+' '+esc(result.unit)+'</strong></p><p>'+fields.filter(([k])=>Number(result[k])>0).map(([k,label])=>esc(label)+' '+esc(result[k])).join(' · ')+(result.used_forklift?' · 使用叉车':'')+'</p>'+(result.description?'<p>附加操作：'+esc(result.description)+'</p>':'');};
window.CKResultPhotos=result=>Array.isArray(result?.location_photos)&&result.location_photos.length?'<div class="ck-location-photos">'+result.location_photos.map(p=>'<a href="'+esc(photoUrl(p))+'" target="_blank" rel="noopener"><img src="'+esc(photoUrl(p))+'" alt="货位照片 / 화물 위치 사진" loading="lazy"></a>').join('')+'</div>':'';
window.CKLocationPhotoPicker=function(host,task){
 let photos=[],pending=false,failed=false;
 host.innerHTML='<fieldset class="ck-location-picker"><legend>货位照片（选填）/ 화물 위치 사진 (선택)</legend><p class="ck-muted">拍清货物和周围货位，方便后续找货。最多 8 张，每张不超过 10MB。</p><div class="ck-actions"><button type="button" data-photo-camera class="ck-small">拍照 / 촬영</button><button type="button" data-photo-album class="ck-small">从相册选择 / 앨범</button></div><input data-photo-input-camera type="file" accept="image/jpeg,image/png,image/webp" capture="environment" hidden><input data-photo-input-album type="file" accept="image/jpeg,image/png,image/webp" multiple hidden><div data-photo-list></div><p data-photo-status role="status"></p></fieldset>';
 const status=host.querySelector('[data-photo-status]'),render=()=>host.querySelector('[data-photo-list]').innerHTML=CKResultPhotos({location_photos:photos});
 const ready=CKSession.request('v2_attachment_list',{related_doc_type:'sop_task',related_doc_id:task.id}).then(r=>{photos=r.items.filter(p=>p.attachment_category==='location_photo');render();}).catch(e=>{failed=true;status.textContent='照片读取失败，请重新打开审核表单：'+e.message;});
 for(const source of ['camera','album']){const input=host.querySelector('[data-photo-input-'+source+']');host.querySelector('[data-photo-'+source+']').onclick=()=>input.click();input.onchange=async()=>{
  await ready;const files=[...input.files];if(!files.length)return;
  if(photos.length+files.length>8){status.textContent='最多上传 8 张照片 / 최대 8장';input.value='';return;}
  pending=true;host.querySelectorAll('button').forEach(b=>b.disabled=true);
  try{for(const file of files){if(!['image/jpeg','image/png','image/webp'].includes(file.type)||file.size>10*1024*1024)throw Error('请选择 10MB 以内的 JPG、PNG 或 WebP 照片');status.textContent='正在上传照片… / 사진 업로드 중';const fd=new FormData();fd.append('action','v2_attachment_upload');fd.append('related_doc_type','sop_task');fd.append('related_doc_id',task.id);fd.append('attachment_category','location_photo');fd.append('file',file);const response=await fetch(window.SOP_API,{method:'POST',credentials:'same-origin',body:fd}),r=await response.json();if(!r.ok)throw Error(r.error||'照片上传失败');photos.push({id:r.id,file_key:r.file_key,file_name:file.name});render();}status.textContent='照片已保存 / 사진 저장 완료';}
  catch(e){status.textContent=e.message;}finally{pending=false;input.value='';host.querySelectorAll('button').forEach(b=>b.disabled=false);}
 };}
 return {async read(){await ready;if(failed)throw Error('请先重新读取货位照片');if(pending)throw Error('请等待照片上传完成 / 사진 업로드 완료를 기다리세요');return photos.map(p=>p.id);}};
};
})();
