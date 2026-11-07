(function(){
'use strict';
const e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const type=f=>/\.pdf$/i.test(f.file_name||'')||f.content_type==='application/pdf'?'pdf':/\.(png|jpe?g|webp|gif)$/i.test(f.file_name||'')||/^image\/(png|jpeg|webp|gif)$/.test(f.content_type||'')?'image':'';
function url(f,mode='open'){const u=new URL(window.SOP_API||'/api',location.origin);u.search='';u.searchParams.set('action','v2_attachment_get');u.searchParams.set('file_key',f.file_key);if(mode==='download')u.searchParams.set('download','1');else if(type(f)==='image')u.searchParams.set('preview','1');return u.pathname+u.search;}
function name(f){if(!f.file_key)return e(f.file_name);const preview=!!type(f);return '<a data-material-name href="'+e(url(f,preview?'open':'download'))+'" '+(preview?'target="_blank" rel="noopener"':'download="'+e(f.file_name)+'"')+'>'+e(f.file_name)+'</a>';}
function actions(f){if(!f.file_key)return '';return '<span data-material-actions> '+(type(f)?'<a data-material-open href="'+e(url(f))+'" target="_blank" rel="noopener">打开/打印 / 열기·인쇄</a> · ':'')+'<a data-material-download href="'+e(url(f,'download'))+'" download="'+e(f.file_name)+'">下载 / 다운로드</a></span>';}
window.CKMaterialFiles={url,name,actions,links:f=>name(f)+actions(f)};
})();
