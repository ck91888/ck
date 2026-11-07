// Called only after the existing attachment authorization succeeds.
const escape=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export async function materialFileResponse(request,env,key,obj){
 const file=await env.DB.prepare('SELECT file_name,content_type FROM v2_attachments WHERE file_key=?').bind(key).first(),name=file?.file_name||key.split('/').pop()||'attachment';
 let mime=(file?.content_type||obj.httpMetadata?.contentType||'application/octet-stream').split(';')[0].trim().toLowerCase();
 if(['application/octet-stream','binary/octet-stream',''].includes(mime)){if(/\.pdf$/i.test(name))mime='application/pdf';else{const ext=name.split('.').pop().toLowerCase();mime=({png:'image/png',jpg:'image/jpeg',jpeg:'image/jpeg',webp:'image/webp',gif:'image/gif'})[ext]||'application/octet-stream';}}
 const image=/^image\/(png|jpeg|webp|gif)$/.test(mime),inline=mime==='application/pdf'||image,u=new URL(request.url),download=u.searchParams.get('download')==='1';
 const headers={'Content-Type':mime,'Content-Disposition':(inline&&!download?'inline':'attachment')+"; filename*=UTF-8''"+encodeURIComponent(name).replace(/['()*]/g,c=>'%'+c.charCodeAt(0).toString(16).toUpperCase()),'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'same-origin'};
 if(image&&!download&&u.searchParams.get('preview')==='1'){
  u.searchParams.delete('preview');u.searchParams.delete('download');
  return new Response('<!doctype html><html><head><meta charset="utf-8"><title>'+escape(name)+'</title><style>body{font:16px sans-serif;margin:20px}img{display:block;max-width:100%;height:auto;margin:auto}button{padding:12px;margin-bottom:16px}@media print{button,h1{display:none}body{margin:0}img{max-width:100%;max-height:100vh;object-fit:contain}}</style></head><body><button type="button" onclick="window.print()">打印 / 인쇄</button><h1>'+escape(name)+'</h1><img alt="'+escape(name)+'" src="'+escape(u.pathname+u.search)+'"></body></html>',{headers:{...headers,'Content-Type':'text/html; charset=utf-8','Content-Disposition':'inline', 'Content-Security-Policy':"default-src 'none'; img-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; frame-ancestors 'self'"}});
 }
 return new Response(obj.body,{headers});
}
