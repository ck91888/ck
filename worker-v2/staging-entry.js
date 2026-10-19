import {accessEnabled,accessScope} from './access-control.js';
import app from './index.js';
import {sessionUser} from './sop-session.js';
import {enabled,ensureResetSchema,activeReset,resetAction} from './test-data-reset.js';
const response=(body,status=200)=>Response.json(body,{status,headers:{'Cache-Control':'no-store'}});
const isApiPath=path=>['/api','/file','/api/file','/001/api','/001/api/file','/001/file','/attendance/api'].includes(path)||path.startsWith('/api/test-data/');
const staging={async fetch(request,env,ctx){
 const url=new URL(request.url),maintenance=url.pathname.startsWith('/api/test-data/');
 if(accessEnabled(env)){
  const api=isApiPath(url.pathname);
  if(!api){
   // Only shipped code/styles are public and cacheable; pages and API data stay authenticated.
   const staticAsset=/^\/(?:shared|001|002|003|shuju|attendance)\/[^/]+\.(?:js|css)$/.test(url.pathname);
   const publicAsset=staticAsset||url.pathname.startsWith('/shared/')||url.pathname.startsWith('/001/')||url.pathname.startsWith('/attendance/')||url.pathname.startsWith('/office-login/')||url.pathname==='/release.json';
   if(!publicAsset&&!await sessionUser(request,env))return Response.redirect(url.origin+'/office-login/?next='+encodeURIComponent(url.pathname+url.search),302);
   const asset=await env.ASSETS.fetch(request);const headers=new Headers(asset.headers);headers.set('Cache-Control',staticAsset&&/^[a-f0-9]{12}$/.test(url.searchParams.get('v')||'')?'public, max-age=31536000, immutable':'no-store');headers.set('X-Content-Type-Options','nosniff');headers.set('Referrer-Policy','same-origin');headers.set('Content-Security-Policy',"frame-ancestors 'none'");return new Response(asset.body,{status:asset.status,headers});
  }
 }

 if(!enabled(request,env))return maintenance?response({ok:false,error:'Not available'},404):app.fetch(request,env,ctx);
 if(maintenance){
  try{
   const action=url.pathname.slice('/api/test-data/'.length);
   if(action==='status'&&request.method!=='GET')return response({ok:false,error:'Method not allowed'},405);
   if(action!=='status'){
    if(request.method!=='POST')return response({ok:false,error:'Method not allowed'},405);
    if(request.headers.get('Origin')!==url.origin||request.headers.get('X-CK-Test-Reset')!=='1'||!request.headers.get('Content-Type')?.includes('application/json'))return response({ok:false,error:'Invalid request origin'},403);
   }
   const user=await sessionUser(request,env);
   return response(await resetAction(action,action==='status'?{}:await request.json(),env,user));
  }catch(error){return response({ok:false,error:error.message},error.status||500);}
 }
 try{
  await ensureResetSchema(env);
  if(await activeReset(env)){
   // Keep login available so a manager can resume an interrupted reset.
   const body=request.method==='POST'&&request.headers.get('Content-Type')?.includes('application/json')?await request.clone().json().catch(()=>({})):{};
   const action=body.action||url.searchParams.get('action');
   if(!['sop_identity','sop_login','sop_logout'].includes(action))return response({ok:false,maintenance:true,error:'测试数据正在清理，请稍后刷新 / 테스트 데이터 삭제 중입니다'},503);
  }
 }catch(error){return response({ok:false,error:'测试维护状态读取失败，请稍后重试 / 잠시 후 다시 시도하세요'},503);}
 return app.fetch(request,env,ctx);
}};
export default {async fetch(request,env,ctx){
 if(!isApiPath(new URL(request.url).pathname))return staging.fetch(request,env,ctx);
 const started=performance.now(),result=await staging.fetch(request,env,ctx);
 const headers=new Headers(result.headers);
 // Time until the application returns a Response; not DNS, TLS, transit or body download.
 // Only a duration is exposed: no SQL, user identity, action payload or credentials.
 headers.append('Server-Timing','app;dur='+Math.max(0,performance.now()-started).toFixed(1));
 headers.set('Cache-Control','private, no-store');
 return new Response(result.body,{status:result.status,statusText:result.statusText,headers});
}};
