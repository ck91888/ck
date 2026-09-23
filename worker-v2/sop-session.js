import {accessEnabled,accessUser,accessSessionAction} from './access-control.js';
// Same-origin session for the isolated, full-system acceptance environment.
// Personal keys stay on the server after sign-in; never placed in URLs or browser storage.
import { principal, handleSop } from './sop.js';
const cookieName='ck_sop_session';
const lifetime=8*60*60;
const enc=new TextEncoder();
const b64=bytes=>btoa(String.fromCharCode(...bytes)).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');
async function signingKey(env){return crypto.subtle.importKey('raw',enc.encode(env.SOP_USERS_JSON||''),{name:'HMAC',hash:'SHA-256'},false,['sign','verify']);}
async function sign(value,env){return b64(new Uint8Array(await crypto.subtle.sign('HMAC',await signingKey(env),enc.encode(value))));}
function cookie(value,seconds){return `${cookieName}=${value}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${seconds}`;}
export async function sessionUser(request,env){
 if(accessEnabled(env))return accessUser(request,env);
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true')return null;
 if(env.SOP_PUBLIC_TEST_ACCESS==='true')return {id:'staging-demo-manager',name:'测试负责人',role:'manager',departments:['bulk','direct_ship','import'],public_test:true};
 const value=(request.headers.get('Cookie')||'').split(';').map(x=>x.trim()).find(x=>x.startsWith(cookieName+'='))?.slice(cookieName.length+1);
 if(!value)return null;
 try{
  const [data,signature]=value.split('.');
  const bytes=Uint8Array.from(atob(signature.replaceAll('-','+').replaceAll('_','/')),x=>x.charCodeAt(0));
  if(!await crypto.subtle.verify('HMAC',await signingKey(env),bytes,enc.encode(data)))return null;
  const p=JSON.parse(decodeURIComponent(atob(data.replaceAll('-','+').replaceAll('_','/'))));
  if(!p.exp||p.exp<Date.now())return null;
  const users=JSON.parse(env.SOP_USERS_JSON);return users.find(x=>x.id===p.id)||null;
 }catch{return null;}
}
export async function sessionAction(body,env,request){
 if(accessEnabled(env))return accessSessionAction(body,env,request);
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_UPGRADE_ENABLED!=='true')return null;
 const headers={'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'};
 const response=(value)=>new Response(JSON.stringify(value),{headers});
 if(body.action==='sop_logout'){headers['Set-Cookie']=cookie('',0);return response({ok:true});}
 if(body.action==='sop_identity'){
  const u=env.SOP_REQUEST_USER;
  return response(u?{ok:true,user:{id:u.id,name:u.name,role:u.role,departments:u.departments||[],public_test:!!u.public_test}}:{ok:false,unauthorized:true,error:'请登录测试系统'});
 }
 if(body.action!=='sop_login')return null;
 const result=await handleSop({...body,action:'sop_session'},{...env,SOP_REQUEST_USER:null});
 if(!result.ok)return response(result);
 const u=principal(body,env);
 const data=b64(enc.encode(encodeURIComponent(JSON.stringify({id:u.id,exp:Date.now()+lifetime*1000}))));
 headers['Set-Cookie']=cookie(data+'.'+await sign(data,env),lifetime);
 return response(result);
}
