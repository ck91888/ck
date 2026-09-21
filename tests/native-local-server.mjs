// Entirely local ephemeral fixture. Uses the real Worker and original built UI.
// Authentication is a local test fixture cookie, never a warehouse credential.
import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
const root=path.resolve('worker-v2/.sop-staging-assets'),DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_AUTO_OUTBOUND:'true',SOP_ROLLOUT_DEPARTMENTS:'bulk',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'虚拟测试负责人',role:'manager',key:crypto.randomUUID()}])};
const login=await worker.fetch(new Request('https://local.fixture/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_login',sop_key:JSON.parse(env.SOP_USERS_JSON)[0].key})}),env);
const cookie=login.headers.get('set-cookie').split(';')[0];
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://127.0.0.1:7831');
 if(url.pathname==='/api'){
  let body='';for await(const chunk of req)body+=chunk;
  const result=await worker.fetch(new Request('https://local.fixture/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body}),env);
  res.writeHead(result.status,{'Content-Type':'application/json'});res.end(await result.text());return;
 }
 let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep)&&file!==root)throw Error('invalid');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
 res.setHeader('Content-Type',({'.html':'text/html','.js':'text/javascript','.css':'text/css'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
}catch(e){res.statusCode=500;res.end(e.message);}});server.listen(7831,'0.0.0.0',()=>console.log('Local full-system acceptance fixture listening on 7831'));
