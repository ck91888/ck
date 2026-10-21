// Local workerd D1 only. No Cloudflare account, remote database, or external fetch.
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
export async function localD1(source){
 const {Miniflare}=require(require.resolve('miniflare',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
 const runtime=new Miniflare({cf:false,host:'127.0.0.1',modules:true,d1Databases:['DB'],script:`export default {async fetch(req,env){try{const queries=await req.json();return Response.json(await env.DB.batch(queries.map(q=>env.DB.prepare(q.sql).bind(...q.args))));}catch(e){return Response.json({error:e.message},{status:500});}}}`});
 const url=await runtime.ready,metrics=[];
 async function batch(statements){
  const start=performance.now(),response=await fetch(url,{method:'POST',body:JSON.stringify(statements.map(s=>({sql:s.sql,args:s.args})))}),data=await response.json();
  if(!response.ok)throw Error(data.error);
  for(let i=0;i<statements.length;i++)metrics.push({sql:statements[i].sql,sql_bytes:Buffer.byteLength(statements[i].sql),parameters:statements[i].args.length,argument_bytes:Math.max(0,...statements[i].args.map(v=>Buffer.byteLength(String(v??'')))),rows:data[i].results?.length||0,...data[i].meta});
  metrics.wall_ms=(metrics.wall_ms||0)+performance.now()-start;return data;
 }
 const prepare=sql=>{let args=[];const s={sql,get args(){return args;},bind(...v){args=v;return s;},async all(){return (await batch([s]))[0];},async first(){return (await s.all()).results[0]||null;},async run(){return s.all();}};return s;};
 const db={prepare,batch,metrics,close:()=>runtime.dispose()};
 try{
  const schema=source.prepare("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'view' THEN 2 ELSE 3 END,name").all();
  for(const item of schema.filter(s=>s.type==='table'))await prepare(item.sql).run();
  for(const item of schema.filter(s=>s.type==='table')){const data=source.prepare('SELECT * FROM "'+item.name+'"').all();for(let i=0;i<data.length;i+=100){const statements=data.slice(i,i+100).map(row=>prepare('INSERT INTO "'+item.name+'" ('+Object.keys(row).map(k=>'"'+k+'"').join(',')+') VALUES ('+Object.keys(row).map(()=>'?').join(',')+')').bind(...Object.values(row)));await batch(statements);}}
  for(const item of schema.filter(s=>s.type!=='table'))await prepare(item.sql).run();
  metrics.length=0;metrics.wall_ms=0;return db;
 }catch(e){await db.close();throw e;}
}
