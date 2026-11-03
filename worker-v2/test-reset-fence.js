import {ensureResetSchema,STAGING_DATABASE} from './test-data-reset.js';
import {shareSchemaBinding} from './schema-ready.js';
export const RESET_FENCE_SCHEMA=[
 'CREATE TABLE IF NOT EXISTS ck_test_reset_epoch(id INTEGER PRIMARY KEY CHECK(id=1),generation INTEGER NOT NULL CHECK(generation>=0))',
 'INSERT OR IGNORE INTO ck_test_reset_epoch VALUES(1,0)',
 "CREATE TABLE IF NOT EXISTS ck_test_reset_phases(run_id TEXT PRIMARY KEY,phase TEXT NOT NULL CHECK(phase IN ('draining','clearing')))",
 "UPDATE ck_test_reset_epoch SET generation=generation+1 WHERE id=1 AND EXISTS(SELECT 1 FROM ck_test_resets r WHERE r.completed_at='' AND NOT EXISTS(SELECT 1 FROM ck_test_reset_phases p WHERE p.run_id=r.id))",
 "INSERT OR IGNORE INTO ck_test_reset_phases SELECT id,'draining' FROM ck_test_resets WHERE completed_at=''",
 'CREATE TABLE IF NOT EXISTS ck_test_reset_write_guards(id TEXT PRIMARY KEY,allowed INTEGER NOT NULL CONSTRAINT ck_test_reset_epoch_guard CHECK(allowed=1))',
 'CREATE TABLE IF NOT EXISTS ck_test_reset_objects(request_id TEXT PRIMARY KEY,generation INTEGER NOT NULL,started_at TEXT NOT NULL,uncertain INTEGER NOT NULL DEFAULT 0 CHECK(uncertain IN (0,1)))'
];
export const resetFenceEnabled=e=>e.SOP_ENVIRONMENT==='staging'&&e.SOP_TEST_RESET_DATABASE===STAGING_DATABASE;
export const resetBusy=()=>Response.json({ok:false,maintenance:true,error:'测试数据正在清理或已重置，请刷新后重试 / 테스트 데이터가 초기화되었습니다. 새로고침하세요.'},{status:503,headers:{'Cache-Control':'no-store'}});
const fenced=()=>Object.assign(Error('测试数据正在清理或已重置，请刷新后重试 / test_reset_fenced'),{code:'test_reset_fenced',status:503});
const q=(db,sql,...args)=>db.prepare(sql).bind(...args);
// A successful check obtains the same SQLite write lock as the business batch.
// External operations keep their whole request eligible only during draining;
// clearing cannot begin until those requests have finished and acknowledged.
const guard=(db,c)=>q(db,`INSERT INTO ck_test_reset_write_guards(id,allowed) SELECT 'write',CASE WHEN
 (EXISTS(SELECT 1 FROM ck_test_reset_epoch WHERE id=1 AND generation=?) AND NOT EXISTS(SELECT 1 FROM ck_test_resets WHERE completed_at=''))
 OR (EXISTS(SELECT 1 FROM ck_test_reset_objects WHERE request_id=? AND generation=?) AND EXISTS(SELECT 1 FROM ck_test_resets r JOIN ck_test_reset_phases p ON p.run_id=r.id WHERE r.completed_at='' AND p.phase='draining'))
 THEN 1 ELSE 0 END WHERE 1 ON CONFLICT(id) DO UPDATE SET allowed=excluded.allowed`,c.generation,c.id,c.generation);
const readonly=sql=>/^\s*(SELECT|EXPLAIN)\b/i.test(sql||'')||/^\s*PRAGMA\s+(?:table_info|table_xinfo|index_list|index_info|index_xinfo|foreign_key_list)\s*\(/i.test(sql||'');
const contexts=new WeakMap();
function databaseFence(db,c){
 const originals=new WeakMap(),sqls=new WeakMap();
 const batch=async statements=>{
  const write=statements.some(s=>!readonly(sqls.get(s)||s.sql));
  if(!write)return db.batch(statements.map(s=>originals.get(s)||s));
  try{return (await db.batch([guard(db,c),...statements.map(s=>originals.get(s)||s)])).slice(1);}
  catch(e){if(/ck_test_reset_epoch_guard/.test(e.message||'')){c.denied=true;throw fenced();}throw e;}
 };
 const prepare=sql=>{const wrap=s=>{sqls.set(s,sql);const p=new Proxy(s,{get(target,key){if(key==='bind')return(...args)=>wrap(target.bind(...args));if(['run','all','first','raw'].includes(key)&&!readonly(sql))return async option=>{const result=(await batch([p]))[0];if(key==='first'){const row=result.results?.[0]||null;return option&&row?row[option]:row;}if(key==='raw')return (result.results||[]).map(row=>Object.values(row));return result;};const v=target[key];return typeof v==='function'?v.bind(target):v;}});originals.set(p,s);sqls.set(p,sql);return p;};return wrap(db.prepare(sql));};
 const wrapper=new Proxy(db,{get(target,key){if(key==='prepare')return prepare;if(key==='batch')return batch;if(key==='exec'||key==='withSession')return()=>{throw Error('Unsupported unguarded database operation during reset fencing');};const v=target[key];return typeof v==='function'?v.bind(target):v;}});
 shareSchemaBinding(wrapper,db);contexts.set(wrapper,c);return wrapper;
}
async function externalAdmission(db,c){
 try{await db.batch([guard(db,c),q(db,'INSERT OR IGNORE INTO ck_test_reset_objects(request_id,generation,started_at) VALUES(?,?,?)',c.id,c.generation,new Date().toISOString())]);c.external=true;}
 catch(e){if(/ck_test_reset_epoch_guard/.test(e.message||'')){c.denied=true;throw fenced();}throw e;}
}
function objectFence(bucket,db,c){return new Proxy(bucket,{get(target,key){const v=target[key];if(typeof v!=='function')return v;if(['get','head','list'].includes(key))return v.bind(target);if(!['put','delete'].includes(key))return()=>{throw Error('Unsupported object mutation during reset fencing');};return async(...args)=>{await externalAdmission(db,c);try{return await v.apply(target,args);}catch(e){c.uncertain=true;await q(db,'UPDATE ck_test_reset_objects SET uncertain=1 WHERE request_id=?',c.id).run();throw e;}};}});}
export async function withResetFence(env,run,onBusy){
 if(!resetFenceEnabled(env)||contexts.has(env.DB))return run(env);
 await ensureResetSchema(env);const db=env.DB,state=await q(db,"SELECT generation,EXISTS(SELECT 1 FROM ck_test_resets WHERE completed_at='') AS active FROM ck_test_reset_epoch WHERE id=1").first();
 if(!state||!Number.isSafeInteger(state.generation)||state.generation<0)throw Error('Reset fence state unavailable');if(state.active)return onBusy?onBusy(env):resetBusy();
 const c={id:crypto.randomUUID(),generation:state.generation,external:false,uncertain:false,denied:false},e={...env,DB:databaseFence(db,c)};
 if(env.R2_BUCKET)e.R2_BUCKET=objectFence(env.R2_BUCKET,db,c);
 try{const result=await run(e);return c.denied?resetBusy():result;}
 catch(error){if(error.code==='test_reset_fenced')return resetBusy();throw error;}
 finally{if(c.external&&!c.uncertain)await q(db,'DELETE FROM ck_test_reset_objects WHERE request_id=? AND generation=? AND uncertain=0',c.id,c.generation).run();}
}
export async function beginResetClearing(env,runId){
 await q(env.DB,"UPDATE ck_test_reset_phases SET phase='clearing' WHERE run_id=? AND phase='draining' AND NOT EXISTS(SELECT 1 FROM ck_test_reset_objects)",runId).run();
 const phase=await q(env.DB,'SELECT phase FROM ck_test_reset_phases WHERE run_id=?',runId).first();
 if(phase?.phase!=='clearing')throw Object.assign(Error('存在尚未确认完成的附件操作，尚未开始清理；请稍后重试或联系维护人员核实 / Pending object operation; reset has not started clearing'),{status:409});
}
