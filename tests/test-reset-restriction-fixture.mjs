// Synthetic memory/local workerd only. The database marker never connects to
// a remote D1 database, and Request URLs are handled directly by local JS.
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';import entry from '../worker-v2/staging-entry.js';
import {ensureResetSchema,STAGING_DATABASE,STAGING_HOST} from '../worker-v2/test-data-reset.js';
import {operationHeaders} from './office-operator-fixture.mjs';
export async function restrictionFixture(t,{active=false,partial=false}={}){
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);t.mock.timers.enable({apis:['Date'],now:Date.parse(day+'T09:00:00+09:00')});
 const f=await ownedHomeFixture(),objects=new Map([['synthetic/preserved-object','synthetic bytes']]);let native;t.after(async()=>{await native?.close();f.DB.raw.close();});
 Object.assign(f.env,{SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true',SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE,R2_BUCKET:{async put(key,body){objects.set(key,body);},async get(key){return objects.get(key);},async delete(key){objects.delete(key);}}});
 await f.ok('office','sop_identity');const person=await f.person(),need=await f.need('合成停用重置验证'),plan=await f.ok('office','v2_inbound_plan_create',{customer:'合成保留入库',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:2}]});
 const kioskResponse=await f.worker.fetch(new Request('https://'+STAGING_HOST+'/attendance/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_login',sop_key:'synthetic-owned-home-fixture'})}),f.env),kioskBody=await kioskResponse.json();assert.equal(kioskBody.ok,true,JSON.stringify(kioskBody));f.cookies.kiosk=kioskResponse.headers.getSetCookie().find(x=>x.startsWith('ck_kiosk_access=')).split(';')[0];
 f.DB.raw.prepare("INSERT INTO v2_003_locations(id,location_name) VALUES('SYN-RESET-SETTING','合成保留库位')").run();
 let runId=null;if(active){await ensureResetSchema(f.env);runId='a'.repeat(32);f.DB.raw.prepare('INSERT INTO ck_test_resets(id,request_id,actor,started_at,tables_json) VALUES(?,?,?,?,?)').run(runId,'synthetic-existing-run','synthetic-manager',new Date().toISOString(),JSON.stringify(['v2_inbound_plans']));f.DB.raw.exec('CREATE TABLE ck_backup_synthetic_preserved AS SELECT * FROM v2_inbound_plans');if(partial)f.DB.raw.exec('DELETE FROM ck_attendance_spans');}
 if(process.env.CK_P1_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 const q=(sql,...args)=>f.env.DB.prepare(sql).bind(...args),rows=async(sql,...args)=>(await q(sql,...args).all()).results;
 const snapshot=async(excluded=[])=>{const result={};for(const {name}of await rows("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name"))if(!excluded.includes(name))result[name]=await rows('SELECT * FROM "'+name+'" ORDER BY rowid');return JSON.stringify(result);};
 const request=async(path,{body,method=body?'POST':'GET',scope='office',host=STAGING_HOST,changes={},headers={}}={})=>{const cookie=f.cookies[scope]||'',multi=body instanceof FormData;return entry.fetch(new Request('https://'+host+path,{method,headers:{Cookie:cookie,...operationHeaders(cookie),...(body?{Origin:'https://'+host,...(!multi?{'Content-Type':'application/json'}:{}),...(path.startsWith('/api/test-data/')?{'X-CK-Test-Reset':'1'}:{})}:{}),...headers},...(body?{body:multi?body:JSON.stringify(body)}:{})}),{...f.env,...changes});};
 return {...f,objects,person,need,plan,runId,q,rows,snapshot,request};
}
