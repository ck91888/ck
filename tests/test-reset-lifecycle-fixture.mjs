// Local synthetic fixture only; the staging database ID is an authorization
// marker in env, never a remote database connection or Cloudflare API call.
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import entry from '../worker-v2/staging-entry.js';
import {STAGING_DATABASE,STAGING_HOST} from '../worker-v2/test-data-reset.js';
export async function resetLifecycleFixture(t){
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);t.mock.timers.enable({apis:['Date'],now:Date.parse(day+'T09:00:00+09:00')});
 const f=await ownedHomeFixture();let native;t.after(async()=>{await native?.close();f.DB.raw.close();});
 Object.assign(f.env,{SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_DISPATCH_SHARE_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true',SOP_ATTENDANCE_SHIFT_ACCOUNTING:'true',SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE});
 const preview=()=>f.ok('office','sop_attendance_return_preview',{badge:f.people.A.badgeId});
 const cursor=p=>({badge:f.people.A.badgeId,id:p.record.id,version:p.version,span_id:p.span_id,span_version:p.span_version});
 const first=await f.task('A','合成重置前派审');t.mock.timers.tick(10*60000);
 await f.ok('office','sop_attendance_checkout',cursor(await preview()));t.mock.timers.tick(10*60000);
 await f.ok('office','sop_attendance_return_confirm',{...cursor(await preview()),confirmed:true});await f.ok('A','sop_login',{badge:f.people.A.badgeId});
 const second=await f.task('A','合成返岗后新派审'),pair=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:pair.pair.id});
 f.DB.raw.prepare("INSERT INTO v2_003_locations(id,location_name) VALUES('SYN-RESET-KEEP','合成保留库位')").run();
 if(process.env.CK_P1_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 const query=(sql,...args)=>f.env.DB.prepare(sql).bind(...args),rows=async(sql,...args)=>(await query(sql,...args).all()).results;
 const reset=async(action,body={},changes={})=>{
  const response=await entry.fetch(new Request('https://'+STAGING_HOST+'/api/test-data/'+action,{method:action==='status'?'GET':'POST',headers:{Cookie:f.cookies.office,Origin:'https://'+STAGING_HOST,'Content-Type':'application/json','X-CK-Test-Reset':'1'},...(action==='status'?{}:{body:JSON.stringify(body)})}),{...f.env,...changes});
  return {status:response.status,result:await response.json()};
 };
 const okReset=async(action,body)=>{const {status,result}=await reset(action,body);assert.equal(result.ok,true,action+': '+JSON.stringify({status,result}));return result;};
 const complete=async run=>{let result;for(let i=0;i<100;i++){result=await okReset('step',{runId:run.id});if(result.run.completedAt)return result;}assert.fail('Synthetic reset did not complete in bounded steps');};
 return {...f,first,second,query,rows,reset,okReset,complete};
}
