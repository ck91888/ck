import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
async function setup(enabled=true){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCEPT_NEW:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-dispatch-share'),...(enabled?{SOP_DISPATCH_SHARE_ENABLED:'true'}:{})},cookies={office:'',field:''};
 async function call(scope,action,b={},cookie){const r=await worker.fetch(new Request('https://fixture.local'+(scope==='field'?'/001/api':'/api'),{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie??cookies[scope],...operationHeaders(cookie??cookies[scope])},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);if(action==='sop_login'&&r.ok)cookies[scope]=r.headers.get('set-cookie').split(';')[0];return rememberOperation(await r.json(),cookies[scope]);}
 const ok=async(s,a,b={},c)=>{const r=await call(s,a,b,c);assert.equal(r.ok,true,a+': '+(r.error||''));return r;};
 await ok('office','sop_login',{sop_key:'synthetic-dispatch-share'});await confirmOperation((a,b)=>call('office',a,b),'合成协作管理员');await ok('office','sop_attendance_config');
 const permanent=async name=>(await ok('office','sop_attendance_register',{name,agency:'가온'})).person;
 const checkin=async p=>(await ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency})).record;
 const grant=(p,departments=['bulk'])=>ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,version:0,departments,reason:'合成核实授权'});
 const login=p=>ok('field','sop_login',{badge:p.badgeId});
 return {DB,env,cookies,call,ok,permanent,checkin,grant,login};
}
const table=db=>db.raw.prepare("SELECT name FROM sqlite_master WHERE name='ck_dispatch_share_pairs'").get();
test('disabled flag leaves collaboration closed and does not create tables',async()=>{
 const s=await setup(false),a=await s.permanent('合成关闭甲');await s.checkin(a);await s.grant(a);await s.login(a);
 const status=await s.ok('field','sop_dispatch_share_status');assert.equal(status.enabled,false);assert.deepEqual(status.partners,[]);
 assert.equal((await s.call('field','sop_dispatch_share_invite',{target_badge:'DAF-NONE'})).error,'派审协作未开启');
 assert.equal(table(s.DB),undefined);
});
test('invite, confirm, cancel, leave and same-day expiry stay pairwise',async()=>{
 const s=await setup(),a=await s.permanent('合成派审甲'),b=await s.permanent('合成派审乙'),c=await s.permanent('合成派审丙'),other=await s.permanent('合成代发派审');
 await s.checkin(a);await s.checkin(b);await s.checkin(c);await s.checkin(other);
 await s.grant(a);await s.grant(b);await s.grant(c);await s.grant(other,['direct_ship']);await s.login(a);
 assert.equal((await s.call('field','sop_dispatch_share_invite',{target_badge:a.badgeId})).ok,false);
 assert.equal((await s.call('field','sop_dispatch_share_invite',{target_badge:other.badgeId})).ok,false);
 const request='SHARE-INVITE-AB';
 const invited=await s.ok('field','sop_dispatch_share_invite',{target_badge:b.badgeId+'|'+b.name,client_req_id:request});
 assert.equal((await s.ok('field','sop_dispatch_share_invite',{target_badge:b.badgeId,client_req_id:request})).pair.id,invited.pair.id);
 assert.equal((await s.call('field','sop_dispatch_share_invite',{target_badge:b.badgeId,client_req_id:'SHARE-INVITE-AB-AGAIN'})).ok,false);
 assert.equal(s.DB.raw.prepare('SELECT COUNT(*) n FROM ck_dispatch_share_pairs').get().n,1);
 await s.login(b);assert.equal((await s.ok('field','sop_dispatch_share_status')).incoming[0].id,invited.pair.id);
 const accepted=await s.ok('field','sop_dispatch_share_accept',{pair_id:invited.pair.id,client_req_id:'SHARE-ACCEPT-AB'});
 assert.equal(accepted.pair.status,'active');
 assert.equal((await s.ok('field','sop_dispatch_share_accept',{pair_id:invited.pair.id,client_req_id:'SHARE-ACCEPT-AB'})).pair.status,'active');
 await s.login(a);const partners=await s.ok('field','sop_dispatch_share_status');assert.equal(partners.partners.length,1);assert.equal(partners.partners[0].partner.id,b.id);
 await s.login(c);assert.equal((await s.call('field','sop_dispatch_share_accept',{pair_id:invited.pair.id})).ok,false);
 await s.login(a);await s.ok('field','sop_dispatch_share_invite',{target_badge:c.badgeId});await s.login(c);const ac=(await s.ok('field','sop_dispatch_share_status')).incoming[0];await s.ok('field','sop_dispatch_share_accept',{pair_id:ac.id});
 const seen=await s.ok('field','sop_dispatch_share_status');assert.equal(seen.partners.length,1);assert.equal(seen.partners[0].partner.id,a.id);
 await s.login(b);assert.equal((await s.ok('field','sop_dispatch_share_status')).partners.some(p=>p.partner.id===c.id),false);
 await s.login(a);await s.ok('field','sop_dispatch_share_leave',{pair_id:invited.pair.id,client_req_id:'SHARE-LEAVE-AB'});
 assert.equal((await s.ok('field','sop_dispatch_share_leave',{pair_id:invited.pair.id,client_req_id:'SHARE-LEAVE-AB'})).pair.status,'ended');
 assert.equal((await s.ok('field','sop_dispatch_share_status')).partners.some(p=>p.partner.id===b.id),false);
 const again=await s.ok('field','sop_dispatch_share_invite',{target_badge:b.badgeId,client_req_id:'SHARE-REINVITE'});
 await s.ok('field','sop_dispatch_share_cancel',{pair_id:again.pair.id,client_req_id:'SHARE-CANCEL'});
 await s.login(b);assert.equal((await s.call('field','sop_dispatch_share_accept',{pair_id:again.pair.id})).ok,false);
 s.DB.raw.prepare("UPDATE ck_dispatch_share_pairs SET status='pending',day='2000-01-01',ended_at='',ended_by='' WHERE id=?").run(again.pair.id);
 assert.equal((await s.ok('field','sop_dispatch_share_status')).incoming.length,0);
 assert.match((await s.call('field','sop_dispatch_share_accept',{pair_id:again.pair.id})).error,/过期|结束/);
});
