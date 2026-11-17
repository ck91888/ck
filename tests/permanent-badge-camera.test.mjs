import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

if(!process.env.CK_JSDOM_MODULE)throw Error('CK_JSDOM_MODULE is required');
const mod=await import(process.env.CK_JSDOM_MODULE),{JSDOM}=mod.default||mod;
const flush=()=>new Promise(resolve=>setImmediate(resolve));
const person={id:'ATP-CAMERA-SYNTHETIC',badgeId:'DAF-CAMERA-SYNTHETIC',name:'虚构长期相机甲',agency:'가온',badgeType:'permanent',enabled:true};

async function setup(t,{defer=false,reject='',enabled=true,existing=false,lookupError='',mode='fixed'}={}){
 const dom=new JSDOM(fs.readFileSync(new URL('../attendance/index.html',import.meta.url),'utf8'),{url:'https://fixture.test/attendance/',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window;
 t.after(()=>w.close());const calls=[],scanners=[];let holdLookup=false,resolveLookup,holdCheckin=false,resolveCheckin;
 Object.defineProperty(w,'isSecureContext',{value:true});
 Object.defineProperty(w.navigator,'mediaDevices',{value:{getUserMedia(){throw Error('Only synthetic scanner permitted');}}});
 w.Html5Qrcode=class{
  constructor(id){this.id=id;this.live=false;this.stops=0;scanners.push(this);}
  start(camera,options,decoded){this.camera=camera;this.decoded=decoded;return new Promise((resolve,fail)=>{this.resolve=()=>{this.live=true;resolve();};if(reject)fail(Object.assign(Error(reject),{name:reject}));else if(!defer)this.resolve();});}
  stop(){this.stops++;this.live=false;return Promise.resolve();}
  clear(){this.cleared=true;}
 };
 w.qrcode=Object.assign(()=>({addData(){},make(){},createSvgTag(){return '<svg></svg>';}}),{stringToBytesFuncs:{'UTF-8':()=>[]}});
 w.CKSession={ready:Promise.resolve(),user:{role:'kiosk'},request:async(action,body={})=>{
  calls.push({action,body});
  if(action==='sop_attendance_config')return {agencies:['가온','하늘'],asOf:new Date().toISOString()};
  if(action==='sop_attendance_lookup'){
   if(lookupError)throw Error(lookupError);
   const result={person:{...person,enabled},record:existing?{...person,id:'DAY-SYNTHETIC',inAt:new Date().toISOString()}:null};
   if(holdLookup)return new Promise(resolve=>resolveLookup=()=>resolve(result));
   return result;
  }
  const result={record:{...person,id:'DAY-SYNTHETIC',agency:body.agency||person.agency,inAt:new Date().toISOString()}};
  if(holdCheckin&&action==='sop_attendance_checkin')return new Promise(resolve=>resolveCheckin=()=>resolve(result));
  return result;
 }};
 w.eval(fs.readFileSync(new URL('../shared/attendance-ui.js',import.meta.url),'utf8'));
 w.eval(fs.readFileSync(new URL('../shared/employee-attendance.js',import.meta.url),'utf8'));
 await w.CKAttendanceKiosk();await w.CKEmployee.kiosk();
 if(mode.startsWith('employee')){w.document.querySelector('[data-kind=employee]').click();if(mode==='employee-out')w.document.querySelector('[data-out]').click();}
 else if(mode==='daily-out')w.document.getElementById('tab-out').click();
 else w.document.getElementById('fixed').click();
 const host=()=>w.document.querySelector(mode.startsWith('employee')?'.ck-employee-terminal':'#terminal-content');
 return {w,calls,scanners,host,start:()=>host().querySelector('[data-camera-start]').click(),emit:(raw,index=scanners.length-1)=>scanners[index].decoded(raw),
  submit:()=>host().querySelector('form').requestSubmit(),lookupCalls:()=>calls.filter(c=>c.action==='sop_attendance_lookup'),checkinCalls:()=>calls.filter(c=>c.action==='sop_attendance_checkin'),
  holdLookup(){holdLookup=true;},resolveLookup:()=>resolveLookup(),holdCheckin(){holdCheckin=true;},resolveCheckin:()=>resolveCheckin()};
}

test('permanent QR reads registered identity once, stops camera, and requires separate check-in confirmation',async t=>{
 const s=await setup(t);assert.equal(s.scanners.length,0);s.holdLookup();s.start();await flush();
 assert.equal(s.scanners[0].camera.facingMode,'environment');
 await Promise.all([s.emit(person.badgeId+'|伪造二维码姓名'),s.emit('DAF-SECOND|不应采用')]);
 assert.equal(s.lookupCalls().length,1);assert.equal(s.lookupCalls()[0].body.badge,person.badgeId);assert.equal(s.checkinCalls().length,0);assert.equal(s.scanners[0].live,false);
 s.resolveLookup();await flush();assert.match(s.host().textContent,/虚构长期相机甲/);assert.doesNotMatch(s.host().textContent,/伪造二维码姓名|不应采用/);
 assert.equal(s.host().querySelector('input:checked').value,'가온');assert.match(s.host().textContent,/DAF-CAMERA-SYNTHETIC/);
 s.host().querySelector('input[value="하늘"]').checked=true;s.holdCheckin();s.submit();s.submit();
 assert.equal(s.checkinCalls().length,1);assert.equal(s.checkinCalls()[0].body.agency,'하늘');s.resolveCheckin();await flush();assert.match(s.host().textContent,/已签到/);
});

test('manual badge and scanner Enter retain lookup followed by explicit confirmation',async t=>{
 const s=await setup(t);s.w.document.getElementById('code').value=person.badgeId+'|任意扫码枪姓名';s.submit();await flush();
 assert.equal(s.scanners.length,0);assert.equal(s.lookupCalls().length,1);assert.equal(s.checkinCalls().length,0);assert.match(s.host().textContent,/虚构长期相机甲/);
 s.submit();await flush();assert.equal(s.checkinCalls().length,1);
});

for(const existing of [false,true])test('disabled permanent badge displays identity without check-in even with existing='+existing,async t=>{
 const s=await setup(t,{enabled:false,existing});s.start();await flush();await s.emit(person.badgeId);await flush();
 assert.match(s.host().textContent,/虚构长期相机甲/);assert.match(s.host().textContent,/工牌已停用/);assert.equal(s.host().querySelector('button.primary'),null);assert.equal(s.host().querySelector('form'),null);assert.equal(s.checkinCalls().length,0);assert.equal(s.scanners[0].live,false);
});

test('daily and employee badge families cannot trigger permanent lookup',async t=>{
 const s=await setup(t);s.start();await flush();for(const code of ['DA-SYNTHETIC','EMP-SYNTHETIC','DAF-','DAF-INVALID!'])await s.emit(code);
 assert.match(s.host().querySelector('[data-camera-status]').textContent,/DAF/);assert.equal(s.lookupCalls().length,0);assert.equal(s.w.document.getElementById('code').value,'');assert.equal(s.scanners[0].live,true);
});

test('unregistered permanent lookup error leaves manual input and camera retry available',async t=>{
 const s=await setup(t,{lookupError:'工牌未登记，请联系工作人员 / 등록되지 않은 명찰입니다'});s.start();await flush();await s.emit('DAF-UNKNOWN-SYNTHETIC');await flush();
 assert.match(s.host().querySelector('[role=alert]').textContent,/未登记/);assert.equal(s.checkinCalls().length,0);assert.equal(s.host().querySelector('[data-camera-start]').disabled,false);assert.equal(s.w.document.getElementById('code').disabled,false);
});

test('late permanent lookup cannot replace a different visible form',async t=>{
 const s=await setup(t);s.holdLookup();s.start();await flush();await s.emit(person.badgeId);s.w.document.getElementById('tab-out').click();await flush();s.resolveLookup();await flush();
 assert.match(s.host().textContent,/请扫描工牌下班/);assert.doesNotMatch(s.host().textContent,/虚构长期相机甲/);assert.equal(s.checkinCalls().length,0);
});

test('closing pending permanent camera ignores late frames and releases eventual stream',async t=>{
 const s=await setup(t,{defer:true});s.start();s.host().querySelector('[data-camera-close]').click();await s.emit(person.badgeId);s.scanners[0].resolve();await flush();await flush();
 assert.equal(s.scanners[0].live,false);assert.equal(s.scanners[0].stops,1);assert.equal(s.lookupCalls().length,0);assert.equal(s.w.document.getElementById('code').value,'');
});

test('pagehide cancels a new camera waiting for the previous camera to finish stopping',async t=>{
 const s=await setup(t);s.start();await flush();const previous=s.scanners[0];let finishStop;
 previous.stop=()=>new Promise(resolve=>finishStop=()=>{previous.live=false;resolve();});
 const second=s.w.document.createElement('form');second.innerHTML='<input>';s.w.document.body.append(second);
 s.w.CKAttendance.badgeCamera(second,second.querySelector('input'));second.querySelector('[data-camera-start]').click();
 s.w.dispatchEvent(new s.w.Event('pagehide'));finishStop();await flush();await flush();
 assert.equal(s.scanners.length,1);assert.equal(previous.live,false);assert.equal(second.querySelector('[data-camera-panel]').hidden,true);
});

for(const mode of ['fixed','daily-out','employee-in','employee-out']){
 test(mode+' permission denial keeps manual input and retry usable',async t=>{
  const s=await setup(t,{mode,reject:'NotAllowedError'});s.start();await flush();assert.match(s.host().querySelector('[data-camera-status]').textContent,/权限被拒绝/);assert.equal(s.host().querySelector('input').disabled,false);assert.equal(s.host().querySelector('[data-camera-start]').disabled,false);assert.equal(s.checkinCalls().length,0);
 });
 test(mode+' close, reopen and backgrounding release camera and reject late frames',async t=>{
  const s=await setup(t,{mode}),code=mode.startsWith('employee')?'EMP-SYNTHETIC':person.badgeId;s.start();await flush();s.host().querySelector('[data-camera-close]').click();await flush();await s.emit(code,0);assert.equal(s.host().querySelector('input').value,'');
  s.start();await flush();Object.defineProperty(s.w.document,'hidden',{configurable:true,value:true});s.w.document.dispatchEvent(new s.w.Event('visibilitychange'));await flush();await s.emit(code,1);
  assert.ok(s.scanners.every(scanner=>!scanner.live));assert.equal(s.host().querySelector('input').value,'');assert.equal(s.lookupCalls().length,0);assert.equal(s.checkinCalls().length,0);
 });
}
