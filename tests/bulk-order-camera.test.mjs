import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const {JSDOM}=(await import(process.env.CK_JSDOM_MODULE)).default;
const flush=()=>new Promise(resolve=>setImmediate(resolve));
async function setup(t,{external=true,defer=false,reject='',unsupported=false}={}){
 const dom=new JSDOM('<body><form><div data-selection></div></form></body>',{url:'https://fixture.test/001/',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window;
 const calls=[],scanners=[];let resolveLookup=null,hold=false,fail=false;
 Object.defineProperty(w,'isSecureContext',{value:true});Object.defineProperty(w.navigator,'mediaDevices',{value:unsupported?undefined:{getUserMedia(){throw Error('Synthetic camera only');}}});
 w.Html5Qrcode=class{constructor(id){this.id=id;scanners.push(this);}start(camera,options,decoded){this.camera=camera;this.options=options;this.decoded=decoded;return new Promise((resolve,fail)=>{this.resolve=()=>{this.live=true;w.document.getElementById(this.id)?.append(w.document.createElement('video'));resolve();};if(reject)fail(Object.assign(Error(reject),{name:reject}));else if(!defer)this.resolve();});}async stop(){this.live=false;this.stops=(this.stops||0)+1;}clear(){w.document.getElementById(this.id)?.replaceChildren();}};
 const item=code=>external?{key:'external:'+code,source_type:'external',work_order_no:code,customer:'合成客户',token:'synthetic-token'}:{key:code,source_type:'need',id:code,revision:2,display_no:code,customer:'合成客户',cargo_groups:{groups:[{id:'GROUP-A'}]}};
 w.CKSession={user:{id:'SYNTHETIC-FIELD',role:'field',departments:['bulk']},request:async(action,body)=>{calls.push({action,body});assert.equal(action,'sop_bulk_batch_candidates');if(!body.code)return {items:[]};if(hold)return new Promise(resolve=>resolveLookup=()=>resolve({items:[item(body.code)]}));if(fail)throw Error('合成拒绝：此单暂不可派工');return {items:[item(body.code)]};}};
 for(const file of ['sop-people.js','attendance-ui.js','bulk-work-batch.js'])w.eval(fs.readFileSync(new URL('../shared/'+file,import.meta.url),'utf8'));
 const form=w.document.querySelector('form'),host=form.querySelector('[data-selection]');form.insertAdjacentHTML('beforeend',w.CKPeopleFields()+'<input name="fixture_customer" value="尚未提交的客户"><button type="submit">派工</button>');
 const picker=w.CKPeoplePicker(form),selection=w.CKBulkOrderSelection(host,{external,need:{id:'SEED',display_no:'SYNTHETIC-SEED',customer:'合成当前客户',revision:1},picker});
 t.after(async()=>{await selection.destroy();await picker.destroy();w.close();});await flush();
 return {w,host,form,picker,selection,calls,scanners,input:host.querySelector('[data-bulk-code]'),start:()=>host.querySelector('[data-camera-start]').click(),emit:raw=>scanners.at(-1).decoded(raw),hold(){hold=true;},resolve(){hold=false;resolveLookup();},fail(value){fail=value;}};
}
for(const external of [false,true])test(`camera adds ${external?'external':'ordinary/grouped'} order through original read validation only`,async t=>{
 const s=await setup(t,{external});s.start();await flush();assert.equal(s.scanners.length,1);assert.equal(s.scanners[0].camera.facingMode,'environment');assert.deepEqual(JSON.parse(JSON.stringify(s.scanners[0].options.qrbox(240,300))),{width:204,height:160});assert.deepEqual(JSON.parse(JSON.stringify(s.scanners[0].options.qrbox(0,0))),{width:50,height:50});assert.equal(s.host.querySelector('video').playsInline,true);
 await s.emit('SYNTHETIC-SECOND');await flush();assert.equal(s.scanners[0].live,false);assert.equal(s.selection.read().length,2);assert.equal(s.calls.filter(x=>x.body.code==='SYNTHETIC-SECOND').length,1);assert.ok(s.calls.every(x=>x.action==='sop_bulk_batch_candidates'));assert.equal(s.input.value,'');assert.equal(s.form.elements.fixture_customer.value,'尚未提交的客户');assert.match(s.host.querySelector('[data-order-status]').textContent,/已加入/);assert.equal(s.host.querySelector('[data-camera-start]').disabled,false);
 s.start();await flush();await s.emit('SYNTHETIC-SECOND');await flush();assert.equal(s.selection.read().length,2);
});
test('one decoded frame and duplicate callbacks create only one candidate lookup; manual Enter still works',async t=>{
 const s=await setup(t);s.start();await flush();await Promise.all([s.emit('SYNTHETIC-SECOND'),s.emit('SYNTHETIC-THIRD')]);await flush();assert.equal(s.calls.length,1);s.input.value='SYNTHETIC-THIRD';const event=new s.w.KeyboardEvent('keydown',{key:'Enter',bubbles:true,cancelable:true});s.input.dispatchEvent(event);await flush();assert.equal(event.defaultPrevented,true);assert.equal(s.selection.read().length,3);assert.equal(s.calls.length,2);
});
test('failed validation preserves scanned number and selected orders; retry uses original add button',async t=>{
 const s=await setup(t);s.fail(true);s.start();await flush();await s.emit('SYNTHETIC-RETRY');await flush();assert.equal(s.selection.read(),null);assert.equal(s.input.value,'SYNTHETIC-RETRY');assert.match(s.host.querySelector('[data-order-status]').textContent,/合成拒绝/);assert.equal(s.input.disabled,false);s.fail(false);s.host.querySelector('[data-order-add]').click();await flush();assert.equal(s.selection.read().length,2);
});
for(const reject of ['NotAllowedError','NotFoundError','NotReadableError'])test(reject+' leaves order-number input available',async t=>{
 const s=await setup(t,{reject});s.start();await flush();await flush();assert.match(s.host.querySelector('[data-camera-status]').textContent,/单号/);assert.equal(s.input.disabled,false);assert.equal(s.host.querySelector('[data-camera-start]').disabled,false);s.input.value='SYNTHETIC-MANUAL';s.host.querySelector('[data-order-add]').click();await flush();assert.equal(s.selection.read().length,2);
});
test('unsupported camera offers manual fallback without opening media',async t=>{const s=await setup(t,{unsupported:true});s.start();assert.equal(s.scanners.length,0);assert.match(s.host.querySelector('[data-camera-status]').textContent,/不支持.*单号/);});
test('close while permission is pending stops late stream and ignores decoded frames',async t=>{
 const s=await setup(t,{defer:true});s.start();await flush();s.host.querySelector('[data-camera-close]').click();s.scanners[0].resolve();await flush();await flush();await s.emit('SYNTHETIC-LATE');await flush();assert.equal(s.scanners[0].live,false);assert.equal(s.calls.length,0);assert.equal(s.selection.read(),null);
});
test('destroy during lookup ignores late result and stops camera without clearing other form input',async t=>{const s=await setup(t);s.hold();s.start();await flush();await s.emit('SYNTHETIC-LATE');await flush();assert.throws(()=>s.selection.read(),/正在核对追加单号/);await s.selection.destroy();s.resolve();await flush();assert.equal(s.host.querySelector('[data-bulk-selected]').children.length,1);assert.equal(s.form.elements.fixture_customer.value,'尚未提交的客户');});
for(const lock of ['event','identity'])test(lock+' change discards old-account lookup and camera callbacks',async t=>{
 const s=await setup(t);s.hold();s.start();await flush();await s.emit('SYNTHETIC-OLD-ACCOUNT');await flush();if(lock==='event')s.w.dispatchEvent(new s.w.Event('ck-field-session-change'));else s.w.CKSession.user={id:'SYNTHETIC-OTHER',role:'field',departments:['bulk']};s.resolve();await flush();assert.equal(s.host.querySelector('[data-bulk-selected]').children.length,1);assert.equal(s.scanners[0].live,false);s.start();await flush();assert.equal(s.scanners.length,1);
});
test('staff and order cameras stop each other before opening; neither camera dispatches work',async t=>{
 const s=await setup(t);s.form.querySelector('[data-staff-camera]').click();await flush();assert.equal(s.scanners[0].live,true);s.start();await flush();await flush();assert.equal(s.scanners[0].live,false);assert.equal(s.scanners[1].live,true);s.form.querySelector('[data-staff-camera]').click();await flush();await flush();assert.equal(s.scanners[1].live,false);assert.equal(s.scanners[2].live,true);assert.equal(s.calls.length,0);
});
test('collapse, hide, pagehide and reopen release media; empty frames do not submit',async t=>{
 const s=await setup(t);s.start();await flush();await s.emit('   ');assert.equal(s.calls.length,0);assert.equal(s.scanners.at(-1).live,true);const details=s.host.querySelector('details');details.open=false;details.dispatchEvent(new s.w.Event('toggle'));await flush();assert.equal(s.scanners.at(-1).live,false);details.open=true;details.dispatchEvent(new s.w.Event('toggle'));s.start();await flush();s.w.dispatchEvent(new s.w.Event('pagehide'));await flush();assert.equal(s.scanners.at(-1).live,false);s.start();await flush();s.form.hidden=true;await flush();assert.equal(s.scanners.at(-1).live,false);
});
