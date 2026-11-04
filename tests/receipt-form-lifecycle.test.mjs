import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const runtime=await import(process.env.CK_JSDOM_MODULE),{JSDOM}=runtime.default||runtime;
const text=file=>fs.readFileSync(new URL('../'+file,import.meta.url),'utf8');
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return{promise,resolve,reject};}
function shipment(id){return {shipment:{id,shipment_no:id,tracking_no:'TRACK-'+id,status:'pending',delivery_method:'express'},items:[{id:'ITEM-'+id,material_code:'SYN-M',name_zh:'Synthetic receipt item',unit:'个',expected_qty:10,location_code:'SYN-A01'}],attachments:[]};}
async function fixture(t){
 const dom=new JSDOM(text('003/index.html'),{url:'https://synthetic.invalid/003/',runScripts:'outside-only'});t.after(()=>dom.window.close());const w=dom.window;
 await new Promise(r=>w.document.readyState==='loading'?w.document.addEventListener('DOMContentLoaded',r,{once:true}):r());
 w.scrollTo=()=>{};w.eval(text('003/config.js'));w.eval(text('shared/latest-list-read.js'));w.eval(text('003/app.js'));
 w.CKSession={scope:'office',user:{id:'SYN-A',role:'manager'}};Object.assign(w.S003,{role:'admin',operatorId:'SYN-A',operatorName:'Synthetic A',locations:[{location_code:'SYN-A01',active:1}],currentView:'receiving'});
 const calls=[],messages=[],renders=[];w.toast=(...x)=>messages.push(x);w.errorText=x=>x;
 for(const fn of ['loadDashboard','loadFieldHome','loadMaterials','loadAssets','loadPurchasing','loadReceiving'])w[fn]=()=>{};
 const gates=new Map();w.api=async(action,data)=>{calls.push({action,data});const q=gates.get(action);if(q?.length)return q.shift().promise;if(action==='v2_003_location_list')return{items:[{location_code:'SYN-A01',active:1}]};throw Error('Unexpected synthetic API '+action);};
 const enqueue=action=>{const d=deferred();if(!gates.has(action))gates.set(action,[]);gates.get(action).push(d);return d;};
 const tick=()=>new Promise(r=>setImmediate(r));
 return{w,calls,messages,renders,enqueue,tick};
}
test('duplicate receipt lookup uses one request and never erases entered quantities or notes',async t=>{
 const f=await fixture(t),d=f.enqueue('v2_003_receiving_lookup'),second=f.enqueue('v2_003_receiving_lookup');const a=f.w.openReceiptByCode('SYN-1'),b=f.w.openReceiptByCode('SYN-1');d.resolve(shipment('SYN-1'));await a;f.w.document.querySelector('.rc-qty').value='7';f.w.E('receiptDiffNote').value='Synthetic note';second.resolve(shipment('SYN-1'));await b;
 assert.equal(f.calls.filter(x=>x.action==='v2_003_receiving_lookup').length,1);assert.equal(f.w.document.querySelector('.rc-qty').value,'7');assert.equal(f.w.E('receiptDiffNote').value,'Synthetic note');await f.w.openReceiptByCode('TRACK-SYN-1');assert.equal(f.w.document.querySelector('.rc-qty').value,'7');
});
for(const stale of ['success','failure'])test('latest receipt wins against older '+stale,async t=>{
 const f=await fixture(t),a=f.enqueue('v2_003_receiving_lookup'),b=f.enqueue('v2_003_receiving_lookup');const pa=f.w.openReceiptByCode('A'),pb=f.w.openReceiptByCode('B');b.resolve(shipment('B'));await pb;f.w.document.querySelector('.rc-qty').value='6';stale==='success'?a.resolve(shipment('A')):a.reject(Error('old error'));await pa;assert.equal(f.w.S003.currentShipment.shipment.id,'B');assert.equal(f.w.document.querySelector('.rc-qty').value,'6');assert.deepEqual(f.messages,[]);
});
for(const mode of ['navigate','null-session','replace-session','legacy-identity'])test('late receipt response cannot act after '+mode,async t=>{
 const f=await fixture(t),d=f.enqueue('v2_003_receiving_lookup'),p=f.w.openReceiptByCode('A');if(mode==='navigate')f.w.goView('assets');if(mode==='null-session')f.w.CKSession.user=null;if(mode==='replace-session')f.w.CKSession.user={id:'SYN-B',role:'manager'};if(mode==='legacy-identity')f.w.S003.operatorId='SYN-B';d.resolve(shipment('A'));await p;assert.notEqual(f.w.S003.currentShipment?.shipment.id,'A');assert.equal(f.w.E('receiptDetail').innerHTML,'');assert.deepEqual(f.messages,[]);
});
test('closing and reopening lookup rejects the earlier modal request and permits retry after failure',async t=>{
 const f=await fixture(t),a=f.enqueue('v2_003_receiving_lookup'),pa=f.w.openReceiptByCode('A');f.w.closeReceivingScanner();const b=f.enqueue('v2_003_receiving_lookup'),pb=f.w.openReceiptByCode('A');a.resolve(shipment('OLD'));await pa;assert.equal(f.w.E('receiptDetail').innerHTML,'');b.reject(Error('current error'));await pb;assert.equal(f.messages.at(-1)[0],'current error');const c=f.enqueue('v2_003_receiving_lookup'),pc=f.w.openReceiptByCode('A');c.resolve(shipment('A'));await pc;assert.equal(f.w.S003.currentShipment.shipment.id,'A');
});
test('late location lookup cannot revive a receipt after navigation',async t=>{
 const f=await fixture(t);f.w.S003.locations=[];const a=f.enqueue('v2_003_receiving_lookup'),loc=f.enqueue('v2_003_location_list'),p=f.w.openReceiptByCode('A');a.resolve(shipment('A'));await f.tick();f.w.goView('assets');loc.resolve({items:[{location_code:'SYN-A01',active:1}]});await p;assert.equal(f.w.S003.currentView,'assets');assert.equal(f.w.E('receiptDetail').innerHTML,'');
});
test('choosing the displayed receipt again invalidates a different pending lookup without discarding edits',async t=>{
 const f=await fixture(t),a=f.enqueue('v2_003_receiving_lookup'),pa=f.w.openReceiptByCode('A');a.resolve(shipment('A'));await pa;f.w.document.querySelector('.rc-qty').value='7';const b=f.enqueue('v2_003_receiving_lookup'),pb=f.w.openReceiptByCode('B');await f.w.openReceiptByCode('A');b.resolve(shipment('B'));await pb;assert.equal(f.w.S003.currentShipment.shipment.id,'A');assert.equal(f.w.document.querySelector('.rc-qty').value,'7');
});
for(const kind of ['material','asset'])for(const mode of ['reopen','navigate','identity','failure','normal'])test(kind+' save completion respects '+mode,async t=>{
 const f=await fixture(t),w=f.w,prefix=kind==='material'?'mf':'af',open=kind==='material'?'openMaterialForm':'openAssetForm',save=kind==='material'?'saveMaterial':'saveAsset',action='v2_003_'+kind+'_save';w[open]();w.E(prefix+'NameZh').value='Synthetic original';w.E(prefix+'Category').value='Synthetic';if(kind==='material')w.E('mfUnit').value='个';let detail=null;w[kind==='material'?'openMaterialDetail':'openAssetDetail']=id=>{detail=id;};const d=f.enqueue(action),p=w[save]({preventDefault(){}});
 if(mode==='reopen'){w[open]();w.E(prefix+'NameZh').value='Synthetic new draft';}if(mode==='navigate'||mode==='failure')w.goView('receiving');if(mode==='identity')w.CKSession.user=null;
 mode==='failure'?d.reject(Error('old error')):d.resolve({id:'SAVED'});await p;assert.equal(f.calls.find(x=>x.action===action).data.name_zh,'Synthetic original');assert.equal(detail,mode==='normal'?'SAVED':null);if(mode==='reopen')assert.equal(w.E(prefix+'NameZh').value,'Synthetic new draft');if(mode!=='normal')assert.deepEqual(f.messages,[]);assert.equal(w.S003.busy,false);
});
for(const mode of ['reopen','identity','normal'])test('location save completion respects '+mode,async t=>{
 const f=await fixture(t),w=f.w;w.openLocationModal();w.E('locCode').value='SYN-ORIGINAL';const d=f.enqueue('v2_003_location_save'),p=w.saveLocation({preventDefault(){}});if(mode==='reopen'){w.closeModal('locationModal');w.openLocationModal();w.E('locCode').value='SYN-NEW';}if(mode==='identity')w.CKSession.user=null;d.resolve({id:'SAVED'});await p;assert.equal(w.E('locationModal').classList.contains('hidden'),mode==='normal');if(mode==='reopen')assert.equal(w.E('locCode').value,'SYN-NEW');if(mode!=='normal')assert.deepEqual(f.messages,[]);
});
test('receipt confirmation result stays attached to its original form after navigating to another shipment',async t=>{
 const f=await fixture(t),w=f.w;const first=f.enqueue('v2_003_receiving_lookup'),p=w.openReceiptByCode('A');first.resolve(shipment('A'));await p;w.document.querySelector('.rc-qty').value='7';const saved=f.enqueue('v2_003_receipt_confirm'),submit=w.submitReceipt({preventDefault(){}});const next=f.enqueue('v2_003_receiving_lookup'),pn=w.openReceiptByCode('B');next.resolve(shipment('B'));await pn;w.document.querySelector('.rc-qty').value='3';saved.resolve({ok:true});await submit;assert.equal(w.S003.currentShipment.shipment.id,'B');assert.equal(w.document.querySelector('.rc-qty').value,'3');assert.equal(f.calls.filter(x=>x.action==='v2_003_receiving_lookup').length,2);
});
test('confirmation cannot replace a newer lookup that is still waiting for its response',async t=>{
 const f=await fixture(t),w=f.w,a=f.enqueue('v2_003_receiving_lookup'),pa=w.openReceiptByCode('A');a.resolve(shipment('A'));await pa;w.document.querySelector('.rc-qty').value='7';const saved=f.enqueue('v2_003_receipt_confirm'),submit=w.submitReceipt({preventDefault(){}});const b=f.enqueue('v2_003_receiving_lookup'),pb=w.openReceiptByCode('B');saved.resolve({ok:true});await submit;assert.equal(f.calls.filter(x=>x.action==='v2_003_receiving_lookup').length,2);b.resolve(shipment('B'));await pb;assert.equal(w.S003.currentShipment.shipment.id,'B');
});
test('refreshing receipt photos retains live quantity, location and notes while successful confirmation clears pending form',async t=>{
 const f=await fixture(t),w=f.w;let a=f.enqueue('v2_003_receiving_lookup'),p=w.openReceiptByCode('A');a.resolve(shipment('A'));await p;w.document.querySelector('.rc-qty').value='7';w.document.querySelector('.rc-note').value='line draft';w.E('receiptDiffNote').value='difference draft';a=f.enqueue('v2_003_receiving_lookup');p=w.openReceiptByCode('A',{refresh:true});w.document.querySelector('.rc-qty').value='8';a.resolve({...shipment('A'),attachments:[{file_key:'SYN-PHOTO',file_name:'Synthetic image'}]});await p;assert.equal(w.document.querySelector('.rc-qty').value,'8');assert.equal(w.document.querySelector('.rc-loc').value,'SYN-A01');assert.equal(w.document.querySelector('.rc-note').value,'line draft');assert.equal(w.E('receiptDiffNote').value,'difference draft');
 a=f.enqueue('v2_003_receiving_lookup');p=w.openReceiptByCode('A',{refresh:true});const complete=shipment('A');complete.shipment.status='received';a.resolve(complete);await p;assert.equal(w.document.querySelector('.rc-qty'),null);
});
test('an old save completion cannot unlock a newer account save',async t=>{
 const f=await fixture(t),w=f.w;w.openMaterialForm();w.E('mfNameZh').value='Synthetic original';const a=f.enqueue('v2_003_material_save'),pa=w.saveMaterial({preventDefault(){}});w.setPage('entryPage');w.CKSession.user={id:'SYN-B',role:'manager'};w.S003.operatorId='SYN-B';w.openMaterialForm();w.E('mfNameZh').value='Synthetic new account';const b=f.enqueue('v2_003_material_save'),pb=w.saveMaterial({preventDefault(){}});a.resolve({id:'OLD'});await pa;assert.equal(w.S003.busy,true);assert.equal(w.E('mfNameZh').value,'Synthetic new account');b.reject(Error('current save failure'));await pb;assert.equal(w.S003.busy,false);assert.equal(f.messages.at(-1)[0],'current save failure');
});
test('old location data cannot replace a new account location list',async t=>{
 const f=await fixture(t),d=f.enqueue('v2_003_location_list'),p=f.w.loadLocations(false);f.w.CKSession.user={id:'SYN-B'};f.w.S003.locations=[{location_code:'NEW-ACCOUNT',active:1}];d.resolve({items:[{location_code:'OLD-ACCOUNT',active:1}]});await p;assert.equal(f.w.S003.locations[0].location_code,'NEW-ACCOUNT');
});
test('scanner callbacks stop after one match and a late camera start is disposed without affecting its replacement',async t=>{
 const f=await fixture(t),w=f.w,instances=[];w.Html5Qrcode=class{constructor(){this.gate=deferred();this.stops=0;this.clears=0;instances.push(this);}start(_a,_b,cb){this.callback=cb;return this.gate.promise;}stop(){this.stops++;return Promise.resolve();}clear(){this.clears++;}};
 const first=w.startReceivingScan();w.closeReceivingScanner();const second=w.startReceivingScan();await f.tick();instances[0].gate.resolve();await first;assert.equal(instances[0].clears,0);assert.equal(w.S003.receivingScanner,instances[1]);instances[1].gate.resolve();await second;const d=f.enqueue('v2_003_receiving_lookup');instances[1].callback('A');instances[1].callback('A');assert.equal(f.calls.filter(x=>x.action==='v2_003_receiving_lookup').length,1);d.resolve(shipment('A'));await f.tick();assert.equal(w.S003.receivingScanner,null);
});
test('a camera starting across a session lock is stopped and cannot query an old identity',async t=>{
 const f=await fixture(t),w=f.w,d=deferred();let callback,stops=0;w.Html5Qrcode=class{start(_a,_b,cb){callback=cb;return d.promise;}stop(){stops++;return Promise.resolve();}clear(){}};const p=w.startReceivingScan();w.CKSession.user=null;d.resolve();await p;callback('A');assert.equal(w.S003.receivingScanner,null);assert.equal(stops,1);assert.equal(f.calls.length,0);
});
