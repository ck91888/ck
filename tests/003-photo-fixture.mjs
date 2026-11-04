import fs from 'node:fs';
export const tick=()=>new Promise(r=>setImmediate(r));
export const jpeg=(size=1024,extra=[])=>{const head=[255,216,...extra,255,192,0,17,8,11,208,15,192,3,1,17,0,2,17,0,3,17,0,255,218,0,12,3,1,0,2,17,3,17,0,63,0],bytes=new Uint8Array(Math.max(size,head.length+2));bytes.set(head);bytes.set([255,217],bytes.length-2);return bytes;};
export async function photoFixture(t,kind='asset'){
 const {JSDOM,VirtualConsole}=await import(process.env.CK_JSDOM_MODULE),errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));const read=p=>fs.readFileSync(new URL('../'+p,import.meta.url),'utf8');
 const dom=new JSDOM(read('003/index.html'),{url:'https://synthetic.invalid/003/',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window;t.after(()=>w.close());await new Promise(r=>w.document.readyState==='loading'?w.document.addEventListener('DOMContentLoaded',r,{once:true}):r());
 w.File=File;w.Blob=Blob;w.FormData=FormData;w.AbortController=AbortController;w.HTMLElement.prototype.scrollIntoView=function(){};
 for(const p of ['003/config.js','shared/latest-list-read.js','shared/photo-upload.js','003/app.js'])w.eval(read(p));
 w.CKSession={scope:'office',user:{id:'synthetic-admin',role:'manager',departments:['bulk'],operation_context:{id:'synthetic-operator'}}};Object.assign(w.S003,{role:'admin',key:'synthetic-key',operatorId:'SYN-OP',operatorName:'Synthetic operator',currentView:kind==='asset'?'asset-detail':'receipt-detail'});
 w.S003.currentAsset={item:{id:'A',status:'available'},attachments:[]};w.S003.currentShipment={shipment:{id:'S',shipment_no:'SYN-S',status:'pending'},attachments:[]};
 const host=w.document.querySelector(kind==='asset'?'#assetDetail':'#receiptDetail');host.innerHTML='<div data-photo003-gallery="'+kind+'" data-photo003-id="'+(kind==='asset'?'A':'S')+'"></div><input id="synthetic-unsaved" value="keep me">';
 const input=w.document.querySelector(kind==='asset'?'#assetPhotoInput':'#arrivalPhotoInput'),calls=[],prepared=[],toasts=[],refreshes=[];let files=[],handler,prepareHandler;
 Object.defineProperty(input,'files',{get:()=>files});input.dataset.photoMode='photo';w.toast=(text,error)=>toasts.push({text,error});w.openAssetDetail=id=>refreshes.push(id);w.openReceiptByCode=id=>refreshes.push(id);
 w.CKPhotoUpload.prepare=async(file,options)=>{prepared.push({file,options});return prepareHandler?prepareHandler(file,options):new File([jpeg(128)],'synthetic.jpg',{type:'image/jpeg'});};
 w.fetch=async(url,options)=>{calls.push({url,form:options.body});return handler?handler(url,options):{ok:true,status:200,json:async()=>({ok:true,id:'ATT-SYN-'+calls.length,file_key:'synthetic/'+calls.length})};};
 const select=(file)=>{files=Array.isArray(file)?file:[file];return input;},run=()=>kind==='asset'?w.uploadAssetPhoto(input):w.uploadArrivalPhoto(input),until=async fn=>{const end=Date.now()+5000;while(!fn()){if(Date.now()>end)throw Error('synthetic 003 photo condition timeout');await tick();}};
 return {w,input,calls,prepared,toasts,refreshes,errors,select,run,until,setHandler(fn){handler=fn;},setPrepare(fn){prepareHandler=fn;},file(size=1024){return new File([jpeg(size)],'synthetic.jpeg',{type:'image/jpeg'});}};
}
