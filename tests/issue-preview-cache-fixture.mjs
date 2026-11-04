// Deterministic preview component only. Image decoding is mocked; counters are estimates, never heap/OOM measurements.
import fs from 'node:fs';
export async function cacheFixture(t,{width=1,height=1,bytes=32,declared=bytes,auto=false,sourceRoot}={}){
 const {JSDOM,VirtualConsole}=await import(process.env.CK_JSDOM_MODULE),errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));
 const dom=new JSDOM('<!doctype html><body><main id="photos"></main><div id="lightbox"></div><img id="pending-upload"></body>',{url:'https://preview.invalid/002/',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc}),w=dom.window;t.after(()=>w.close());await new Promise(r=>w.document.readyState==='loading'?w.document.addEventListener('DOMContentLoaded',r,{once:true}):r());
 w._currentView='issue-detail';w.Blob=Blob;w.AbortController=AbortController;w.requestAnimationFrame=fn=>{queueMicrotask(()=>fn(0));return 1;};
 w.HTMLElement.prototype.getClientRects=function(){for(let n=this;n&&n.nodeType===1;n=n.parentElement)if(n.hidden||n.style.display==='none')return [];return [{}];};
 const urls=new Map(),revoked=[],calls=[],observed=[],foreign='blob:pending-upload-source',observers=[];let sequence=0,handler,decodeError=false;
 w.URL.createObjectURL=blob=>{const u='blob:owned-preview-'+(++sequence);urls.set(u,{bytes:blob.size,width,height});return u;};w.URL.revokeObjectURL=url=>{revoked.push(url);urls.delete(url);};w.Image=class{set src(v){this._src=v;queueMicrotask(()=>{this.naturalWidth=width;this.naturalHeight=height;decodeError?this.onerror?.():this.onload?.();});}get src(){return this._src;}};
 const encoded=new Uint8Array(Math.max(bytes,16));encoded.set([137,80,78,71]);w.fetch=async(url,options)=>{calls.push({url,options});if(handler)return handler(url,options);return new Response(encoded,{headers:{'Content-Type':'image/png'}});};
 w.IntersectionObserver=class{constructor(cb){this.cb=cb;observers.push(this);}observe(target){observed.push(target);if(auto)queueMicrotask(()=>this.cb([{target,isIntersecting:true}]));}unobserve(){}disconnect(){}};
 const source=sourceRoot?fs.readFileSync(sourceRoot+'/shared/issue-images.js','utf8'):fs.readFileSync(new URL('../shared/issue-images.js',import.meta.url),'utf8');w.eval(source);w.document.dispatchEvent(new w.Event('DOMContentLoaded'));
 const tick=()=>new Promise(r=>setImmediate(r)),until=async fn=>{const end=Date.now()+5000;while(!fn()){if(Date.now()>end)throw Error('synthetic preview state timeout');await tick();}};
 const add=async(n=1)=>{const start=w.document.querySelectorAll('[data-issue-image]').length;w.document.querySelector('#photos').insertAdjacentHTML('beforeend',Array.from({length:n},(_,i)=>w.CKIssueImages.html({file_name:'Synthetic '+(start+i)+'.png',file_size:declared},'/file?id='+String(start+i))).join(''));await tick();};
 const open=async i=>{w.CKIssueImages.open('/file?id='+i,'Synthetic '+i+'.png',declared);await until(()=>w.document.querySelector('[data-view-status]')?.textContent.includes('已加载'));};
 const close=async()=>{w.CKIssueImages.close();await tick();};
 const estimate=()=>[...urls.values()].reduce((n,x)=>n+x.bytes+x.width*x.height*4,0);
 return {w,urls,revoked,calls,observed,foreign,errors,add,open,close,tick,until,estimate,intersect(){for(const observer of observers)observer.cb([...w.document.querySelectorAll('[data-issue-image]')].map(target=>({target,isIntersecting:true})));},setHandler(fn){handler=fn;},setDecodeError(v){decodeError=v;}};
}
