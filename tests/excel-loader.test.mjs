import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../shared/excel-loader.js',import.meta.url),'utf8');
function fixture(){
 const scripts=[],timers=new Map();let id=0;
 const context={document:{currentScript:{getAttribute:()=>'/shared/xlsx.full.min.js?v=36a42f409fe9'},createElement:()=>({remove(){this.removed=true;}}),head:{append:script=>scripts.push(script)}},setTimeout:fn=>{timers.set(++id,fn);return id;},clearTimeout:key=>timers.delete(key)};
 context.window=context;vm.createContext(context);vm.runInContext(source,context);
 const complete=()=>{context.XLSX={read(){},utils:{},writeFile(){}};scripts.at(-1).onload();};
 return {context,scripts,timers,complete,load:context.CKEnsureExcel};
}
test('Excel stays off the initial path; concurrent consumers share one fingerprinted script and warm calls reuse it',async()=>{
 const f=fixture();assert.equal(f.scripts.length,0);const a=f.load(),b=f.load();assert.equal(a,b);assert.equal(f.scripts.length,1);assert.equal(f.scripts[0].src,'/shared/xlsx.full.min.js?v=36a42f409fe9');f.complete();assert.equal(await a,f.context.XLSX);assert.equal(await b,f.context.XLSX);assert.equal(await f.load(),f.context.XLSX);assert.equal(f.scripts.length,1);assert.equal(f.timers.size,0);
});
test('failed Excel download rejects every waiting operation and retries only on a new explicit call',async()=>{
 const f=fixture(),a=f.load(),b=f.load();f.scripts[0].onerror();await assert.rejects(a,/重试/);await assert.rejects(b,/重试/);assert.equal(f.scripts.length,1);assert.equal(f.scripts[0].removed,true);assert.equal(f.timers.size,0);const retry=f.load();assert.equal(f.scripts.length,2);f.complete();await retry;
});
test('a stalled or malformed library does not leave future operations permanently stuck',async()=>{
 for(const failure of ['timeout','malformed']){const f=fixture(),pending=f.load();if(failure==='timeout')[...f.timers.values()][0]();else f.scripts[0].onload();await assert.rejects(pending,/Excel/);assert.equal(f.timers.size,0);const next=f.load();f.complete();await next;}
});
test('an already available library needs no new request',async()=>{
 const f=fixture();f.context.XLSX={read(){},utils:{},writeFile(){}};assert.equal(await f.load(),f.context.XLSX);assert.equal(f.scripts.length,0);
});
