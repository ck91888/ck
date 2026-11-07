import assert from 'node:assert/strict';import {ownedHomeFixture} from './field-owned-home-fixture.mjs';import {localD1} from './d1-local-runtime.mjs';
export const LOCATION_CODES=['FE01-A01',"FE01-O'Brien",'FE01-"箱"','FE01-\\库位\\한글','FE01-<货>&"\'-🙂','FE01-&quot;&#39;&amp;','FE01-A\u2028B\u2029C','FE01" onpointerenter="window.__fe01Marker=1;//'];
export async function locationFixture(t,{native=process.env.CK_P1_NATIVE_D1==='true'}={}){
 const day=new Date(Date.now()+32400000).toISOString().slice(0,10);t.mock.timers.enable({apis:['Date'],now:Date.parse(day+'T09:00:00+09:00')});const f=await ownedHomeFixture();let d1;t.after(async()=>{await d1?.close();f.DB.raw.close();});if(native){d1=await localD1(f.DB.raw);f.env.DB=d1;}
 const q=(sql,...args)=>f.env.DB.prepare(sql).bind(...args),rows=async(sql,...args)=>(await q(sql,...args).all()).results;
 const request=async(scope,action,body={},changes={})=>{const path=scope==='A'?'/001/api':scope==='kiosk'?'/attendance/api':'/api',response=await f.worker.fetch(new Request('https://synthetic.invalid'+path,{method:'POST',headers:{'Content-Type':'application/json',Origin:'https://synthetic.invalid',Cookie:scope==='anonymous'?'':f.cookies[scope]||''},body:JSON.stringify({action,...body})}),{...f.env,...changes});return{response,data:await response.json()};};
 const save=async body=>{const r=await request('office','v2_003_location_save',{operator_id:'SYN-FE01',operator_name:'Synthetic location operator',...body});assert.equal(r.data.ok,true,JSON.stringify(r.data));return r.data;};
 const locations=[];for(const [i,code]of LOCATION_CODES.entries()){const saved=await save({location_code:code,location_name:'Synthetic "库位" <b> & 한국 '+i,active:1});locations.push({id:saved.id,code});}const inactive=await save({location_code:'FE01-INACTIVE',location_name:'Synthetic inactive',active:0});
 return {...f,q,rows,request,save,locations,inactive};
}
