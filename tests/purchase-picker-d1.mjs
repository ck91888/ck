// Real API code + native local workerd D1 SQL. No account, remote database or real catalog.
import assert from 'node:assert/strict';import fs from 'node:fs';import {createHash} from 'node:crypto';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {localD1} from './d1-local-runtime.mjs';
const source=database(),insert=source.raw.prepare('INSERT INTO v2_003_materials(id,material_code,name_zh,name_ko,category,spec,barcode,unit,current_qty,min_qty,status,note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)');
for(let i=0;i<10000;i++){const n=String(i).padStart(5,'0');insert.run('SYN-'+n,'MC-'+n,'合成 '+n,'합성 '+n,i%2?'类别独有':'category','100%_ ÉCOLE ΣΟΣ İSTANBUL','BAR-'+n,'个',i,0,i===9998?'inactive':'active','Synthetic note '.repeat(15));}
const DB=await localD1(source.raw),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'},results=[];
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const call=async(data={})=>{const r=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'v2_003_material_list',...data})}),env),text=await r.text(),body=JSON.parse(text);assert.equal(body.ok,true,body.error);return {body,bytes:Buffer.byteLength(text)};};
try{
 await call({limit:1});const before=hash((await DB.prepare('SELECT * FROM v2_003_materials ORDER BY id').all()).results),old=source.raw.prepare("SELECT *,CASE WHEN status='active' AND min_qty>0 AND current_qty<=min_qty THEN 1 ELSE 0 END AS low_stock FROM v2_003_materials WHERE status='active' ORDER BY low_stock DESC,category,name_zh,material_code").all();
 for(const input of [{search:''},{search:'类别独有'},{search:'100%_'},{search:'école'},{search:'σος'},{search:'istanbul',search_locale:'tr'},{search:'MC-09999'},{search:'not present'},{search:'_',offset:9950}]){
  DB.metrics.length=0;DB.metrics.wall_ms=0;const start=performance.now(),r=await call({purchase_picker:1,limit:50,...input}),apiMs=performance.now()-start,q=input.search.toLocaleLowerCase(input.search_locale),expected=old.filter(x=>['material_code','barcode','name_zh','name_ko','spec','category'].some(k=>String(x[k]||'').toLocaleLowerCase(input.search_locale).includes(q)));
  assert.equal(r.body.total,expected.length);assert.deepEqual(r.body.items.map(x=>x.id),expected.slice(input.offset||0,(input.offset||0)+50).map(x=>x.id));const metrics=[...DB.metrics];assert.equal(metrics.reduce((n,x)=>n+(x.rows_written||0),0),0);
  results.push({input,total:r.body.total,returned:r.body.items.length,apiMs,bytes:r.bytes,rowsRead:metrics.reduce((n,x)=>n+(x.rows_read||0),0),rowsWritten:0,projectedQueryRows:metrics.filter(x=>x.sql.startsWith('SELECT id,material_code,barcode')).map(x=>x.rows)});
 }
 assert.equal(hash((await DB.prepare('SELECT * FROM v2_003_materials ORDER BY id').all()).results),before);const out=process.env.CK_FE08_D1_OUT||'/tmp/ck-purchase-picker-d1.json';fs.writeFileSync(out,JSON.stringify({ok:true,synthetic:true,nativeD1:true,workerCodeRunsInNode:true,results},null,2));console.log(JSON.stringify({ok:true,cases:results.length,results}));
}finally{await DB.close();source.raw.close();}
