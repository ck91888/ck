import {DatabaseSync} from 'node:sqlite';
import fs from 'node:fs';
import vm from 'node:vm';
export function database(file=':memory:'){
 const db=new DatabaseSync(file);
 const source=fs.readFileSync(new URL('../worker-v2/index.js',import.meta.url),'utf8');
 const migrations=vm.runInNewContext(source.slice(source.indexOf('const MIGRATIONS ='),source.indexOf('// 每次发布迁移变化时'))+'\nMIGRATIONS');
 for(const sql of migrations){try{db.exec(sql);}catch(e){if(!sql.trim().startsWith('ALTER'))throw e;}}
 db.exec(fs.readFileSync(new URL('../worker-v2/migrations/20260917_sop.sql',import.meta.url),'utf8'));
 const prepare=(sql)=>{
  let args=[];const statement={sql,get args(){return args;},bind(...values){args=values;return statement;},
   async first(){return db.prepare(sql).get(...args)||null;},async all(){return {results:db.prepare(sql).all(...args)};},async run(){const r=db.prepare(sql).run(...args);return{success:true,meta:{changes:r.changes}};}};return statement;
 };
 return {raw:db,prepare,async batch(statements){db.exec('BEGIN');try{const out=[];for(const s of statements){const r=db.prepare(s.sql).run(...s.args);out.push({success:true,meta:{changes:r.changes}});}db.exec('COMMIT');return out;}catch(e){db.exec('ROLLBACK');throw e;}}};
}
