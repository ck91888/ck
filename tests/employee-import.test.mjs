import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
import {database} from './d1-adapter.mjs';import {handleAttendance} from '../worker-v2/attendance.js';
function setup(){const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_REQUEST_USER:{id:'M',name:'Manager',role:'manager'}};const call=(a,b={})=>handleAttendance({action:'sop_attendance_'+a,client_req_id:crypto.randomUUID(),...b},env);return {DB,env,call};}
const entry=(employeeNo,extra={})=>({name:'测试职员',employeeNo,department:'bulk',enabled:null,...extra});
const apply=async(call,rows,extra={})=>{const preview=await call('employee_import_preview',{rows});return call('employee_import',{rows,previewToken:preview.previewToken,...extra});};
test('preview writes no personnel; atomic import retries preserve badge IDs and do not punch attendance',async()=>{
 const {call,DB}=setup(),rows=[entry('001'),entry('ABC',{name:'김테스트',department:'office'})];
 const preview=await call('employee_import_preview',{rows});assert.deepEqual(preview.counts,{create:2,update:0,unchanged:0,error:0});assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,0);
 const payload={rows,previewToken:preview.previewToken,client_req_id:crypto.randomUUID()},r=await call('employee_import',payload);assert.deepEqual(await call('employee_import',payload),r);
 const people=(await call('employee_people')).items;assert.equal(people.length,2);assert.equal(people.find(p=>p.employeeNo==='001').badgeId,'EMP-001');assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_days').get().n,0);
 assert.equal((await apply(call,rows)).counts.unchanged,2);assert.equal((await call('employee_people')).items.length,2);
});
test('matching number updates preserve historical punches, disabled state, identity and unlisted people',async()=>{
 const {call}=setup();await apply(call,[entry('A'),entry('B',{enabled:false}),entry('C')]);const a=(await call('employee_search',{term:'A'})).items[0];await call('checkin',{badge:a.badgeId});
 const r=await apply(call,[entry('a',{name:'修订姓名',department:'office'}),entry('B')]);assert.equal(r.counts.update,1);assert.equal(r.counts.unchanged,1);
 const people=(await call('employee_people')).items,updated=people.find(p=>p.employeeNo==='A');assert.equal(updated.id,a.id);assert.equal(updated.badgeId,a.badgeId);assert.equal(updated.version,2);assert.equal(people.find(p=>p.employeeNo==='B').enabled,false);assert.equal(people.length,3);
 const historical=(await call('summary',{scope:'employee'})).items[0].record;assert.equal(historical.name,'测试职员');assert.equal(historical.department,'bulk');
 assert.equal((await call('employee_update',{id:a.id,version:2,name:'再修订',department:'office',enabled:true})).person.version,3);
});
test('invalid rows and duplicate numbers reject the entire batch with original row numbers',async()=>{
 const {call,DB}=setup(),rows=[entry('GOOD'),entry('dup',{row:4}),entry('DUP',{row:6}),entry('',{name:'',department:'wrong'})];const p=await call('employee_import_preview',{rows});assert.equal(p.valid,false);assert.equal(p.counts.error,3);assert.equal(p.items[2].row,6);assert.match(p.items[1].errors.join(),/重复/);
 await assert.rejects(call('employee_import',{rows,previewToken:p.previewToken}),/错误/);assert.equal(DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,0);
 await assert.rejects(call('employee_import_preview',{rows:Array.from({length:201},(_,i)=>entry('E'+i))}),/200/);
});
test('manager-only preview/apply and stale preview do not write partial updates',async()=>{
 const {call,env}=setup();await apply(call,[entry('A')]);const rows=[entry('A',{name:'导入姓名'}),entry('B')],p=await call('employee_import_preview',{rows});
 for(const role of ['viewer','kiosk','dispatcher']){env.SOP_REQUEST_USER.role=role;await assert.rejects(call('employee_import_preview',{rows}),/权限/);await assert.rejects(call('employee_import',{rows,previewToken:p.previewToken}),/权限/);}env.SOP_REQUEST_USER.role='manager';
 const a=(await call('employee_people')).items[0];await call('employee_update',{id:a.id,version:1,name:'最新姓名',department:'bulk',enabled:true});await assert.rejects(call('employee_import',{rows,previewToken:p.previewToken}),/重新预览/);assert.equal((await call('employee_people')).items.length,1);
});
test('clock-in after validation rolls back all imported rows and audit records',async()=>{
 const {call,DB}=setup();await apply(call,[entry('A')]);const a=(await call('employee_people')).items[0],rows=[entry('NEW'),entry('A',{enabled:false})],p=await call('employee_import_preview',{rows});
 const batch=DB.batch.bind(DB);let injected=false;DB.batch=async statements=>{
  if(!injected&&statements[0]?.args[4]==='sop_attendance_employee_import'){injected=true;DB.raw.prepare("INSERT INTO ck_attendance_days VALUES(?,?,?,?,?,?,?,?,?,1)").run('RACEDAY',a.id,a.badgeId,a.name,'bulk','2026-09-22','RACE',new Date().toISOString(),'');}
  return batch(statements);
 };
 await assert.rejects(call('employee_import',{rows,previewToken:p.previewToken}),/变化|冲突/);const people=(await call('employee_people')).items;assert.equal(people.length,1);assert.equal(people[0].enabled,true);assert.equal(people[0].version,1);
 const next=await call('employee_import_preview',{rows});assert.equal(next.valid,false);assert.match(next.items[1].errors.join(),/尚未签退/);
});
test('all 200 rows fit one batch and an injected database error rolls every row back',async()=>{
 const {call,DB}=setup(),rows=Array.from({length:200},(_,i)=>entry('E'+i));const r=await apply(call,rows);assert.equal(r.counts.create,200);assert.equal((await call('employee_people')).items.length,200);
 DB.raw.exec("CREATE TRIGGER fail_import BEFORE INSERT ON ck_employee_profiles WHEN NEW.employee_no='FAIL' BEGIN SELECT RAISE(ABORT,'fixture failure'); END");await assert.rejects(apply(call,[entry('FIRST'),entry('FAIL')]),/冲突/);assert.equal((await call('employee_people')).items.length,200);
});
const ctx={window:{},console};ctx.window=ctx;vm.createContext(ctx);vm.runInContext(fs.readFileSync(new URL('../shared/xlsx.full.min.js',import.meta.url),'utf8'),ctx);vm.runInContext(fs.readFileSync(new URL('../shared/employee-import.js',import.meta.url),'utf8'),ctx);
test('real template preserves leading zero IDs, bilingual departments, CSV export and formula rejection',()=>{
 const labels={bulk:'大货 / 대량',office:'办公室 / 사무실'},bytes=Buffer.from(fs.readFileSync(new URL('../shared/templates/employees.xlsx.b64',import.meta.url),'utf8'),'base64'),X=ctx.XLSX;
 const book=X.read(bytes,{type:'buffer'}),s=book.Sheets[book.SheetNames[0]];assert.throws(()=>ctx.CKEmployeeImport.parse(book,labels),/没有职员/);
 X.utils.sheet_add_aoa(s,[['测试甲','0001','대량',''],['김테스트','K002','办公室 / 사무실','停用']],{origin:'A2'});const parsed=ctx.CKEmployeeImport.parse(book,labels);assert.equal(parsed[0].employeeNo,'0001');assert.equal(parsed[0].department,'bulk');assert.equal(parsed[0].enabled,null);assert.equal(parsed[1].enabled,false);
 const csv=X.read('姓名,工号,部门,状态\n甲,0002,大货 / 대량,在职',{type:'string',raw:true});assert.equal(ctx.CKEmployeeImport.parse(csv,labels)[0].employeeNo,'0002');
 s.B2={t:'s',v:'0001',f:'TEXT(1,"0000")'};assert.throws(()=>ctx.CKEmployeeImport.parse(book,labels),/公式/);
});
