import assert from 'node:assert/strict';import test from 'node:test';
import {agencyFixture} from './attendance-agencies-fixture.mjs';import '../shared/attendance-agencies.js';import {financeWorkbookData} from '../shared/labor-export-data.js';
const label=globalThis.CKAttendanceAgencies.label;
test('company display maps exact legacy tokens and preserves other text, separators and source values',()=>{
 for(const [input,output]of [['CKIK','한국인력개발'],['리드인','리드인'],['CKIK／其他','한국인력개발／其他'],[' CKIK / 리드인 ／CKIK ',' 한국인력개발 / 리드인 ／한국인력개발 '],['CKIK,其他；CKIK','한국인력개발,其他；한국인력개발'],['CKIKX','CKIKX'],['prefix CKIK','prefix CKIK'],['<img src=x onerror=1>','<img src=x onerror=1>'],[null,'']])assert.equal(label(input),output);
});
test('new agency accepts temporary checkin and permanent registration/update without changing historical attendance',async t=>{
 const f=await agencyFixture(t),config=await f.ok('sop_attendance_config');assert.ok(config.agencies.includes('리드인'));assert.equal(config.agencies.filter(x=>x==='CKIK').length,1);assert.ok(!config.agencies.includes('한국인력개발'));
 const daily=(await f.ok('sop_attendance_checkin',{name:'合成리드인临时',agency:'리드인'})).record;assert.equal(daily.agency,'리드인');
 const p=(await f.ok('sop_attendance_register',{name:'合成旧公司长期',agency:'CKIK'})).person,day=(await f.ok('sop_attendance_checkin',{badge:p.badgeId,agency:p.agency})).record;
 const before=await f.rows('SELECT * FROM ck_attendance_days WHERE id=?',day.id),updated=await f.ok('sop_attendance_permanent_update',{id:p.id,version:p.version,name:p.name,agency:'리드인',enabled:true,reason:'合成默认公司调整'});assert.equal(updated.person.agency,'리드인');assert.equal(updated.person.badgeId,p.badgeId);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_days WHERE id=?',day.id),before);assert.equal((await f.ok('sop_attendance_lookup',{badge:p.badgeId})).record.agency,'CKIK');
});
test('old CKIK exact search/filter, reprint and dispatch list retain canonical identity and audit values',async t=>{
 const f=await agencyFixture(t),p=(await f.ok('sop_attendance_register',{name:'合成旧公司筛选',agency:'CKIK'})).person,day=(await f.ok('sop_attendance_checkin',{badge:p.badgeId,agency:'CKIK'})).record;
 const found=await f.ok('sop_attendance_search',{name:p.name,agency:'CKIK'});assert.equal(found.items.length,1);assert.equal(found.items[0].record.id,day.id);assert.equal((await f.ok('sop_attendance_search',{name:p.name,agency:'리드인'})).items.length,0);
 const printed=await f.ok('sop_attendance_print',{id:day.id});assert.equal(printed.record.agency,'CKIK');assert.equal(printed.record.badgeId,p.badgeId);assert.equal(printed.record.inAt,day.inAt);
 const permissions=await f.ok('sop_daily_dispatch_list');assert.equal(permissions.items.find(x=>x.id===p.id).agency,'CKIK');
 const result=await f.ok('sop_attendance_daily_export',{start_date:day.day,end_date:day.day,agency:'CKIK'});assert.equal(result.total,1);assert.equal(result.filters.agency,'CKIK');assert.equal(result.items[0].agency,'CKIK');assert.equal((await f.ok('sop_attendance_daily_export',{start_date:day.day,end_date:day.day,agency:'리드인'})).total,0);
 const history=JSON.stringify(await f.rows('SELECT * FROM ck_attendance_events'));assert.ok(history.includes('CKIK'));assert.ok(!history.includes('한국인력개발'));assert.equal((await f.rows('SELECT count(*) n FROM ck_attendance_days'))[0].n,1);
});
test('new selection works for explicit company correction while stale versions remain rejected',async t=>{
 const f=await agencyFixture(t),r=(await f.ok('sop_attendance_checkin',{name:'合成公司更正',agency:'CKIK'})).record,changed=await f.ok('sop_attendance_company',{id:r.id,version:r.version,agency:'리드인'});assert.equal(changed.record.agency,'리드인');assert.equal(changed.record.inAt,r.inAt);assert.equal(changed.record.badgeId,r.badgeId);
 assert.equal((await f.call('sop_attendance_company',{id:r.id,version:r.version,agency:'CKIK'})).ok,false);assert.equal((await f.call('sop_attendance_register',{name:'合成别名拒绝',agency:'한국인력개발'})).ok,false);assert.equal((await f.call('sop_attendance_checkin',{name:'合成未知公司',agency:'未知'})).ok,false);
});
test('all finance sheets display mapped companies including merged values while inputs stay byte-for-byte unchanged',()=>{
 const row={date:'2026-11-04',name:'合成跨公司人员',badge:'DAF-SYNTHETIC',agency:'CKIK／리드인',inAt:'2026-11-04T09:00:00+09:00',outAt:'',checkoutState:'未下班',minutes:{bulk:60},flags:[],intervals:[{agency:'CKIK',start:'2026-11-04T09:00:00+09:00',end:'',minutes:{bulk:60}}]},meta={start:row.date,end:row.date,agency:'CKIK',asOf:'2026-11-04T01:00:00Z'},original=JSON.stringify({row,meta}),book=financeWorkbookData([row],meta);
 assert.equal(book.daily[1][3],'한국인력개발／리드인');assert.equal(book.spans[1][3],'한국인력개발');assert.match(book.notes.find(x=>x[0]==='生效筛选')[1],/한국인력개발/);assert.equal(book.daily[1][11],1);assert.equal(JSON.stringify({row,meta}),original);
});
