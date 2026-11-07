import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import * as badgeIdentity from '../shared/badge-identity.js';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const window={CKBadgeIdentity:badgeIdentity};vm.runInNewContext(fs.readFileSync(new URL('../shared/sop-people.js',import.meta.url),'utf8'),{window});
const selection=window.CKPeopleSelection;
const plain=value=>JSON.parse(JSON.stringify(value));
test('different badge IDs add people; duplicate IDs do not rename or replace a participant',()=>{
 const s=selection();assert.equal(s.add(' A | 甲 ').added,true);assert.equal(s.add('B|乙').added,true);assert.equal(s.add('C|丙').added,true);
 const duplicate=s.add(' A | 另一个名字 ');assert.equal(duplicate.added,false);assert.match(duplicate.message,/已添加.*姓名不一致/);
 assert.deepEqual(plain(s.read()),{workers:[{id:'A',name:'甲'},{id:'B',name:'乙'},{id:'C',name:'丙'}],lead_id:'A'});
 for(const invalid of ['','A','A|',' |乙','A|乙|丙'])assert.throws(()=>s.add(invalid),/工牌格式/);
 assert.equal(s.people.length,3);
});
test('adding and removing assistants preserves the selected lead and returned data is isolated',()=>{
 const s=selection();s.add('A|甲');s.add('B|乙');s.select('B');s.add('C|丙');s.remove('A');assert.equal(s.read().lead_id,'B');
 const read=s.read();read.workers[0].name='changed';assert.equal(s.read().workers[0].name,'乙');
});
test('removing the lead requires explicit reselection, including after a new scan',()=>{
 const s=selection([{id:'A',name:'甲'},{id:'B',name:'乙'}],'B');assert.match(s.remove('B'),/重新选择/);assert.throws(()=>s.read(),/主操作员/);
 s.add('C|丙');assert.throws(()=>s.read(),/主操作员/);s.select('C');assert.equal(s.read().lead_id,'C');
 s.remove('A');s.remove('C');assert.throws(()=>s.read(),/至少登记/);
});
test('reopened saved selections retain all participants and the assigned lead',()=>{
 const original=selection();original.add('A|甲');original.add('B|乙');original.select('B');const saved=original.read();
 const reopened=selection(saved.workers,saved.lead_id);reopened.add('C|丙');assert.equal(reopened.read().lead_id,'B');assert.equal(reopened.read().workers.length,3);
});
test('system daily badge case variants select one canonical identity without renaming the person',()=>{
 const id='DA-20261012-ABCDEF12',s=selection();assert.equal(s.add(id.toLowerCase()+'|Synthetic daily').added,true);
 const duplicate=s.add(id+'|Different name');assert.equal(duplicate.added,false);assert.match(duplicate.message,/姓名不一致/);
 assert.deepEqual(plain(s.read()),{workers:[{id,name:'Synthetic daily'}],lead_id:id});
 s.add('DA-20261012-ABCDEF13|Synthetic daily');s.select('da-20261012-abcdef13');assert.equal(s.read().workers.length,2);assert.equal(s.read().lead_id,'DA-20261012-ABCDEF13');
 s.remove('da-20261012-abcdef12');assert.equal(s.read().workers.length,1);s.remove('da-20261012-abcdef13');assert.throws(()=>s.read(),/至少登记/);
});
test('reopened lowercase daily crew canonicalizes and deduplicates while retaining original first metadata',()=>{
 const id='DA-20261012-ABCDEF12',s=selection([{id:id.toLowerCase(),name:'Original',borrowed:true},{id,name:'Conflicting snapshot'},{id:'DA-20261012-ABCDEF13',name:'Other'}],id.toLowerCase());
 assert.equal(s.people.length,2);assert.equal(s.people[0].name,'Original');assert.equal(s.people[0].borrowed,true);assert.equal(s.read().lead_id,id);
 assert.match(s.remove(id.toLowerCase()),/重新选择/);assert.throws(()=>s.read(),/主操作员/);s.select('da-20261012-abcdef13');assert.equal(s.read().lead_id,'DA-20261012-ABCDEF13');
});
test('custom badge spelling and unrecognized malformed daily inputs are never broadly uppercased',()=>{
 const s=selection(),ids=['DAF-Synthetic-Ab12','DAF-SYNTHETIC-AB12','EMP-Custom-Mixed','EMP-CUSTOM-MIXED','da-20260230-abcdef12','da-20261012-abcdef12\u200b'];
 for(const id of ids)s.add(id+'|Synthetic custom');assert.deepEqual(plain(s.read().workers).map(p=>p.id),ids);
});
test('borrow availability, duplicate selection and confirmation use the same canonical daily worker', {skip:!runtime},async()=>{
 const dom=new runtime.JSDOM('<body><section></section>',{runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,id='DA-20261012-ABCDEF12',calls=[];
 try{
  w.CKBadgeIdentity=badgeIdentity;w.confirm=()=>true;w.CKDocumentLabels={jobSummary:()=> 'Synthetic source',jobType:()=> 'Synthetic work'};
  w.CKSession={request:async(action,body)=>{calls.push({action,body});return {can_borrow:true,worker_name:'Synthetic daily',source:{job_id:'SOURCE',segment_id:'SEGMENT',revision:3}};}};
  w.eval(fs.readFileSync(new URL('../shared/sop-people.js',import.meta.url),'utf8'));
  const root=w.document.querySelector('section');root.innerHTML=w.CKPeopleFields();const picker=w.CKPeoplePicker(root,{allowEmpty:true,borrowContext:{payload:{action:'v2_unload_job_start',plan_id:'SYNTHETIC-PLAN'}}});
  const add=async raw=>{root.querySelector('[data-staff-badge]').value=raw;root.querySelector('[data-staff-add]').click();await picker.ready();};
  await add(id.toLowerCase()+'|Synthetic daily');await add(id+'|Synthetic daily');
  assert.equal(calls.length,1);assert.equal(calls[0].body.worker_id,id);assert.deepEqual(plain(picker.read()),{workers:[{id,name:'Synthetic daily'}],lead_id:id,borrow_confirmations:[{worker_id:id,source_job_id:'SOURCE',source_segment_id:'SEGMENT',source_revision:3}]});
  root.querySelector('[data-staff-people] button').click();assert.deepEqual(plain(picker.read()),{workers:[],lead_id:''});await picker.destroy();
 }finally{w.close();}
});
