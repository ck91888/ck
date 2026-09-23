import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
const window={};vm.runInNewContext(fs.readFileSync(new URL('../shared/sop-people.js',import.meta.url),'utf8'),{window});
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
