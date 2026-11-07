import test from 'node:test';
import assert from 'node:assert/strict';
import {expandMarks,normalizeCargoGroups} from '../worker-v2/cargo-groups.js';
import {cargoFixture} from './cargo-fixture.mjs';
import {cargoReservations} from '../worker-v2/cargo-reservations.js';
import {ensureBatchMaterialState} from '../worker-v2/batch-material-state.js';

test('natural numeric widths and internal mark dashes expand complete and shortened endpoints',()=>{
 for(const [source,prefix] of [['LU-1-LU-55','LU-'],['LU1-LU55','LU'],['LU-1-55','LU-'],['LU1-55','LU'],['LU-1 ~ LU-55','LU-'],['LU-1–LU-55','LU-']])assert.deepEqual(expandMarks(source),Array.from({length:55},(_,i)=>prefix+(i+1)),source);
 assert.deepEqual(expandMarks('LU-9-LU-11,13,15-16'),['LU-9','LU-10','LU-11','LU-13','LU-15','LU-16']);
 assert.deepEqual(expandMarks('BOX-2026-A-99-BOX-2026-A-101'),['BOX-2026-A-99','BOX-2026-A-100','BOX-2026-A-101']);
 assert.deepEqual(expandMarks('LU-1'),['LU-1']);
});
test('leading zeroes remain exact and mixed padding is rejected with a corrective hint',()=>{
 assert.deepEqual(expandMarks('LU-009-LU-011'),['LU-009','LU-010','LU-011']);
 assert.deepEqual(expandMarks('LU099-LU101'),['LU099','LU100','LU101']);
 assert.deepEqual(expandMarks('LU-01,LU-1'),['LU-01','LU-1']);
 for(const source of ['LU-01-LU-3','LU1-LU055','CP001-03'])assert.throws(()=>expandMarks(source),/前导零.*位数须一致/,source);
});
test('invalid, duplicate and oversized ranges retain rejection; ambiguous syntax explains explicit endpoints',()=>{
 for(const source of ['LU-3-LU-1','LU-1-DP-3','LU1-LU60000','LU9007199254740992-LU9007199254740993','LU-1-LU-55,LU-10'])assert.throws(()=>expandMarks(source),undefined,source);
 assert.throws(()=>expandMarks('LU-1~LU-3~LU-5'),/歧义.*完整起止箱唛/);
 assert.throws(()=>expandMarks('LU-1-DP-3'),/前缀须一致.*~/);
});
test('group normalization retains original range text and enforces exact one-mark box count',()=>{
 const source='LU-1-LU-55',group={range:source,box_count:55,one_mark_one_box:true},out=normalizeCargoGroups({total_range:source,groups:[group]});
 assert.equal(out.groups[0].range,source);assert.equal(out.groups[0].marks.length,55);assert.equal(out.total_range,source);assert.deepEqual(out.coverage.unassigned,[]);
 assert.throws(()=>normalizeCargoGroups({groups:[{...group,box_count:54}]}),/箱数须与范围数量一致/);
});
test('real outbound reservations use the same expansion and still reject overlap, outside marks and invalid or excessive quantities',async t=>{
 const s=cargoFixture();t.after(()=>s.DB.raw.close());s.user={...s.user,scope:'office',role:'manager'};s.env.SOP_REQUEST_USER=s.user;await ensureBatchMaterialState(s.env);
 const state=s.state(),g=state.cargo_groups.groups[0];Object.assign(g,{range:'LU-1-LU-55',marks:expandMarks('LU-1-LU-55'),box_count:55,one_mark_one_box:true});s.DB.raw.prepare("UPDATE sop_records SET state=? WHERE id='N'").run(JSON.stringify(state));
 await s.call('sop_cargo_group_complete',{group_id:g.id,quantity:55,unit:'箱'});await s.call('sop_cargo_group_review',{group_ids:[g.id],decision:'pass',reason:'合成范围审核'});
 const reserve=(shipping_range,quantity)=>cargoReservations(s.body('sop_cargo_reservation_create',{group_id:g.id,reservation:{expected_ship_at:'2026-10-16',outbound_mode:'milk_express',destination:'合成范围验收',shipping_range,quantity,unit:'箱'}}),s.env,s.user);
 await reserve('LU-1-LU-9',9);assert.equal(s.state().links[0].cargo_reservation.shipping_range,'LU-1-LU-9');
 const before=JSON.stringify(s.state()),orders=s.DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n;
 for(const args of [['LU-9-LU-11',3],['LU-56-LU-57',2],['LU-10-LU-55',0],['LU-10-LU-55',47]]){await assert.rejects(()=>reserve(...args));assert.equal(JSON.stringify(s.state()),before);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n,orders);}
 await reserve('LU-10-LU-55',46);assert.equal(s.state().links.length,2);assert.equal(s.state().links[1].cargo_reservation.shipping_marks.length,46);
});
