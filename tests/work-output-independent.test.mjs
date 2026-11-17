import test from 'node:test';import assert from 'node:assert/strict';
import {cargoFixture} from './cargo-fixture.mjs';import {cargoReservations} from '../worker-v2/cargo-reservations.js';import {cargoAvailability,cargoReady} from '../worker-v2/cargo-allocation.js';
test('shared source carton produces two independently reviewed results and each result retains its own outbound limit',async()=>{
 const s=cargoFixture(),data=s.state();data.planned_quantity=1;for(const [i,g] of data.cargo_groups.groups.entries()){g.range='CP001';g.marks=['CP001'];g.box_count=i?8:12;g.one_mark_one_box=false;}s.DB.raw.prepare("UPDATE sop_records SET state=? WHERE id='N'").run(JSON.stringify(data));
 try{
  for(const [group_id,quantity] of [['G0',20],['G1',7]]){await s.call('sop_cargo_group_complete',{group_id,quantity,unit:'箱'});await s.call('sop_cargo_group_review',{group_ids:[group_id],decision:'pass',reason:'合成实际产出核对'});}
  const office={...s.user,role:'manager',scope:'office'},reserve=(group_id,quantity)=>cargoReservations(s.body('sop_cargo_reservation_create',{group_id,reservation:{schema:2,quantity,unit:'箱',shipping_range:'CP001',expected_ship_at:'2026-10-20',outbound_mode:'milk_express'}}),s.env,office);
  await reserve('G0',20);await reserve('G1',7);const state=s.state(),ids=state.links.map(l=>l.outbound_id);assert.equal(state.planned_quantity,1);assert.equal(new Set(state.links.flatMap(l=>l.cargo_allocations.map(a=>a.output_id))).size,2);assert.ok(ids.every(id=>cargoReady(state,id)));
  assert.deepEqual(ids.map(id=>s.DB.raw.prepare('SELECT planned_box_count FROM v2_outbound_orders WHERE id=?').get(id).planned_box_count),[20,7]);const before=structuredClone(state);await assert.rejects(()=>reserve('G0',1),/超过|重复占用/);assert.deepEqual(s.state(),before);assert.deepEqual((await cargoAvailability(s.env,state)).groups.slice(0,2).map(g=>g.remaining),[0,0]);
 }finally{s.DB.raw.close();}
});
