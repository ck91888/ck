import test from 'node:test';
import assert from 'node:assert/strict';
import {canonicalDailyBadge,isSystemDailyBadge,isAttendanceBadge,sameWorkerBadge} from '../shared/badge-identity.js';

test('generated daily identifiers have one ASCII case identity without changing their value',()=>{
 const variants=['DA-20261012-A1B2C3D4','da-20261012-a1b2c3d4','Da-20261012-a1B2c3D4'];
 for(const id of variants){assert.equal(isSystemDailyBadge(id),true);assert.equal(canonicalDailyBadge(id),variants[0]);assert.equal(sameWorkerBadge(id,variants[0]),true);}
 assert.equal(sameWorkerBadge(variants[0],'DA-20261012-A1B2C3D5'),false);
});
test('custom and permanent identifiers retain case-sensitive identity',()=>{
 for(const id of ['DAF-0123abcd','DAF-Custom-Ab','EMP-Synthetic-Ab','DA-261012-AB12','DA-20261012-CUSTOM','CUSTOM-Ab']){
  assert.equal(isSystemDailyBadge(id),false);assert.equal(canonicalDailyBadge(id),id);assert.equal(sameWorkerBadge(id,id.toLowerCase()),false);
 }
});
test('only real calendar dates and exact eight-digit hexadecimal suffixes qualify',()=>{
 assert.equal(isSystemDailyBadge('da-20240229-a1b2c3d4'),true);
 for(const id of ['DA-20260229-A1B2C3D4','DA-20261312-A1B2C3D4','DA-20260012-A1B2C3D4','DA-20261000-A1B2C3D4','DA-20261012-A1B2C3DG','DA-20261012-A1B2C3D','DA-20261012-A1B2C3D45'])assert.equal(isSystemDailyBadge(id),false,id);
});
test('identity normalization cannot silently remove hidden or non-ASCII characters',()=>{
 for(const id of ['da-20261012-a1b2c3d4\u200b','da-20261012-a1b2c3d4 ',' da-20261012-a1b2c3d4','ＤＡ-20261012-A1B2C3D4','da-20261012-a1b2c3d4|Synthetic']){
  assert.equal(isSystemDailyBadge(id),false);assert.equal(canonicalDailyBadge(id),id);
 }
});
test('managed-prefix admission is broader than the narrow daily identity equivalence',()=>{
 for(const id of ['da-20261012-a1b2c3d4','Da-CUSTOM','daf-Custom-Ab','emp-Synthetic','DAF-Custom-Ab','EMP-Synthetic'])assert.equal(isAttendanceBadge(id),true);
 for(const id of ['SYNTHETIC-WORKER','DA','DEPARTMENT-X','XDA-20261012-A1B2C3D4'])assert.equal(isAttendanceBadge(id),false);
});
