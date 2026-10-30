import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {customerInitials,outboundDisplayBase,nextOutboundDisplayNo} from '../worker-v2/outbound-number.js';

test('public outbound number uses customer pinyin initials and planned ship date',()=>{
 assert.equal(customerInitials('宝袋'),'BD');
 assert.equal(customerInitials('东亚'),'DY');
 assert.equal(customerInitials('刘佳奇'),'LJQ');
 assert.equal(customerInitials('重庆'),'CQ');
 assert.equal(customerInitials('Acme Inc'),'AI');
 assert.equal(outboundDisplayBase('宝袋','2026-10-03'),'CHU-BD-20261003');
 assert.throws(()=>outboundDisplayBase('宝袋','2026-02-31'),/日期无效/);
});

test('same customer and ship date get distinct sequential numbers, while other dates stay independent',async()=>{
 const DB=database(),env={DB};
 const numbers=await Promise.all(Array.from({length:10},()=>nextOutboundDisplayNo(env,'宝袋','2026-10-03')));
 assert.equal(new Set(numbers).size,10);
 assert.ok(numbers.includes('CHU-BD-20261003'));
 assert.ok(numbers.includes('CHU-BD-20261003-10'));
 assert.equal(await nextOutboundDisplayNo(env,'东亚','2026-10-03'),'CHU-DY-20261003');
 assert.equal(await nextOutboundDisplayNo(env,'宝袋','2026-10-04'),'CHU-BD-20261004');
});

test('new number allocation skips existing public numbers when the sequence table is first introduced',async()=>{
 const DB=database(),env={DB};
 DB.raw.prepare('INSERT INTO v2_outbound_orders(id,display_no) VALUES(?,?),(?,?)').run('OB-legacy-one','CHU-BD-20261003','OB-legacy-two','CHU-BD-20261003-02');
 assert.equal(await nextOutboundDisplayNo(env,'宝袋','2026-10-03'),'CHU-BD-20261003-03');
});
