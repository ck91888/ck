import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../shuju/app.js',import.meta.url),'utf8');
const context={window:{CKDocumentLabels:{jobLabel:j=>j.label,jobDispatcher:j=>j.dispatcher}}};
vm.createContext(context);
vm.runInContext(source.slice(source.indexOf('function esc('),source.indexOf('function getKey('))+source.slice(source.indexOf('function ordersResultMarkup('),source.indexOf('async function loadOrders(')),context);
const metric=(label,quantity,extra={})=>({label,quantity,unit:'箱',phase:'finish',...extra});
test('result presentation prioritizes nonzero metrics, preserves zeroes and complete explanations without changing source rows',()=>{
 const row={quantity_metrics:[metric('零值A',0),metric('有效A',19),metric('零值B','0'),metric('有效B',2),metric('有效C',3),metric('有效D',4)],result_summary:'完整说明；单位未记录；合成备注'.repeat(100)};
 const before=JSON.stringify(row),html=context.ordersResultMarkup(row),summary=html.split('</summary>')[0];
 assert.match(summary,/有效A/);assert.match(summary,/有效B/);assert.match(summary,/有效C/);assert.doesNotMatch(summary,/有效D|零值A|零值B/);assert.match(summary,/6项 · 含2项零值/);
 assert.ok(html.includes(row.result_summary));assert.match(html,/零值A/);assert.match(html,/有效D/);assert.match(html,/收起结果/);assert.equal(JSON.stringify(row),before);
});
test('all recorded zeroes, missing values and text-only history remain distinct',()=>{
 const zeros=context.ordersResultMarkup({quantity_metrics:[metric('有记录',0),metric('有记录二','0'),metric('缺失',null),metric('空白','')]}),summary=zeros.split('</summary>')[0];
 assert.match(summary,/有记录/);assert.match(summary,/>0 /);assert.match(summary,/2项零值/);assert.doesNotMatch(zeros,/未记录结果|缺失|空白/);
 const missing=context.ordersResultMarkup({quantity_metrics:[]});assert.match(missing,/未记录结果（不等于零产出）/);assert.doesNotMatch(missing,/<details/);
 const text='仅记录工时，无数量结果；合成说明'.repeat(200),fallback=context.ordersResultMarkup({result_summary:text});assert.ok(fallback.includes(text));assert.match(fallback,/展开完整结果/);
});
test('start snapshots and units retain their labels; untrusted result and reference text is escaped',()=>{
 const html=context.ordersResultMarkup({quantity_metrics:[metric('<img src=x>',17,{phase:'start',unit:'托'})],result_summary:'<script>synthetic</script>'});
 assert.match(html,/开始快照/);assert.match(html,/17 <small>托/);assert.doesNotMatch(html,/<img|<script>/);assert.match(html,/&lt;script&gt;/);
 const full='合成中文한글'.repeat(50),ref=context.ordersReferenceMarkup({label:full,dispatcher:'<img src=x>'});assert.ok(ref.includes(full));assert.match(ref,/展开单号 \/ 派审员/);assert.doesNotMatch(ref,/<img/);
});
