import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';

// Copy to <checkout>/tests, or run here with CK_REVIEW_REPO=<isolated checkout>.
// Actual 002 functions + actual staging wrapper/prepareOutbound + real Worker.
// Only DOM input values are simulated. SQLite is process memory; no network.
const repo = path.resolve(process.env.CK_REVIEW_REPO || fileURLToPath(new URL('../', import.meta.url)));
const {default: worker} = await import(pathToFileURL(path.join(repo, 'worker-v2/index.js')));
const {database} = await import(pathToFileURL(path.join(repo, 'tests/d1-adapter.mjs')));
const app = fs.readFileSync(path.join(repo, '002/app.js'), 'utf8');
const chain = fs.readFileSync(path.join(repo, 'shared/work-chain-ui.js'), 'utf8');
const entry = fs.readFileSync(path.join(repo, 'shared/sop-entry.js'), 'utf8');
const between = (source, start, end) => {
  const from = source.indexOf(start), to = source.indexOf(end, from + start.length);
  assert.ok(from >= 0 && to > from, 'Source fixture boundary missing: ' + start);
  return source.slice(from, to);
};

function fixture(t) {
  const DB = database(); t.after(() => DB.raw.close());
  const env = {DB, SOP_ENVIRONMENT:'staging', SOP_UPGRADE_ENABLED:'true', SOP_PUBLIC_TEST_ACCESS:'true', SOP_WORK_CHAIN_ENABLED:'true'};
  const raw = body => worker.fetch(new Request('https://fixture.invalid/api', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)}), env);
  const ok = async (action, body={}) => {
    const result = await (await raw({action, client_req_id:crypto.randomUUID(), ...body})).json();
    assert.equal(result.ok, true, action + ': ' + (result.error || result.message));
    return result;
  };
  return {DB, raw, ok};
}

function frontend(f) {
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, {id, value:'', checked:false, disabled:false, textContent:'Create', style:{}, innerHTML:'', children:[], querySelectorAll:()=>[]});
    return nodes.get(id);
  };
  const alerts=[], sent=[], replies=[], actions=[];
  let loseNext=false, blockNext=null;
  const c={console, Date, Math, crypto, JSON, Promise, TypeError,
    V2_API:'https://fixture.invalid/api', getKey:()=>'', getUser:()=> 'QA synthetic service', kstToday:()=> '2026-10-01', L:s=>s, unitTypeLabel:s=>s,
    _ibCreateMaterials:[], _obCreateMaterials:[], _obLineCount:0, detailNeeds:{},
    getIbcBizClasses:()=>['bulk'], getIbcLines:()=>[{unit_type:'carton',planned_qty:Number(node('qa-ibc-qty').value)}], getIbcLinkObRows:()=>[],
    _renderIbcMaterialsList(){}, _renderOcMaterialsList(){}, ensureFirstIbcLine(){}, goTab(){}, alert:s=>alerts.push(s),
    document:{getElementById:node, querySelectorAll:()=>[]}};
  c.window=c; vm.createContext(c);
  vm.runInContext(between(app, 'var _actionLocks =', '// ===== State'), c);
  vm.runInContext(between(app, 'var _WRITE_ACTIONS =', 'async function uploadFile'), c);
  vm.runInContext(between(app, 'function clearOcMaterials()', 'async function submitOutbound'), c);
  vm.runInContext(between(app, 'function clearIbcMaterials()', '// ===== Outbound Detail'), c);
  vm.runInContext(between(app, 'async function submitOutbound(', '// 出库资料文件上传 helper'), c);
  vm.runInContext(between(app, 'async function submitInbound(', '// 确保入库创建表单'), c);
  // Load the actual staging quantity/source projection instead of copying it.
  vm.runInContext('var selected = null; function enabled(){return true;}\n'+between(chain, 'function prepareOutbound(body)', 'function hideFileControls'), c);
  c.CKWorkChain={enabled:()=>true, prepareOutbound:c.prepareOutbound};
  c.nativeApi=c.api;
  vm.runInContext(between(entry, 'window.api=async function(body)', 'async function setup()'), c);
  // Capture completion while preserving the production action lock and button state.
  const originalLock=c.withActionLock;
  c.withActionLock=(key, button, label, fn)=>originalLock(key, button, label, ()=>{const p=fn(); actions.push(p); return p;});
  c.fetch=async (_url, options)=>{
    const body=JSON.parse(options.body); sent.push(body);
    if(blockNext){const gate=blockNext; blockNext=null; await gate;}
    const response=await f.raw(body); replies.push(await response.clone().json());
    if(loseNext){loseNext=false; throw new TypeError('QA response lost after commit');}
    return response;
  };
  function fillInbound(qty=50){
    for(const [id,value] of Object.entries({'ibc-customer':'QA inbound intent', 'ibc-date':'2026-10-01', 'ibc-cargo':'QA cargo', 'ibc-arrival':'2026-10-03', 'ibc-purpose':'QA', 'ibc-remark':'isolated memory only', 'qa-ibc-qty':String(qty)}))node(id).value=value;
  }
  function fillOutbound(need, qty=60){
    c.selected={...need, schedule_unit:'箱', remaining:need.remaining ?? need.planned_quantity};
    for(const [id,value] of Object.entries({'oc-customer':need.customer,'oc-biz-class':'bulk','oc-outmode':'customer_pickup','oc-expected-ship-at':'2026-10-03','oc-planned-box':String(qty),'oc-planned-pallet':'0','oc-uses-stock-op':'0','oc-instruction':need.instructions,'ck-link-quantity':String(qty)}))node(id).value=value;
    node('view-outbound_create').style.display='block';
  }
  async function submit(kind){await c[kind==='inbound'?'submitInbound':'submitOutbound'](node('qa-submit')); await Promise.all(actions);}
  function block(){let resolve; blockNext=new Promise(r=>{resolve=r;}); return resolve;}
  return {c,node,alerts,sent,replies,fillInbound,fillOutbound,submit,block,lose(){loseNext=true;}};
}

async function need(f) {
  const created=await f.ok('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'QA-OFFICE-INTENT',customer:'QA outbound intent',title:'QA synthetic work',instructions:'QA instruction',planned_quantity:100,planned_unit:'箱'});
  return {...(await f.ok('sop_get',{id:created.id})).record,remaining:100};
}
const rows = (f,kind) => f.DB.raw.prepare(kind==='inbound'?'SELECT id,display_no FROM v2_inbound_plans':'SELECT id,display_no FROM v2_outbound_orders').all();

for(const kind of ['inbound','outbound']){
  test('002 '+kind+' retries lost committed reply with same intent and displays the original number', async t=>{
    const f=fixture(t), p=frontend(f);
    if(kind==='inbound')p.fillInbound(); else p.fillOutbound(await need(f));
    p.lose(); await p.submit(kind);
    assert.equal(rows(f,kind).length,1);
    assert.match(p.alerts.at(-1),/失败/);
    assert.notEqual(p.node(kind==='inbound'?'ibc-customer':'oc-customer').value,'');
    await p.submit(kind);
    assert.equal(p.sent.length,2);
    assert.equal(p.sent[0].client_req_id,p.sent[1].client_req_id);
    assert.equal(p.replies[1].ok,true);
    assert.equal(p.replies[0].id,p.replies[1].id);
    assert.equal(rows(f,kind).length,1);
    assert.ok(p.alerts.at(-1).includes(rows(f,kind)[0].display_no));
    assert.equal(p.node(kind==='inbound'?'ibc-customer':'oc-customer').value,'');
  });

  test('002 '+kind+' editing quantity after failed reply starts a new intent',async t=>{
    const f=fixture(t),p=frontend(f); let n;
    if(kind==='inbound')p.fillInbound(50); else{n=await need(f);p.fillOutbound(n,60);}
    p.lose();await p.submit(kind);
    if(kind==='inbound')p.node('qa-ibc-qty').value='40';
    else{
      const current=(await f.ok('sop_get',{id:n.id})).record;
      p.fillOutbound({...current,remaining:40},40);
    }
    await p.submit(kind);
    assert.notEqual(p.sent[0].client_req_id,p.sent[1].client_req_id);
    assert.equal(p.replies[1].ok,true);
    assert.equal(rows(f,kind).length,2);
    if(kind==='outbound')assert.equal(f.DB.raw.prepare('SELECT SUM(planned_box_count) n FROM v2_outbound_orders').get().n,100);
    else assert.deepEqual(f.DB.raw.prepare('SELECT planned_qty FROM v2_inbound_plan_lines ORDER BY rowid').all().map(x=>x.planned_qty),[50,40]);
  });

  test('002 '+kind+' successful clear and an identical new form use a fresh intent',async t=>{
    const f=fixture(t),p=frontend(f); let n;
    if(kind==='inbound')p.fillInbound(20); else{n=await need(f);p.fillOutbound(n,20);}
    await p.submit(kind);
    assert.equal(p.c._createSubmissionIntents[kind==='inbound'?'v2_inbound_plan_create':'v2_outbound_order_create'],undefined);
    if(kind==='inbound')p.fillInbound(20);
    else p.fillOutbound({...((await f.ok('sop_get',{id:n.id})).record),remaining:80},20);
    await p.submit(kind);
    assert.notEqual(p.sent[0].client_req_id,p.sent[1].client_req_id);
    assert.equal(rows(f,kind).length,2);
  });

  test('002 '+kind+' explicit form clear after uncertain reply clears its pending intent',async t=>{
    const f=fixture(t),p=frontend(f);
    if(kind==='inbound')p.fillInbound(20); else p.fillOutbound(await need(f),20);
    p.lose();await p.submit(kind);
    const action=kind==='inbound'?'v2_inbound_plan_create':'v2_outbound_order_create';
    assert.ok(p.c._createSubmissionIntents[action]);
    p.c[kind==='inbound'?'clearIbcMaterials':'clearOcMaterials']();
    assert.equal(p.c._createSubmissionIntents[action],undefined);
  });

  test('002 '+kind+' concurrent repeated clicks are locked to one request',async t=>{
    const f=fixture(t),p=frontend(f);
    if(kind==='inbound')p.fillInbound();else p.fillOutbound(await need(f));
    const release=p.block();
    const first=p.submit(kind);
    assert.equal(p.node('qa-submit').disabled,true);
    const second=p.submit(kind);
    assert.equal(p.sent.length,1);
    release();await Promise.all([first,second]);
    assert.equal(rows(f,kind).length,1);
    assert.equal(p.sent.length,1);
    assert.equal(p.node('qa-submit').disabled,false);
  });
}
