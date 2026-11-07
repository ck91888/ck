import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

// UI race candidates: actual 001 start/load/finish functions; simulated DOM
// values and controlled detail promises only. No browser, network or Worker writes.
const repo=path.resolve(process.env.CK_REVIEW_REPO||fileURLToPath(new URL('../',import.meta.url)));
const source=fs.readFileSync(path.join(repo,'001/app.js'),'utf8');
const between=(start,end)=>{const i=source.indexOf(start),j=source.indexOf(end,i+start.length);assert.ok(i>=0&&j>i,start);return source.slice(i,j);};
function fixture(t){
  const nodes=new Map(),detail=new Map(),finishBodies=[],alerts=[],actions=[];
  let inputs=[];
  const node=id=>{
    if(!nodes.has(id)){
      const el={id,value:'',disabled:false,style:{},textContent:'',dataset:{},querySelector:()=>null,querySelectorAll:()=>[],setAttribute(){},removeAttribute(){}};
      let html='';
      Object.defineProperty(el,'innerHTML',{get:()=>html,set(value){html=String(value);if(id==='inboundResultLines')inputs=[...html.matchAll(/<input\b([^>]*)>/g)].filter(m=>m[1].includes('ib-putaway-input')).map(m=>({value:(m[1].match(/\bvalue="([^"]*)"/)||[])[1]||'',getAttribute:key=>(m[1].match(new RegExp(key+'="([^"]*)"'))||[])[1]||''}));}});
      nodes.set(id,el);
    }
    return nodes.get(id);
  };
  const plan=(id,qty)=>({ok:true,plan:{id,status:'putting_away',display_no:id,customer:'QA only',cargo_summary:'QA'},lines:[{unit_type:'carton',actual_qty:qty}]});
  const c={console,Date,Math,JSON,Promise,crypto,
    _currentPage:'inbound',_activeJobId:'JOB-OLD',_pageParams:{job_type:'inbound_direct',biz_class:'direct_ship'},_ibResolvedKind:'system',_ibResolvedPlanId:'PLAN-B',_ibResolvedPlan:{selected_external_inbound_no:'QA-B'},
    JOB_TYPE_LABEL:{inbound_direct:'QA inbound'},getWorkerId:()=> 'QA-A',getWorkerName:()=> 'QA alpha',hasOtherActiveJob:()=>false,_resolveBizClass:()=> 'direct_ship',
    stopInboundScan(){},refreshInboundWorkers(){},startJobPoll(){},clearActiveJob(){c._activeJobId='';},goPage(){},isAlreadyCompletedResponse:()=>false,
    saveActiveJob:id=>{c._activeJobId=id;},alert:s=>alerts.push(s),esc:s=>String(s||''),unitLabel:s=>s,renderInboundPlanRemark:()=>'',renderInboundMaterialsReadonly:()=>'',
    document:{getElementById:node,querySelector:selector=>selector.includes('finishInbound')?node('qa-finish-btn'):null,querySelectorAll:selector=>selector==='.ib-putaway-input'?inputs:[]},
    withActionLock(key,_button,_label,fn){const promise=fn();actions.push({key,promise});return promise;}};
  c.window=c;
  c.api=async body=>{
    if(body.action==='v2_inbound_job_start')return{ok:true,job_id:'JOB-B',plan_id:'PLAN-B',already_joined:true};
    if(body.action==='v2_inbound_plan_detail'){
      let release;const promise=new Promise(r=>{release=r;});detail.set(body.id,{release,promise});return promise;
    }
    if(body.action==='v2_inbound_job_finish'){finishBodies.push(structuredClone(body));return{ok:false,error:'QA captured finish only'};}
    return{ok:true};
  };
  vm.createContext(c);
  vm.runInContext(between('async function startInbound(', 'var _inboundPlanData ='),c);
  // Keep proposed loading-state helpers beside this existing state declaration
  // so the regression executes them, rather than copying their implementation.
  vm.runInContext(between('var _inboundPlanData =', 'async function inboundLeave('),c);
  vm.runInContext(between('async function finishInbound(', 'async function refreshInboundWorkers('),c);
  node('inboundResultLines').innerHTML='<input class="ib-putaway-input" data-unit="carton" value="20">';
  const tick=()=>new Promise(setImmediate);
  t.after(async()=>{for(const[id,p]of detail)p.release(plan(id,id==='PLAN-A'?20:30));await tick();});
  return{c,node,detail,finishBodies,alerts,actions,plan,tick,inputs:()=>inputs};
}

test('starting the second job cannot submit previous-job quantity while its plan detail is pending',async t=>{
  const p=fixture(t);const started=p.c.startInbound(null);await p.tick();
  assert.ok(p.detail.has('PLAN-B'),'actual start must issue the current plan read');
  await p.c.finishInbound(null);await p.tick();
  assert.equal(p.finishBodies.length,0,'loading gate must prevent old 20 being sent for JOB-B');
  assert.equal(p.inputs().length,0,'old result rows must be cleared synchronously before awaiting detail');
  p.detail.get('PLAN-B').release(p.plan('PLAN-B',30));await started;await p.tick();
  assert.equal(p.c._inboundPlanData.plan.id,'PLAN-B');
});

test('failed current-plan detail keeps completion blocked instead of accepting empty or old lines',async t=>{
  const p=fixture(t);const loading=p.c.loadInboundPlanInfo('PLAN-B');
  p.detail.get('PLAN-B').release({ok:false,error:'QA detail failed'});await loading;
  await p.c.finishInbound(null);await p.tick();
  assert.equal(p.finishBodies.length,0,'failed detail is not ready for system-plan completion');
  assert.equal(p.inputs().length,0);
});

test('late previous-job detail cannot overwrite the newer job data or result DOM',async t=>{
  const p=fixture(t);p.c._activeJobId='JOB-A';const a=p.c.loadInboundPlanInfo('PLAN-A');
  p.c._activeJobId='JOB-B';const b=p.c.loadInboundPlanInfo('PLAN-B');
  p.detail.get('PLAN-B').release(p.plan('PLAN-B',30));await b;
  assert.equal(p.c._inboundPlanData.plan.id,'PLAN-B');
  p.detail.get('PLAN-A').release(p.plan('PLAN-A',20));await a;
  assert.equal(p.c._inboundPlanData.plan.id,'PLAN-B','response must match the still-active job and request generation');
  assert.equal(p.inputs()[0].value,'30');
});

test('a genuinely missing legacy plan ID clears all old result rows immediately',async t=>{
  const p=fixture(t);await p.c.loadInboundPlanInfo('');
  assert.equal(p.inputs().length,0);assert.equal(p.c._inboundPlanData,null);assert.equal(p.detail.size,0);
});

test('late original load cannot let the external-reference wrapper clear the newer job user input',async t=>{
  const p=fixture(t),wrapper=fs.readFileSync(path.join(repo,'shared/inbound-flow-ui.js'),'utf8');
  p.c.progressHtml=()=>'';p.c.document.createElement=()=>({className:'',innerHTML:'',textContent:''});
  const decorations=[];p.node('inboundPlanInfo').prepend=x=>decorations.push(x);p.node('inboundResultLines').prepend=()=>{};
  const start=wrapper.indexOf('const planInfo=window.loadInboundPlanInfo;'),end=wrapper.indexOf('\n   };',start);
  assert.ok(start>=0&&end>start);vm.runInContext(wrapper.slice(start,end+'\n   };'.length),p.c);
  p.c._activeJobId='JOB-A';const a=p.c.loadInboundPlanInfo('PLAN-A');p.c._activeJobId='JOB-B';const b=p.c.loadInboundPlanInfo('PLAN-B');
  const current=p.plan('PLAN-B',30);current.plan.inbound_progress={total:2};p.detail.get('PLAN-B').release(current);await b;
  p.inputs()[0].value='35';p.detail.get('PLAN-A').release(p.plan('PLAN-A',20));await a;
  assert.equal(p.c._inboundPlanData.plan.id,'PLAN-B');assert.equal(p.inputs()[0].value,'35');assert.equal(decorations.length,1);
});

test('verified old no-plan inbound may complete without inventing an inbound plan or old quantities',async t=>{
  const p=fixture(t),api=p.c.api;p.c.api=async body=>body.action==='v2_ops_job_detail'?{ok:true,job:{id:p.c._activeJobId,job_type:'inbound_direct',related_doc_type:'external_inbound'}}:api(body);
  await p.c.loadInboundPlanInfo('');await p.c.finishInbound(null);await p.tick();
  assert.equal(p.finishBodies.length,1);assert.equal(p.finishBodies[0].result_lines.length,0);assert.equal(p.c._inboundPlanData,null);
});

test('a mismatched plan response cannot unlock finish for the current job',async t=>{
  const p=fixture(t),loading=p.c.loadInboundPlanInfo('PLAN-B');p.detail.get('PLAN-B').release(p.plan('PLAN-A',20));await loading;await p.c.finishInbound(null);await p.tick();
  assert.equal(p.finishBodies.length,0);assert.equal(p.inputs().length,0);assert.equal(p.node('qa-finish-btn').disabled,true);
});
