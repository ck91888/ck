(function(){
  // ===== Helper: display name for plan =====
  function planNo(plan){ return (plan && (plan.display_no || plan.id)) || ''; }

  function unitLabelSafe(key){
    if(typeof unitLabel==='function') return unitLabel(key);
    return key;
  }

  // ===== Override loadInboundPlans: add display_no to each option =====
  window.loadInboundPlans = async function(selectId){
    var sel=document.getElementById(selectId);
    if(!sel) return;
    var current=sel.value||'';
    var res=await api({action:'v2_inbound_plan_list',start_date:'',end_date:'',status:''});
    var items=(res&&res.ok&&res.items)?res.items.slice():[];

    var opts='<option value="">-- 选择入库计划/입고계획 선택 --</option>';
    // Group by date, show newest first
    var byDate={};
    items.forEach(function(p){
      if(p.status==='completed'||p.status==='cancelled') return;
      var d=p.plan_date||'';
      if(!byDate[d]) byDate[d]=[];
      byDate[d].push(p);
    });
    Object.keys(byDate).sort().reverse().forEach(function(d){
      byDate[d].sort(function(a,b){ return String(a.created_at||'').localeCompare(String(b.created_at||'')); });
      byDate[d].forEach(function(p){
        var no=p.display_no||p.id;
        opts+='<option value="'+esc(p.id)+'" data-display-no="'+esc(no)+'">['+esc(no)+'] '+esc(p.customer||'')+' - '+esc(p.cargo_summary||'')+'</option>';
      });
    });
    sel.innerHTML=opts;
    if(current) sel.value=current;

    if(selectId==='unloadPlanSelect') previewSelectedPlan();
  };

  // ===== Get display_no from selected option =====
  function selectedDisplayNo(){
    var sel=document.getElementById('unloadPlanSelect');
    if(!sel||sel.selectedIndex<1) return '';
    return sel.options[sel.selectedIndex].getAttribute('data-display-no')||'';
  }

  // ===== Render plan card =====
  function renderUnloadPlanCard(planData, displayNo){
    if(!planData||!planData.plan) return;
    _unloadPlanData=planData;
    var p=planData.plan;
    var lines=planData.lines||[];
    var card=document.getElementById('unloadPlanCard');
    var info=document.getElementById('unloadPlanInfo');
    var area=document.getElementById('unloadPlanLinesArea');
    if(card) card.style.display='';
    if(info){
      info.innerHTML='<div><b>'+esc(displayNo||planNo(p))+'</b> | '+esc(p.plan_date)+' | '+esc(p.customer||'')+'</div>'+
        '<div class="muted">'+esc(p.cargo_summary||'')+(p.remark?' — '+esc(p.remark):'')+'</div>';
    }
    if(area){
      if(lines.length>0){
        var tbl='<table class="mini-table"><tr><th>类型/유형</th><th>计划/계획</th></tr>';
        lines.forEach(function(ln){ tbl+='<tr><td>'+unitLabelSafe(ln.unit_type)+'</td><td>'+ln.planned_qty+'</td></tr>'; });
        tbl+='</table>';
        area.innerHTML=tbl;
      } else {
        area.innerHTML='<span class="muted">无明细 / 명세 없음</span>';
      }
    }
  }

  // ===== Preview: on dropdown change =====
  async function previewSelectedPlan(){
    var sel=document.getElementById('unloadPlanSelect');
    var card=document.getElementById('unloadPlanCard');
    if(!sel||!card) return;
    var planId=sel.value||'';
    if(!planId){ card.style.display='none'; return; }
    var no=selectedDisplayNo();
    var res=await api({action:'v2_inbound_plan_detail',id:planId});
    if(res&&res.ok&&res.plan){
      renderUnloadPlanCard(res, no||planNo(res.plan));
    } else {
      card.style.display='none';
    }
  }

  // ===== Override showUnloadWorking =====
  window.showUnloadWorking = function(job){
    document.getElementById('unloadEntryCard').style.display='none';
    document.getElementById('unloadWorkersCard').style.display='';
    document.getElementById('unloadResultCard').style.display='';

    if(_unloadPlanData&&_unloadPlanData.plan){
      renderUnloadPlanCard(_unloadPlanData, planNo(_unloadPlanData.plan));
    } else {
      document.getElementById('unloadPlanCard').style.display='none';
    }

    buildUnloadResultForm();
    refreshUnloadWorkers();
    stripDiffRequired();
  };

  // ===== Override initUnload =====
  window.initUnload = async function(){
    _unloadPlanData=null;
    stopUnloadScan();

    if(_activeJobId){
      var res=await api({action:'v2_ops_job_detail',job_id:_activeJobId});
      if(res&&res.ok&&res.job&&res.job.job_type==='unload'&&res.job.status==='working'){
        // Only load plan if related to inbound_plan, not field_feedback
        if(res.job.related_doc_type==='inbound_plan'&&res.job.related_doc_id){
          var planRes=await api({action:'v2_inbound_plan_detail',id:res.job.related_doc_id});
          if(planRes&&planRes.ok) _unloadPlanData=planRes;
        } else {
          _unloadPlanData=null; // feedback-first flow: no plan data
        }
        showUnloadWorking(res.job);
        startJobPoll('unload');
        return;
      }
    }

    showUnloadEntry();
  };

  // ===== Override startUnload =====
  window.startUnload = async function(){
    if(_startInflight) return;
    _startInflight=true;
    try{
      var sel=document.getElementById('unloadPlanSelect');
      var planId=sel?sel.value:'';

      var res=await api({
        action:'v2_unload_job_start',
        plan_id:planId,
        worker_id:getWorkerId(),
        worker_name:getWorkerName(),
        biz_class:''
      });

      if(res&&res.ok){
        saveActiveJob(res.job_id, res.worker_seg_id);
        stopUnloadScan();

        _unloadPlanData=null;
        if(planId){
          var planRes=await api({action:'v2_inbound_plan_detail',id:planId});
          if(planRes&&planRes.ok) _unloadPlanData=planRes;
        }

        if(!res.already_joined){
          alert(res.is_new_job?'已创建卸货任务 / 하차 작업 생성됨':'已加入卸货任务 / 하차 작업 참여됨');
        }
        var jobRes=await api({action:'v2_ops_job_detail',job_id:res.job_id});
        if(jobRes&&jobRes.ok) showUnloadWorking(jobRes.job);
        startJobPoll('unload');
      } else {
        alert('失败/실패: '+(res?res.error:'unknown'));
      }
    }finally{ _startInflight=false; }
  };

  // ===== Override unloadComplete =====
  window.unloadComplete = async function(){
    if(!_activeJobId) return;
    var resultLines=getUnloadResultLines();
    if(resultLines.length===0){ alert('请至少填写一项实际数量 / 실제 수량을 최소 1건 입력하세요'); return; }
    var planLines=(_unloadPlanData&&_unloadPlanData.lines)||[];
    var diffNote=((document.getElementById('unloadDiffNote')||{}).value||'').trim();
    if(planLines.length>0){
      var hasDiff=false; var actualMap={};
      resultLines.forEach(function(r){ actualMap[r.unit_type]=r.actual_qty; });
      planLines.forEach(function(ln){ if((actualMap[ln.unit_type]||0)!==(ln.planned_qty||0)) hasDiff=true; });
      if(hasDiff&&!diffNote) diffNote='现场实收数量与计划数量不一致';
    }
    var remark=((document.getElementById('unloadRemark')||{}).value||'').trim();

    // Determine which finish API to call based on whether this is feedback-first flow
    var fbId=localStorage.getItem('v2_unplanned_fb_id')||'';
    var actionName = fbId ? 'v2_unplanned_unload_finish' : 'v2_unload_job_finish';

    var res=await api({action:actionName,job_id:_activeJobId,worker_id:getWorkerId(),result_lines:resultLines,diff_note:diffNote,remark:remark,complete_job:true});
    if(res&&res.ok){
      var msg='卸货已完成 / 하차 완료';
      if(fbId||res.feedback_id){
        msg+='\n（现场反馈已更新，请协同中心补充信息并转正 / 현장 피드백 업데이트됨, 협업센터에서 정보 보완 후 전환 필요）';
      }
      alert(msg);

      // Clean up feedback tracking
      localStorage.removeItem('v2_unplanned_fb_id');

      // Check if there's a parent job to resume
      var resumed = await checkAndResumeParent();
      if(!resumed){
        clearActiveJob();
        _unloadPlanData=null;
        goPage('home');
      }
    }
    else if(res&&res.error==='others_still_working'){ alert('还有'+res.active_count+'人参与中，无法完成 / 아직 '+res.active_count+'명 참여 중, 완료 불가'); }
    else if(res&&res.error==='empty_result'){ alert(res.message||'至少填写一项实际数量'); }
    else if(res&&res.error==='diff_note_required'){
      // Auto-fill default diff note and retry
      diffNote='现场实收数量与计划数量不一致（自动补充）';
      var el=document.getElementById('unloadDiffNote');
      if(el) el.value=diffNote;
      var retryRes=await api({action:actionName,job_id:_activeJobId,worker_id:getWorkerId(),result_lines:resultLines,diff_note:diffNote,remark:remark,complete_job:true});
      if(retryRes&&retryRes.ok){
        alert('卸货已完成（已自动补充差异备注）/ 하차 완료 (차이 메모 자동 추가됨)');
        localStorage.removeItem('v2_unplanned_fb_id');
        var resumed2 = await checkAndResumeParent();
        if(!resumed2){ clearActiveJob(); _unloadPlanData=null; goPage('home'); }
      } else {
        alert('失败/실패: '+(retryRes?retryRes.error:'unknown'));
      }
    }
    else { alert('失败/실패: '+(res?res.error:'unknown')); }
  };

  // ===== Diff label: optional =====
  function stripDiffRequired(){
    var diffArea=document.getElementById('unloadDiffArea');
    if(!diffArea) return;
    var lbl=diffArea.querySelector('label');
    if(lbl) lbl.textContent='差异备注（可选） / 차이 메모(선택)';
  }

  // ===== Override startUnloadNoPlan: feedback-first flow =====
  window.startUnloadNoPlan = async function(){
    if(_startInflight) return;
    _startInflight=true;
    try{
      // Check if we're coming from an interrupt
      var interruptInfo=null;
      try{ interruptInfo=JSON.parse(localStorage.getItem(V2_INTERRUPT_KEY)||'null'); }catch(e){}

      var res=await api({
        action:'v2_unplanned_unload_start',
        worker_id:getWorkerId(),
        worker_name:getWorkerName(),
        parent_job_id:(interruptInfo&&interruptInfo.parent_job_id)||'',
        interrupt_type:(interruptInfo&&interruptInfo.parent_job_id)?'unload':''
      });
      if(res&&res.ok){
        saveActiveJob(res.job_id, res.worker_seg_id);
        // Store feedback_id for use when completing
        localStorage.setItem('v2_unplanned_fb_id', res.feedback_id||'');
        stopUnloadScan();
        _unloadPlanData=null; // No plan in feedback-first flow
        alert('已创建计划外卸货单: '+(res.display_no||res.feedback_id)+'\n계획외 하차 작업 생성됨');
        var jobRes=await api({action:'v2_ops_job_detail',job_id:res.job_id});
        if(jobRes&&jobRes.ok) showUnloadWorking(jobRes.job);
        startJobPoll('unload');
      } else {
        alert('失败/실패: '+(res?res.error:'unknown'));
      }
    }finally{ _startInflight=false; }
  };

  // ===== Override loadInboundPlans: tag dynamic + field_working plans =====
  var _origLoadInboundPlans = window.loadInboundPlans;
  window.loadInboundPlans = async function(selectId){
    var sel=document.getElementById(selectId);
    if(!sel) return;
    var current=sel.value||'';
    var res=await api({action:'v2_inbound_plan_list',start_date:'',end_date:'',status:''});
    var items=(res&&res.ok&&res.items)?res.items.slice():[];

    var opts='<option value="">-- 选择入库计划/입고계획 선택 --</option>';
    var byDate={};
    items.forEach(function(p){
      if(p.status==='completed'||p.status==='cancelled') return;
      var d=p.plan_date||'';
      if(!byDate[d]) byDate[d]=[];
      byDate[d].push(p);
    });
    Object.keys(byDate).sort().reverse().forEach(function(d){
      byDate[d].sort(function(a,b){ return String(a.created_at||'').localeCompare(String(b.created_at||'')); });
      byDate[d].forEach(function(p){
        var no=p.display_no||p.id;
        var tag = (p.source_type==='field_dynamic') ? '[现场动态] ' : '';
        if(p.status==='unloaded_pending_info') tag='[待补充] ';
        opts+='<option value="'+esc(p.id)+'" data-display-no="'+esc(no)+'">'+tag+'['+esc(no)+'] '+esc(p.customer||'')+' - '+esc(p.cargo_summary||'')+'</option>';
      });
    });
    sel.innerHTML=opts;
    if(current) sel.value=current;

    if(selectId==='unloadPlanSelect') previewSelectedPlan();
  };

  // ===== Bind dropdown change event =====
  document.addEventListener('DOMContentLoaded', function(){
    stripDiffRequired();
    var sel=document.getElementById('unloadPlanSelect');
    if(sel){ sel.addEventListener('change', previewSelectedPlan); }
  });
})();
