(function(){
  // ===== Helper: display name for plan =====
  function planNo(plan){ return (plan && (plan.display_no || plan.id)) || ''; }
  function unitLabelSafe(key){ if(typeof unitTypeLabel==='function') return unitTypeLabel(key); return key; }

  function collectInboundLinesStrict(){
    var lines=typeof getIbcLines==='function'?getIbcLines():[];
    return (lines||[]).filter(function(ln){ return ln&&Number(ln.planned_qty||0)>0; });
  }

  // ===== Override submitInbound =====
  window.submitInbound = async function(){
    var date=document.getElementById('ibc-date').value||kstToday();
    var customer=document.getElementById('ibc-customer').value.trim();
    var biz=document.getElementById('ibc-biz').value;
    var cargo=document.getElementById('ibc-cargo').value.trim();
    var arrival=document.getElementById('ibc-arrival').value.trim();
    var purpose=document.getElementById('ibc-purpose').value.trim();
    var remark=document.getElementById('ibc-remark').value.trim();
    var lines=collectInboundLinesStrict();
    var autoOb=document.getElementById('ibc-auto-ob').checked;
    if(!customer){ alert(L('customer')+' '+L('required')+'!'); return; }
    if(lines.length===0){ alert(L('plan_lines')+' '+L('required')+'!'); return; }
    if(!cargo){ cargo=lines.map(function(ln){ return unitLabelSafe(ln.unit_type)+' '+ln.planned_qty; }).join(' / '); }
    var payload={ action:'v2_inbound_plan_create', plan_date:date, customer:customer, biz_class:biz, cargo_summary:cargo, expected_arrival:arrival, purpose:purpose, remark:remark, lines:lines, created_by:getUser() };
    if(autoOb){ payload.auto_create_outbound=true; payload.ob_operation_mode=(document.getElementById('ibc-ob-opmode')||{}).value||''; payload.ob_outbound_mode=(document.getElementById('ibc-ob-outmode')||{}).value||''; payload.ob_instruction=(document.getElementById('ibc-ob-instruction')||{}).value||''; }
    var res=await api(payload);
    if(res&&res.ok){
      var msg=L('success')+': '+(res.display_no||res.id);
      if(res.outbound_id) msg+='\n'+L('auto_create_outbound')+': '+res.outbound_id;
      alert(msg);
      document.getElementById('ibc-customer').value='';
      document.getElementById('ibc-cargo').value='';
      document.getElementById('ibc-arrival').value='';
      document.getElementById('ibc-purpose').value='';
      document.getElementById('ibc-remark').value='';
      document.getElementById('ibcLinesBody').innerHTML='';
      document.getElementById('ibc-auto-ob').checked=false;
      document.getElementById('ibcAutoObFields').style.display='none';
      if(typeof addIbcLine==='function') addIbcLine();
      goTab('inbound');
    } else {
      alert(L('error')+': '+(res?res.error:'unknown'));
    }
  };

  // ===== Override loadInboundList =====
  window.loadInboundList = async function(){
    var body=document.getElementById('inboundListBody'); if(!body) return;
    body.innerHTML='<div class="card muted">'+L('loading')+'</div>';
    var start=document.getElementById('ibFilterStart').value;
    var end=document.getElementById('ibFilterEnd').value;
    var status=document.getElementById('ibFilterStatus').value;
    var res=await api({action:'v2_inbound_plan_list',start_date:start,end_date:end,status:status});
    if(!res||!res.ok){ body.innerHTML='<div class="card muted">'+L('error')+'</div>'; return; }
    var items=res.items||[];
    if(items.length===0){ body.innerHTML='<div class="card muted">'+L('no_data')+'</div>'; return; }
    items.sort(function(a,b){
      var da=String(a.plan_date||''); var db=String(b.plan_date||'');
      if(da!==db) return db.localeCompare(da);
      return String(b.created_at||'').localeCompare(String(a.created_at||''));
    });
    var html='<div class="card">';
    items.forEach(function(p){
      var dynTag = (p.source_type==='field_dynamic') ? '<span style="background:#ff9800;color:#fff;font-size:10px;padding:1px 4px;border-radius:2px;margin-right:4px;">动态</span>' : '';
      html+='<div class="list-item" onclick="openInboundDetail(\''+esc(p.id)+'\')"><div class="item-title"><span class="st st-'+esc(p.status)+'">'+esc(stLabel(p.status))+'</span> '+dynTag+'<span class="biz-tag biz-'+esc(p.biz_class)+'">'+esc(bizLabel(p.biz_class))+'</span> '+esc(p.display_no||p.id)+' · '+esc(p.customer||'--')+'</div><div class="item-meta">'+esc(p.plan_date||'')+' · '+esc(p.cargo_summary||'')+' · '+esc(fmtTime(p.created_at))+'</div></div>';
    });
    html+='</div>'; body.innerHTML=html;
  };

  // ===== Override loadInboundDetail =====
  var _origLoadInboundDetail=window.loadInboundDetail;
  window.loadInboundDetail=async function(){
    await _origLoadInboundDetail();
    if(!_currentInboundId) return;
    var res=await api({action:'v2_inbound_plan_detail',id:_currentInboundId});
    if(!res||!res.ok||!res.plan) return;
    var pretty=planNo(res.plan);
    window._currentInboundPretty=pretty;
    window._currentInboundPlanCache=res.plan;
    var body=document.getElementById('inboundDetailBody'); if(!body) return;
    var titleEl=body.querySelector('.card div[style*="font-size:16px"]');
    if(titleEl) titleEl.textContent=pretty;
  };

  // ===== Override printIbQr: A4 单据布局，右上角小二维码 =====
  window.printIbQr=function(){
    var displayNo=window._currentInboundPretty||_currentInboundId||'';
    var planId=_currentInboundId||'';
    var plan=window._currentInboundPlanCache||{};

    // Generate small QR SVG in main page
    var qrHtml='';
    try{ qrHtml=buildInboundQrHtml(planId, 3); }catch(e){ qrHtml='<div style="color:red;font-size:10px;">QR error</div>'; }

    // Collect lines table from detail page
    var detailBody=document.getElementById('inboundDetailBody');
    var tables=detailBody?detailBody.querySelectorAll('table.line-table'):[];
    var linesHtml=tables.length?tables[0].outerHTML:'';

    // Biz label helper
    var bizMap={direct_ship:'直发/직배송',bulk:'大货/대량',return_op:'退件/반품',inventory_op:'库内/창고'};
    var bizText=bizMap[plan.biz_class]||plan.biz_class||'';

    var win=window.open('','_blank');
    var html='<!doctype html><html><head><meta charset="utf-8"/><title>'+esc(displayNo)+'</title>'+
      '<style>'+
      'body{font-family:"Microsoft YaHei","Helvetica Neue",Arial,sans-serif;margin:20px 30px;color:#000;}'+
      '.print-header{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:3px solid #000;padding-bottom:10px;margin-bottom:14px;}'+
      '.print-title{font-size:22px;font-weight:900;}'+
      '.print-sub{font-size:13px;color:#333;margin-top:4px;}'+
      '.qr-box{text-align:center;flex-shrink:0;margin-left:20px;}'+
      '.qr-box svg{width:100px;height:100px;}'+
      '.qr-label{font-size:10px;color:#666;margin-top:2px;line-height:1.3;}'+
      '.info-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px 24px;font-size:13px;margin-bottom:14px;}'+
      '.info-grid .label{font-weight:700;}'+
      'table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px;}'+
      'th,td{border:1px solid #333;padding:5px 6px;text-align:left;}'+
      'th{background:#eee;font-weight:700;}'+
      '.sig-row{display:flex;gap:40px;margin-top:30px;font-size:13px;}'+
      '.sig-item{flex:1;}'+
      '.sig-line{border-bottom:1px solid #333;height:30px;margin-top:4px;}'+
      '.footer{margin-top:16px;font-size:11px;color:#888;text-align:center;border-top:1px dashed #ccc;padding-top:6px;}'+
      '@media print{@page{size:A4;margin:15mm 20mm;} body{margin:0;}}'+
      '</style></head><body>'+

      // Header: 左标题 + 右二维码
      '<div class="print-header">'+
        '<div>'+
          '<div class="print-title">入库计划单</div>'+
          '<div class="print-sub">CK 仓储</div>'+
        '</div>'+
        '<div class="qr-box">'+qrHtml+'<div class="qr-label">'+esc(displayNo)+'<br/>'+esc(planId)+'</div></div>'+
      '</div>'+

      // Info grid: 两列正文
      '<div class="info-grid">'+
        '<div><span class="label">入库单号：</span>'+esc(displayNo)+'</div>'+
        '<div><span class="label">货物摘要：</span>'+esc(plan.cargo_summary||'')+'</div>'+
        '<div><span class="label">计划日期：</span>'+esc(plan.plan_date||'')+'</div>'+
        '<div><span class="label">预计到达：</span>'+esc(plan.expected_arrival||'--')+'</div>'+
        '<div><span class="label">客户：</span>'+esc(plan.customer||'')+'</div>'+
        '<div><span class="label">提出人：</span>'+esc(plan.created_by||'')+'</div>'+
        '<div><span class="label">业务分类：</span>'+esc(bizText)+'</div>'+
        (plan.remark?'<div><span class="label">备注：</span>'+esc(plan.remark)+'</div>':'')+
        (plan.purpose?'<div style="grid-column:1/-1;"><span class="label">入库目的：</span>'+esc(plan.purpose)+'</div>':'')+
      '</div>'+

      // Lines table
      linesHtml+

      // Signature row
      '<div class="sig-row">'+
        '<div class="sig-item"><span class="label">制单人：</span>'+esc(plan.created_by||'')+'<div class="sig-line"></div></div>'+
        '<div class="sig-item"><span class="label">仓库确认：</span><div class="sig-line"></div></div>'+
        '<div class="sig-item"><span class="label">客户签收：</span><div class="sig-line"></div></div>'+
        '<div class="sig-item"><span class="label">日期：</span><div class="sig-line"></div></div>'+
      '</div>'+

      '<div class="footer">Printed from CK Warehouse V2</div>'+
      '<script>window.onload=function(){window.print();}<\/script>'+
      '</body></html>';
    win.document.write(html);
    win.document.close();
  };

  // ===== Auto add first line =====
  function ensureOneLine(){
    var tbody=document.getElementById('ibcLinesBody');
    if(tbody&&!tbody.children.length&&typeof addIbcLine==='function') addIbcLine();
  }
  document.addEventListener('DOMContentLoaded',function(){
    ensureOneLine();
    var btn=document.getElementById('btnNewInbound');
    if(btn){ btn.addEventListener('click',function(){ setTimeout(ensureOneLine,30); }); }
  });
})();
