/* Personal interval accounting; no fixed rest deduction or headcount division. */

  'use strict';
  const AGENCIES=['가온','포레인','동인천'];
  const DEPTS=['bulk','direct_ship','import'];
  const normalizeName=value=>String(value||'').normalize('NFC').trim().replace(/\s+/g,' ');
  const kstDay=value=>new Date(new Date(value).getTime()+9*3600000).toISOString().slice(0,10);
  function validName(value){const name=normalizeName(value);if(!name||name.length>40||/[|<>\u0000-\u001f]/u.test(name))throw Error('请填写正确姓名 / 이름을 확인하세요');return name;}
  function attendanceReport(record,segments,asOf,day=record.day){
    const result={presence:0,bulk:0,direct_ship:0,import:0,rest:0,unassigned:0,other:0,conflict:0,flags:[]};
    const from=Date.parse(day+'T00:00:00+09:00'),to=from+86400000;
    const now=Date.parse(asOf),arrival=Date.parse(record.inAt),departure=record.outAt?Date.parse(record.outAt):now;
    if(!Number.isFinite(now)||!Number.isFinite(arrival)||!Number.isFinite(departure)||departure<arrival){result.flags.push('上下班时间异常');return result;}
    if(!record.outAt&&kstDay(asOf)>record.day){result.flags.push('跨日未签退，待核实');if(day>record.day)return result;}
    const start=Math.max(from,arrival),end=Math.min(to,departure,now);
    if(end<=start)return result;
    result.presence=(end-start)/60000;
    const intervals=[],bounds=new Set([start,end]);
    const add=(s,e,type,department)=>{const rawS=Date.parse(s),rawE=e?Date.parse(e):now;if(!Number.isFinite(rawS)||!Number.isFinite(rawE)||rawE<rawS){result.flags.push('作业时间异常');return;}if(type==='job'&&(rawS<arrival||(record.outAt&&rawE>departure)))result.flags.push('任务超出签到区间');const a=Math.max(start,rawS),b=Math.min(end,rawE);if(b>a){intervals.push({a,b,type,department});bounds.add(a);bounds.add(b);}};
    for(const s of segments.filter(s=>s.badgeId===record.badgeId)){if(!s.end&&record.outAt)result.flags.push('已签退但作业未退出');add(s.start,s.end,'job',s.department);}
    for(const r of record.breaks||[])add(r.start,r.end,'rest','');
    const sorted=[...bounds].sort((a,b)=>a-b);
    for(let i=0;i<sorted.length-1;i++){
      const a=sorted[i],b=sorted[i+1],minutes=(b-a)/60000,active=intervals.filter(x=>x.a<b&&x.b>a);
      const jobs=active.filter(x=>x.type==='job'),rest=active.some(x=>x.type==='rest');
      const departments=new Set(jobs.map(x=>DEPTS.includes(x.department)?x.department:'other'));
      if((rest&&jobs.length)||departments.size>1){result.conflict+=minutes;result.flags.push(rest?'休息与作业重叠':'跨部门时间重叠');}
      else if(rest)result.rest+=minutes;
      else if(departments.size===1)result[[...departments][0]]+=minutes;
      else result.unassigned+=minutes;
    }
    result.flags=[...new Set(result.flags)];
    return result;
  }

export { attendanceReport, kstDay };
