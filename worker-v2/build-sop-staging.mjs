import { mkdir, copyFile, readFile, writeFile, rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
const here=dirname(fileURLToPath(import.meta.url)),root=resolve(here,'..'),out=resolve(here,'.sop-staging-assets');
if(!out.startsWith(root+'\\')&&!out.startsWith(root+'/'))throw Error('Unsafe build output');
await rm(out,{recursive:true,force:true});await mkdir(out,{recursive:true});
// Explicit original application allowlist. Never publish server code or credentials.
const apps=['001','002','003','shuju','attendance'];
const shared=['html5-qrcode.min.js','xlsx.full.min.js','qrcode.min.js','sop-entry.js','sop-native.js','sop-native.css','sop-session.js','sop-dispatch-ui.js','sop-planning-ui.js','sop-people.js','ck-design.css','ck-office.css','labor-department.js','inbound-flow-ui.js','courier-ui.js','courier-rules.js','courier.css','attendance-ui.js','field-work.js','work-result-ui.js','test-data-reset.js','test-data-reset.css','employee-attendance.js','employee-attendance.css','employee-import.js','unload-trip-ui.js','unload-trip.css','native-lifecycle-ui.js','native-lifecycle.css','feedback-link-ui.js','feedback-link.css','access-ui.css'];
await mkdir(resolve(out,'shared'),{recursive:true});
for(const f of shared)await copyFile(resolve(root,'shared',f),resolve(out,'shared',f));
await mkdir(resolve(out,'templates'),{recursive:true});
await writeFile(resolve(out,'templates/employees.xlsx'),Buffer.from(await readFile(resolve(root,'shared/templates/employees.xlsx.b64'),'utf8'),'base64'));
await writeFile(resolve(out,'templates/pallet-details.xlsx'),Buffer.from(await readFile(resolve(root,'shared/templates/pallet-details.xlsx.b64'),'utf8'),'base64'));
await writeFile(resolve(out,'shared/sop-rollout.js'),"window.CK_SOP_ROLLOUT={enabled:true,staging:true,publicAccess:false,accessControl:true};\nwindow.SOP_API=location.origin+(location.pathname.startsWith('/001/')?'/001/api':location.pathname.startsWith('/attendance/')?'/attendance/api':'/api');\n");
const head='<link rel="stylesheet" href="/shared/access-ui.css"><link rel="stylesheet" href="/shared/sop-native.css"><link rel="stylesheet" href="/shared/ck-design.css"><link rel="stylesheet" href="/shared/ck-office.css"><link rel="stylesheet" href="/shared/employee-attendance.css"><link rel="stylesheet" href="/shared/courier.css"><link rel="stylesheet" href="/shared/unload-trip.css"><link rel="stylesheet" href="/shared/native-lifecycle.css"><link rel="stylesheet" href="/shared/feedback-link.css"><script src="/shared/sop-rollout.js"></script><script src="/shared/sop-session.js"></script>';
for(const app of apps){
 await mkdir(resolve(out,app),{recursive:true});
 for(const f of app==='attendance'?['index.html','style.css']:['index.html','app.js','config.js','style.css']){
  let content=await readFile(resolve(root,app,f),'utf8');
  if(f==='config.js')content=content.replace(/var V2_API\s*=\s*"[^"]+";/,"var V2_API=window.SOP_API;").replace(/var OPS_KEY\s*=\s*"[^"]+";/,"var OPS_KEY='';");
  if(f==='index.html'){
   if(app==='001')content=content.replace(/<div class="home-btn" onclick="(?:window.open[^"]*|goPage\('realtime_board'\))">[\s\S]*?<\/div>\s*<\/div>/g,'');
   content=content.replace(/<script src="\.\.\/shared\/sop-(entry|rollout)\.js[^\"]*"><\/script>/g,'');
   const libraries=['html5-qrcode.min.js','xlsx.full.min.js','qrcode.min.js'].filter(name=>!content.includes(name)).map(name=>'<script src="/shared/'+name+'"></script>').join('');
   content=content.replace('<body>','<body class="ck-system" data-app="'+app+'">');
   content=content.replace('</head>',head+'</head>').replace('</body>',libraries+'<script src="/shared/sop-planning-ui.js"></script><script src="/shared/sop-people.js"></script><script src="/shared/sop-native.js"></script><script src="/shared/sop-dispatch-ui.js"></script><script src="/shared/attendance-ui.js"></script><script src="/shared/employee-import.js"></script><script src="/shared/employee-attendance.js"></script><script src="/shared/work-result-ui.js"></script><script src="/shared/field-work.js"></script><script src="/shared/inbound-flow-ui.js"></script><script src="/shared/courier-ui.js"></script><script src="/shared/unload-trip-ui.js"></script><script src="/shared/native-lifecycle-ui.js"></script><script src="/shared/feedback-link-ui.js"></script><script src="/shared/sop-entry.js"></script></body>');
  }
  await writeFile(resolve(out,app,f),content);
 }
}
let home=await readFile(resolve(root,'shared/ck-home.html'),'utf8');
home=home.replace('<body>','<body class="ck-stage-home">').replace('</head>',head+'</head>').replace('</body>','<script src="/shared/attendance-ui.js"></script><script src="/shared/employee-attendance.js"></script><script src="/shared/field-work.js"></script><script src="/shared/inbound-flow-ui.js"></script><script src="/shared/sop-entry.js"></script></body>');
home=home.replace('</head>','<link rel="stylesheet" href="/shared/test-data-reset.css"></head>').replace('</body>','<script src="/shared/test-data-reset.js"></script></body>');
await writeFile(resolve(out,'index.html'),home);
await mkdir(resolve(out,'office-login'),{recursive:true});
await writeFile(resolve(out,'office-login/index.html'),'<!doctype html><html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>CK 办公室登录</title>'+head+'</head><body class="ck-system"><script>CKSession.ready.then(()=>{const p=new URLSearchParams(location.search).get("next")||"/";location.replace(/^\\/(?:002|003|shuju)(?:\\/|$)/.test(p)?p:"/");});</script></body></html>');
await copyFile(resolve(root,'docs/sop-acceptance.html'),resolve(out,'验收说明.html'));
await writeFile(resolve(out,'_redirects'),'/sop/ / 302\n/sop / 302\n');
await writeFile(resolve(out,'_headers'),'/*\n  Cache-Control: no-store\n  X-Content-Type-Options: nosniff\n  Referrer-Policy: same-origin\n');
await writeFile(resolve(out,'release.json'),JSON.stringify({release:'20260923-employee-default-access',builtAt:new Date().toISOString(),modules:apps}));
console.log('Prepared five CK applications with attendance and unified design.');
