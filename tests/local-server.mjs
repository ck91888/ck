// Local isolated fixture; never connects to production or imports real warehouse data.
import http from 'node:http';import fs from 'node:fs';import path from 'node:path';
import {database} from './d1-adapter.mjs';import {handleSop} from '../worker-v2/sop.js';
const root=path.resolve(new URL('..',import.meta.url).pathname),DB=database();
const env={DB,SOP_UPGRADE_ENABLED:'true',SOP_ENVIRONMENT:'local-test',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'测试经理',role:'manager',key:'local-fixture-only'}])};
DB.raw.exec("INSERT INTO v2_outbound_orders(id,customer,instruction,biz_class,status,uses_stock_operation) VALUES('OB-DEMO','演示客户','贴标100箱','bulk','operation_reserved',1); INSERT INTO v2_inbound_plans(id,customer) VALUES('IB-DEMO','演示客户'); INSERT INTO v2_issue_tickets(id,biz_class,issue_description) VALUES('I-DEMO','bulk','演示异常');");
const port=7831;
const server=http.createServer(async(req,res)=>{
 try{
 const url=new URL(req.url,'http://127.0.0.1:'+port);
 if(url.pathname==='/api'){
  let data='';for await (const chunk of req){data+=chunk;if(data.length>1000000)throw Error('too large');}
  res.setHeader('Content-Type','application/json');res.end(JSON.stringify(await handleSop(JSON.parse(data),env)));return;
 }
 if(url.pathname==='/sop/config.js'){res.setHeader('Content-Type','text/javascript');res.end("window.SOP_API='/api';");return;}
 let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep))throw Error('invalid path');
 if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
 const type={'.html':'text/html','.css':'text/css','.js':'text/javascript'}[path.extname(file)]||'application/octet-stream';res.setHeader('Content-Type',type);res.end(fs.readFileSync(file));
 }catch(e){res.statusCode=400;res.end(e.message);}
});server.listen(port,'127.0.0.1',()=>console.log('Isolated fixture: http://127.0.0.1:'+port+'/sop/'));
