// Read-only UI ownership. No request cancellation or mutation/API interception.
(function(w){
 'use strict';
 var slots=Object.create(null);
 function identity(legacy){
  var session=w.CKSession,user=session&&session.user;
  var stamp=JSON.stringify([session?session.scope:'',user&&user.id,user&&user.scope,user&&user.role,user&&user.departments,user&&user.operation_context,legacy?legacy():'']);
  return function(){
   var next=w.CKSession,current=next&&next.user;
   return session===next&&(!session||!!user&&current===user)&&stamp===JSON.stringify([next?next.scope:'',current&&current.id,current&&current.scope,current&&current.role,current&&current.departments,current&&current.operation_context,legacy?legacy():'']);
  };
 }
 w.CKLatestListRead={
  identity:identity,
  begin:function(scope,host,legacy){
   var owns=identity(legacy),ready=false,token={};slots[scope]=token;
   token.owned=function(){return owns()&&!!host&&host.isConnected!==false&&(!host.id||w.document.getElementById(host.id)===host);};
   token.current=function(){return slots[scope]===token&&token.owned();};
   token.commit=function(){if(!token.current())return false;ready=true;return true;};
   token.canUse=function(){return ready&&token.current();};
   return token;
  }
 };
})(window);
