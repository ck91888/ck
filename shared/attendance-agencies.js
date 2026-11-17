// Presentation only. CKIK remains the canonical stored/submitted company key.
(function(root){'use strict';
 function label(value){return String(value??'').split(/([／/、,，;；|\n])/).map(part=>part.replace(/^(\s*)CKIK(\s*)$/,'$1한국인력개발$2')).join('');}
 root.CKAttendanceAgencies=Object.freeze({label});
})(globalThis);
