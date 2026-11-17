// Native local workerd D1 SQL, actual service code, no remote account/database.
import assert from 'node:assert/strict';import {database} from './d1-adapter.mjs';import {localD1} from './d1-local-runtime.mjs';import {photoReceiptCases} from './003-photo-idempotency-fixture.mjs';
const source=database(),DB=await localD1(source.raw);try{assert.equal((await photoReceiptCases(DB,'native D1')).length,8);console.log('PASS native D1 003 photo receipt suite 8/0');}finally{await DB.close();source.raw.close();}
