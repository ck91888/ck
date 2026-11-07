# FE07 / FE08 integration onto published b01cba6 — local review checkpoint

This is a local candidate for independent review. No feature branch or testing branch was pushed, no deployment was attempted, and no remote business database was read or written. Physical devices, domestic networks, and real financial/upload acceptance were not tested.

## Scope and provenance

- Published baseline: `b01cba66c9aaca5e4573727928d6b17908e429f2`.
- Original FE07: `ae53277e458e568f1dd23fee1a6327864ac7af6e`.
- Original FE08: `4a3b2a8550e50f612e1fe1f5d6022260ce766c03` (contains FE07).
- Original candidates and b01 share `6faee8b`; neither old candidate contains b01. The merge preserves b01 and all eight reviewed FE07/FE08 commits rather than resetting the testing branch or replaying older whole files.
- Local integration branch: `feature/integrate-fe07-fe08-20261104`.
- Business merge checkpoint: `cbed854520471fbd6a69ee03f626d469e158c6bc`, parents b01 and 4a3b2a8. Subsequent changes only extend integration tests and this record; business source remains identical to that checkpoint.
- Three merge conflicts were confined to `shared/attendance-ui.js`: retain the financial export instance/lifecycle while adding page state, retain the date-range Excel button alongside pagers, and retain company-label mapping while adding filter/page reset handlers.
- Six business files differ from b01: `shared/attendance-ui.js`, `003/app.js`, `003/config.js`, `003/index.html`, `worker-v2/index.js`, `worker-v2/purchase-material-picker.js`.
- FE07 renders 50 personnel/deleted rows per page and reads filter controls once per pass. Filtering, sorting/order, totals and exports still use the full filtered report. Office and field actions keep their existing permissions.
- FE08 opens a purchase request without preloading its catalog. A picker reads 50 projected active materials per page; searches preserve complete category/literal/locale matching. Pending reads are serialized/coalesced with owner tokens. Chosen materials/quantities remain across pages and failures, and stale account/form responses cannot render or select.
- The existing `v003CanRead` check remains before the new picker branch. The normal non-picker query is unchanged. No schema, index, deployment configuration, export/agency backend, upload or photo business source changed.
- Thirteen protected files (including both Wrangler configurations) compare byte-for-byte with b01; hashes are in `protected-source-and-ancestry.json`.
- Metadata ancestry checks confirm FE03 `c600105`, FE04 `625e884`, reset-disable `953f6f4`, and FE09 `71057b0` are not ancestors of this integration. No code from those branches was read or reused for this change.
- No repository `AGENTS.md` or relevant `.agents/skills/SKILL.md` was present in the workspace/checkout paths inspected at the start.

## Current verification and original failures

Full current Node suite: **2014 passed, 0 failed, 0 skipped**, exit 0, duration 661905 ms. The 118 targeted tests are included in the full suite; do not add them as new tests to that count. The build and source syntax/diff checks also passed.

Three first-run browser commands failed while the full Node suite was running; the raw logs and failed exit records remain unchanged:

1. `workforce-browser.log`: 15-second timeout waiting for the raw CSV download after management/rest/void/restore operations. A diagnostic copy added only context/page-error logging and import-path changes, retained all original assertions, and passed all four office/field × 1440/390 flows. After the full suite ended, the **unchanged original script** passed all four flows in `workforce-browser-serial.log`. No assertion or timeout was altered. The first run had no trace, so its cause is **unconfirmed**, not declared a repaired product defect or proven resource contention.
2. `purchase-picker-browser.log`: 15-second `page.goto('/003/')` load timeout. The **unchanged original script** later passed both viewports serially in `purchase-picker-browser-serial.log`. The first timeout likewise has no trace or confirmed root cause.
3. `finance-native-browser.log`: the new test's response-settlement probe incorrectly watched `CKSession.request`, whereas financial export uses direct `fetch`. This is an identified **test instrumentation defect**. The probe now watches the actual fetch/JSON settlement and waits for both the held response handler and the client request to settle; assertions are retained/expanded. The corrected test passed both viewports in `finance-native-browser-final.log`. Product code was not changed for this failure.

The first functional runner therefore ended exit 1 with **15 passing / 3 failing commands**. The serial recheck runner ended exit 0 with all three passing; this does not erase the first run. The older FE07/FE08 records and their retained failures are preserved in [FE07](20261104-workforce-filter-paging.md) and [FE08](20261104-purchase-material-picker.md). Original FE07 did not separately run native D1; the new native integration covers the office paging/financial boundary, not every workforce mutation in native D1.

## Coverage matrix on the integrated business tree

All data is synthetic and isolated. Built assets are exercised with local authenticated Worker handlers; browser dimensions are emulation, not physical-device validation. Raw logs below are relative to `/workspace/scratch/ck-p1-test-runtime/fe0708-integration/` unless an absolute path is given.

| Area | Coverage and final result | Evidence |
| --- | --- | --- |
| Full regression | 2014/0/0, exit 0 | `full.log` |
| Targeted UI/API | 118/0/0; workforce existing actions/refresh + picker API/UI + purchasing/retry | `/workspace/scratch/ck-p1-test-runtime/fe0708-integration-targeted.log` |
| Published-source differential | 52 combinations: four roles/field modes × 13 filters, including CKIK/리드인, original row HTML/order/actions, full CSV, summaries, deleted rows; no input mutation | `differential.log`, `differential.json` |
| Workforce actual pages | 1440/390 office and field; 50/50/21 pages; keyboard/focus, details, department, rest start/end, void/restore same ID/badge/time, off-page search, all 121 CSV; field 123 employees/daily, no business writes | `workforce-browser-serial.log`; original failure and diagnostic retained |
| Picker actual page | 1440/390, real 321-active catalog; 50-row paging, last-record/category/literal search; chosen quantities, close/reopen, late responses, retry and actual purchase apply; no inventory/ledger changes | `purchase-picker-browser-serial.log`; first navigation failure retained |
| New native D1 + finance integration | 121 actual daily check-ins, company CKIK/리드인/가온; page 2 still exports all 121 to raw CSV, span CSV and Excel; filtered CKIK returns 41 with 한국인력개발 label; hidden/destroyed export aborts with no late file; source facts unchanged; compatibility/public field+kiosk historical export remains 403 | `finance-native-browser-final.log` |
| New picker native D1 permissions | Six existing authorization modes, matched against original non-picker mode; anonymous/bad key remain 401 even with malformed locale (authorization precedes branch); legacy allowed keys/public-field rule unchanged; business snapshot unchanged | `picker-permissions-d1.log` |
| Picker native D1 scale/API | 10000 catalog records, 9999 active; nine queries including literal `%/_`, Unicode/locale, last record/no match/last page; exact original active order and no writes | `picker-d1.log`, `picker-d1.json` |
| Full Worker in workerd | Native D1, en-US/el/tr/zh-CN Unicode/category/literal matching and paging, seven cases | `picker-workerd.log` |
| Agency UI | 1440/390 kiosk and office: six company keys, legacy default/reprint/search, new company/correction/register/history/permissions, raw CSV and all three XLSX sheets | `agency-browser.log` |
| Company/date/return combinations | Returns off/on × shift policy off/on × 1440/390; three Excel filters each; midnight old/new company assignment, precise hours and source rows unchanged | `agency-ownership-browser.log` |
| Midnight native D1 | Returns off/on, policy off/on closed overnight export; missing checkout/finite cutoff/next shift, punches unchanged | `midnight-d1.log` |
| Date-range native D1 | 205 people beyond UI/current-page and API page, cross-year dates, 207 person-days, changed-snapshot rejection, office boundary, original punches/spans unchanged | `finance-d1.log` |
| Six-PDF actual grouped creation | Both widths, lost acknowledgements/transient upload recovery, one plan with six links/no duplicates | `six-pdf-browser.log` |
| Upload deadline | Absolute deadline across retries/backoff/queue/cancel, both widths, same request key replay, no duplicate objects | `upload-deadline-browser.log` |
| Other material upload entrances | Grouped/edit/request paths, same keys and all six attachments, loss of acknowledgement/permanent failure/manual retry | `upload-entrances-browser.log` |
| Unload photos | Both widths: re-entry/cache lifecycle, pending failures and retry, URL cleanup, different identity cannot inherit pending content | `unload-photo-browser.log` |
| Photo compression | Existing full quality/QR/barcode/upload matrix with attendance returns and auto-dispatch enabled | `photo-compression-browser.log` |
| Purchase edits and shipment replay | Existing actual-page quantity/state/rollback and idempotent shipment regressions | `purchase-edit-browser.log`, `shipment-replay-browser.log` |
| Bulk output draft | Existing PC/mobile draft/reopen/conflict/final-output and no premature-result regression | `bulk-draft-browser.log` |

All 18 functional groups have a passing completed run; the first runner's three failures above remain part of the result. Group counts and individual assertions have different granularities and must not be summed into one test count. Per-command exit/timing records are `functional-exits.json` and `serial-recheck-exits.json`.

## Performance on the current published baseline

The before/after scripts were run **serially before the full tests/browser regressions**, with the same b01 source as baseline and the integrated source as candidate. These are single local synthetic samples, not p95 estimates or remote throughput guarantees. Raw measurements retain all values and variations.

FE07 mounts/filters actual browser DOM from synthetic reports. Backend report computation, report size and the two initial report/config requests are unchanged.

| Records / viewport | b01 initial draw + paint ms | integrated ms | DOM nodes before → after |
| --- | ---: | ---: | ---: |
| 100 / 1440 | 330.0 | 2226.7 | 2326 → 1222 |
| 1000 / 1440 | 1397.6 | 304.2 | 22096 → 1222 |
| 10000 / 1440 | 17150.4 | 881.6 | 219796 → 1222 |
| 10000 / 390 field | 10667.7 | 682.8 | 313437 → 1675 |

The 100-record initial candidate sample was slower; it is preserved and no across-the-board startup improvement is claimed. At 10000, current rows/cards remain at 50 while the complete report, totals and office 10000-row export remain intact. Candidate 10000 filters painted in roughly 124–229 ms on desktop and 75–124 ms on the 390 field view (see raw JSON for exact values); per-scale filters may vary. No causal diagnosis or statistical confidence is assigned to cold-start/small-case variation.

FE08 measures actual application functions and Worker API with a stated synthetic **100 ms delay per request**:

| Catalog | b01 request-form ms / initial directory requests | integrated ms / requests | preload bytes before / candidate page+last-search bytes |
| --- | ---: | ---: | ---: |
| 100 | 255.1 / 1 | 10.8 / 0 | 62470 / 12238 |
| 1000 | 1011.5 / 5 | 13.9 / 0 | 625303 / 12241 |
| 10000 | 7786.2 / 50 | 1.3 / 0 | 6263224 / 12244 |

On 10000, opening the integrated picker then takes 220.3 ms and a last-record search 313.4 ms; two requests return 50 + 1 records. The old in-memory search was faster once its complete catalog had already loaded. On 100, first candidate picker opening (859.7 ms) was slower than the old already-preloaded picker (58.7 ms). The raw form/picker/search timings are retained; do not compare only form-open latency and claim every interaction is faster.

Native D1 10000-record API fixture: old full preload = 50 requests, 6622514 bytes, 1500000 rows read; new empty-search page = 11227 bytes, 30000 rows read, 50 projected records. Nonempty searches read **all 9999 active projected records** (20000 D1 rows read), then match in Worker code to preserve the original complete literal/category/browser-locale behavior. This is not indexed full-text search or a claim that D1 reads only 50 rows. API wall timings in this fixture overlapped functional/full checks and are observational; isolated UI timings above and row/byte counts have separate provenance.

Raw results: `performance-exits.json`, `performance-summary.json`, `fe07-{baseline,candidate}/result.json`, `fe08-{baseline,candidate}/result.json`, `picker-d1.json`.

## Reproduction / handoff

Worktree: `/workspace/scratch/ck-fe07-fe08-integration-20261104`. Shared test dependencies: `/workspace/scratch/ck-p1-test-runtime/node_modules`. Set `CODEX_PRIMARY_RUNTIME_NODE_MODULES` to that path; JSDOM node tests also use `CK_JSDOM_MODULE=/workspace/scratch/ck-p1-test-runtime/node_modules/jsdom/lib/api.js`.

- Build: `node worker-v2/build-sop-staging.mjs`.
- Full: `node --test --test-concurrency=2 tests/*.test.mjs` with the JSDOM module environment variable above.
- Differential: `node tests/workforce-paging-diff.mjs`, set `CK_FE07_BASELINE_ROOT` to a clean b01 checkout and `CK_FE07_DIFF_OUT` to a private result path. Both sides load the current company mapping.
- Native finance integration: `CK_AGENCY_NATIVE_D1=true node tests/fe0708-finance-integration-browser.mjs`.
- Native picker permission boundary: `node tests/fe0708-picker-permissions-d1.mjs`.
- Other exact functional commands/environments and raw logs are recorded in `functional-cases.json`, `functional-exits.json`, and `serial-recheck-exits.json`.
- Avoid concurrent full-suite load when diagnosing browser navigation/download timing. Do not alter assertions or timeouts to mask a failure.

An intermediate pause checkpoint retains the tracked patch and untracked test copies at `pause-checkpoint/`. It is an earlier in-progress snapshot, not the final candidate. Source/tests/logs contain only synthetic people and materials; no attached real financial exports, customer PDFs/photos, credentials or online query results are committed. No forbidden deployment-check or site/login access was retried. Final independent QA, publication authorization and future testing-branch head checks remain outstanding.
