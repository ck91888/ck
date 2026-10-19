# Department guidance at bulk dispatch — 2026-10-16

> Superseded: the user corrected the business rule on 2026-10-17. Collaboration work plans all use bulk execution; the department-routing conclusion below is historical and no longer applies. See [the corrected rule and acceptance](20261017-collaboration-bulk-execution.md).

The confirmed code defect is a misleading entry flow: an authorized dispatcher could scan a pending non-bulk work plan in the bulk work-order workspace, see the normal staff picker, add a worker and only then receive “此作业属于其他部门，请从对应现场入口办理”. The printed sheet was a generic warehouse work plan without its actual department. The backend already rejected the incompatible department; the new display makes this restriction clear before staff selection.

This release starts from `c4eaaba66f7fdf94f0a99533c15083e8feb8c8e0`. The reported mobile URL identifies the authorized `ck-v2-api-sop-staging.ck91888.workers.dev` alias. A `workers.dev` suffix does not establish that the user visited a different or production Worker.

## Implemented change

- The field work sheet displays the stored work-plan department. A pending non-bulk plan without an existing task shows an explanation before the staff picker, process dispatch or multi-order selector is mounted. It sends no dispatch request and starts no clock. Existing tasks and direct-forward confirmation retain their existing behavior.
- The printed work sheet displays its actual department; absent historical department values display “未记录” rather than assuming bulk.
- The inventory creation form explains that department selects the field dispatch route and is independent of shipping method.
- The backend keeps the same role, department, stored-need matching, attendance and worker checks. An authorized incompatible request now names the actual department in its error. A forged bulk department or an unauthorized department still fails. No plan or person is reclassified and no migration or live data correction is performed.

## Findings that are verified

A real Worker/API fixture uses the same authorized dispatcher for three otherwise equivalent inventory plans. `bulk` dispatch succeeds with one checked-in worker. `direct_ship` and `import` reproduce the old exact message and create no task, event or work segment. The error is separate from the worker badge's employment department.

The real office form defaults to `bulk`. At both 1440 and 390 pixels, two fictional DS-prefixed supply-chain numbers and two milk-express outbound bookings do not change that selection: the submitted and stored work-plan department remains bulk, printing shows bulk, and the original one-worker dispatch succeeds. The shipping method and supply-chain text do not automatically classify this plan as direct-ship.

## Verification

All records and identities are synthetic. SQLite runs in memory and browser requests are restricted to loopback.

- Five new Worker cases cover valid bulk inventory dispatch, readable authorized non-bulk plans with precise rejection and unchanged business rows, forged bulk submissions and missing department authorization.
- Full regression: 1,150 tests, zero failures or skips.
- Six real-browser cases cover office defaults, two-booking submission mapping, printed department, valid bulk staff dispatch and early direct-ship/import guidance at 1440 and 390 pixels. Non-bulk opening sends no dispatch request and changes no clock. Mobile pages fit the viewport, and there are no JavaScript exceptions.
- Four existing real-browser ownership/transfer cases preserve the personal default list, recipient badge handover, unchanged clocks and rejection of stale identity responses.
- Seven existing real-browser bulk-work cases preserve one-person multi-order selection, personal rest, whole-batch pause/resume, one root clock, release after the first whole-order review, later output/consumable isolation and ordinary multi-worker single orders.
- Build, syntax and `git diff --check` pass.

Persistent local evidence is under `/workspace/ck-department-1016-evidence/`: `baseline-audit.json`, `baseline-targeted.log` (80 passing existing cases), `focused.log`, `regression-final.log`, `browser.log`, `browser/result.json`, synthetic screenshots, `ownership-browser.log` and `batch-browser.log`.

## Actual-case attribution remains unverified

The parent reviewed the two supplied images and provided their contents. This executor's supported Library materialization attempts failed and left no readable local image bytes; its fallback supplied metadata/OCR only. No independent pixel inspection of those actual images is claimed.

Attempts to read `/release.json`, session identity and the reported plan from the actual staging alias were blocked by the execution network proxy with `Tunnel connection failed: 403 Forbidden`, before a site response or login challenge. The available tools expose no authenticated CK database reader. Consequently this release does not establish that the reported plan was stored as direct-ship or that it was created with an incorrect selection. The minimum missing evidence is the department displayed in that plan's authenticated office detail (or its read-only `department` field). No password, cookie, badge impersonation or permission expansion is needed to supply that fact.

Publication is restricted to `upgrade/sop-safe-rollout-20260917`, `sop-test.ck91888.cn` and `ck-v2-api-sop-staging`. Main and production remain unchanged. The confirmed entry-guidance correction must be distinguished from the still-unverified cause of the individual live plan.
