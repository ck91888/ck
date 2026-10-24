# Mixed shipping packages after reviewed work

Scope: test branch `upgrade/sop-safe-rollout-20260917`, Worker `ck-v2-api-sop-staging`, test site `sop-test.ck91888.cn`. Baseline verified as `c6b9462ba7f980509cb6a4f8c17ac20dbe68ea93`. No production deployment, main merge, new credentials, migration, bulk data rewrite, live document operation, pagination/report change or attachment-drag change.

## Existing entry and cause

- Work-plan detail already offers **安排出库** and **关联历史出库计划**. **调整出库数量与单位** is for unfinished reservations and is hidden/rejected once a reviewed result exists.
- Dashboard `v2_ops_job_result_update` appends legacy `manual_correction` records; the SOP guard rejects that route for reviewed SOP jobs. It cannot safely synchronize this work-plan shipping basis.
- Whole-job allocation used one `result.quantity` and `result.unit` and summed every link into that number. A fully allocated pallet result has no remaining scalar capacity for separately verified cartons. Work metrics such as packed boxes and consumed small cartons are not independent shipping packages.
- Grouped and multiple-process plans already have their own output, mapping-version and allocation workflow. This change does not route them through whole-job correction or convert their existing data.

## Corrected workflow

In **协同中心 → 作业计划 → 单项作业详情 → 出库安排 → 实际出货构成**, open **申请更正出货构成**. Enter the **complete corrected composition**, for example `5 托 + 1 箱`, and the reason/evidence confirming the independent carton. Submit for review. The old approved basis remains effective while pending.

An authorized office manager/reviewer, or the original task's authorized dispatcher, reviews with a required explanation. Service role can propose but cannot approve. Dispatcher ownership/delegation and department rules follow existing task-review semantics. Existing staging office authentication uses an administrator session; this release creates no new identities or role configuration and does not enforce a different-person review rule. Field sessions cannot use these office correction actions.

After approval, **安排出库** offers the remaining unit(s) for each row. A synthetic five-pallet order already booked keeps its exact original row; the new carton can be scheduled separately. Mixed units display separately, never as six packages. The historical-link and old single-create endpoints also validate the selected unit and capacity. Historical links reject cancelled/shipped/completed orders, existing loading work and mismatched known box/pallet counts.

The additive `shipping_output` JSON contains the current reviewed composition and version. `shipping_output_change` retains the latest proposal and review. Existing `sop_events` store full immutable before/after states, actor identity/name, reasons, times and revisions for every proposal, return and approval. Original `result`, task reviews, result rows, labor rows, attachments and existing outbound snapshots are unchanged. This corrects the shipping basis, not productivity metrics. Work/consumable metrics are never inferred as extra output.

## Integrity boundaries

- Positive safe integers and known units only. Explicit synonyms such as pallet/托盘 and carton/box normalize to 托 and 箱. Repeated same-unit entries aggregate; different units are never converted.
- Each unit has total, occupied and remaining quantities. Cancelled orders release only their allocation; shipped/completed and missing referenced orders remain consumed. Shipment completion still uses the existing loading workflow.
- Proposal and approval both reject reductions below currently occupied quantity in any unit. Approval rechecks current links. Existing orders are neither updated nor cancelled by a correction.
- Every booking and correction writes the original SOP revision-guarded event in the same D1 batch as its changes. Competing requests cannot overwrite or oversell. Exact retries recover the original result, including ambiguous committed acknowledgements; changed-payload reuse of a correction request is rejected rather than rescued by the generic SOP replay catch.
- Historical linkage also claims the outbound ID through existing event uniqueness. The same batch rechecks order status/counts and active loading work, so different needs cannot concurrently claim one order and a changed or newly loading order rolls back the link. Nullable legacy timestamps remain compatible.
- Closed/cancelled, unreviewed, direct-forward, grouped and multiple-process records cannot enter this correction workflow. Their existing workflows remain in use. Initial whole-job work feedback still uses its original main quantity; mixed shipping composition is explicitly proposed/reviewed after work approval, whether or not an outbound has already been booked.

## Isolated validation

All tests use the actual Worker with in-memory SQLite and synthetic identities/documents. No live business API was called for acceptance. The private source screenshot could not be materialized in this executor; diagnosis used the supplied, already-reviewed description, not a claim of independent inspection.

- `tests/mixed-shipping.test.mjs`: nine scenarios covering five pallets linked then one extra carton; unlinked mixed composition; repeated partial shipments; cancellation release; shipped and missing-order occupancy; zero/negative/fractional/excess quantities; unit synonyms; service/viewer/department/dispatcher ownership boundaries; returned review; retry/content conflict; lost acknowledgements; concurrent proposals/bookings/review; frozen and loading historical links; cross-need claims and injected status/loading races. Original order, task, result and worker rows are compared unchanged.
- `CK_JSDOM_MODULE=/tmp/ck-test-deps/node_modules/jsdom/lib/api.js node --test tests/*.test.mjs`: 1,173 passed, zero failures/skips. Intermediate runs exposed a shared legacy form mock without `dataset` and nullable old outbound timestamps; both compatibility issues were fixed. The final targeted SOP/work-chain/mixed-shipping run passed all 39 tests.
- `node tests/mixed-shipping-browser.mjs`: real built pages, actual proposal → review → one-carton booking, retained draft on editor close/reopen and unchanged old pallet order. Eight native viewport/zoom combinations: 1440/1280 at 100/125/150%, and 390/320 at 100%. An additional 390px unlinked case rejects excess pallets while retaining both rows, then successfully books pallets and cartons separately. Records actual CSS width, DPR and screenshot path. All 41 list, proposal, review, approved, booking and unlinked-case screenshots were visually inspected and remain private under `/tmp/ck-mixed-shipping-evidence/`.
- Visual review found that the reused standalone booking form still displayed group-reservation guidance. Its whole-job context now explains per-unit allocation; group forms keep their original guidance. New controls wrap on narrow screens; existing list layout, bilingual labels, numeric inputs and review buttons remain usable without horizontal overflow.
- Staging build, syntax checks and `git diff --check`. Generated assets, local dependencies and screenshots are not committed.
- After integration on `604f6f42d525452006cb9a1e1db8ac73c558afd0`, the full suite again passed all 1,173 tests and the complete mixed-shipping browser matrix passed. The six attachment-release files match that parent byte-for-byte. Its attachment browser regression passed 14 flows and eight layouts; issue-entry and issue-cancellation browser regressions also passed. Integrated logs are `/tmp/ck-mixed-integrated-{full,browser,attachments,entry,cancel}.log`.

Release marker: `20261019-mixed-shipping`. Deployment is complete only after the exact released commit's **Workers Builds: ck-v2-api-sop-staging** check succeeds; local build success alone is not deployment evidence.

Release sequencing: integrate on the parent's verified attachment release `604f6f42d525452006cb9a1e1db8ac73c558afd0`, preserving its six files and the preceding performance release, then revalidate and fast-forward the test branch. No production release is authorized.

## Remaining onsite check

On the test site, authorized staff must confirm which packages are physically independent and review the complete composition before arranging the remaining carton. No real document was corrected on the user's behalf. Confirm the available office identity/approval workflow with the actual users and test the normal browser/network; synthetic tests do not establish physical stock truth, mainland network behavior or scanner behavior. Grouped mixed packaging continues to require the existing per-group output workflow and was not expanded in this release.
