# Original multi-order dispatch: first whole-order review cutoff

Target: the existing `upgrade/sop-safe-rollout-20260917` test branch, `ck-v2-api-sop-staging` and `https://sop-test.ck91888.cn`. The starting deployed head is `e41f8e98ba274e7db0f210df09df6f2c761026da`; its login resilience code is retained. Original checkpoint `8c46ebe4eaf3b013c3fceb5609faede8cc3171fc` remains preserved on its original local branch. No production deployment, main change, real-record acceptance test or reset operation is part of this work.

## Resulting behavior

The original need and external-order dispatch forms can select several orders inline. There is no separate multi-order entry or mode. Selecting several orders permits exactly one actual operator. Selecting one order retains the original multiple-worker flow. The dispatcher scans staff, enters results and performs reviews; the operator does not need to log in or touch the device.

All selected orders operate under one actual batch labor timeline. Opening a different order only changes the detail being viewed. Waiting for review counts toward batch labor; explicit pause and personal rest do not. The first whole order to pass review closes the batch labor clock and releases the operator in the same database transaction. Each remaining order can still receive its own results and review. Those later actions neither extend the old batch clock nor close, resume or alter the operator's new task.

For a grouped order, partial group approval and intermediate process approval do not qualify as whole-order completion. All groups and all required processes must be approved. Ready subsequent processes stay in the same batch without additional worker clocks. Final whole-order group approval releases the operator immediately; later entry of process consumables/task closeout does not extend labor. A returned review remains correctable within the original child task, without scanning the operator again.

Each order retains its own documents, range, outputs and history. The batch screen and management dashboard show actual labor duration, operator, member-order status, reported outcomes and reviewed outcomes. Quantities aggregate only within the same metric and unit. Intermediate grouped process quantities are not added to final cargo output a second time. A later-day remaining-order result brings its batch onto that day's dashboard with zero additional batch time and the original cumulative duration.

## Atomicity and historical compatibility

New batches carry `timing_mode: batch_labor`. Their sole actual worker clock belongs to a `bulk_batch` root; child tasks never receive worker segments. Database guards reject a second live root worker and all child clock inserts. Ordinary and grouped review changes claim one `ck_bulk_labor_finishes` row per batch. Its insert closes only that root's segments, records the cutoff source/time and a system audit event, then marks root labor complete. Concurrent or repeated reviews cannot close the root twice. Failure later in the result transaction rolls the cutoff, clock closure and audit back together.

Batch pause/resume, personal rest, replacement and dispatcher transfer use the existing entry and attendance authorization. Once labor is complete, resume/replacement cannot recreate it; returning from rest cannot reattach the old batch. Legacy generic finish/leave/resume routes cannot bypass batch review. Cancelling every assignment releases labor without fabricating reviewed outcomes. A cancelled grouped source can subsequently use ordinary single-order dispatch without joining the old batch.

Historical batches without the new timing mode retain their original child clocks and switching behavior. No existing labor segments are redistributed or rewritten. The two batch bookkeeping tables are included in the existing authorized staging reset archive allowlist; this release does not invoke reset or clear data.

## Validation evidence

Validation uses the actual local Worker, isolated in-memory SQLite, synthetic R2 content, generated application pages and Chromium. Real business data and user screenshots are not copied into the repository.

Final verification: all 1,112 Node regression tests passed with zero failures and skips. The 23 batch cases plus 17 staging-maintenance cases passed together. The original-flow browser acceptance, plan-save recovery browser regression, dispatcher-personal-work browser regression and the retained login-resilience browser cases all passed. Script syntax, staging build and whitespace checks passed. The release marker is `20261015-bulk-first-review`.

- `tests/bulk-work-batch.test.mjs`: original multi-order creation; worker/source/quantity guards; review wait; concurrent and repeated first reviews; rollback after cutoff; corrections; pause/rest/replacement/signout; return from rest after release; native external results; cancellation; grouped multi-stage whole-order cutoff; exact controlled labor; resume versus review race; next-day outcomes; original one-order/multiple-worker flow; historical switching/races; explicit dispatcher transfer and off-duty claim.
- The controlled timing case records 26 total batch minutes: 24 for the initial operator and 2 for the explicit replacement. Seven minutes of rest and four minutes of pause are excluded. Eight later minutes before the remaining review add no batch labor. A separate case includes 5 minutes of review waiting in a 15-minute cutoff.
- `tests/bulk-work-batch-browser.mjs`: the original need, external and grouped-process dispatch forms; multiselect and duplicate scans; rejecting a second operator; duplicate submit; independent draft restoration; first-review release; a new concurrent task surviving all remaining results; original single-order multiple-worker dispatch; separate attachments; PC/mobile batch and dashboard views.
- Synthetic browser images are in `/tmp/ck-bulk-first-review-evidence/`, including `original-multiselect-390.png`, `original-multiselect-1440.png`, `released-results-390.png`, `dashboard-1440.png` and `dashboard-390.png`.
- Final logs: `/tmp/ck-bulk-first-review-final-targeted.log`, `/tmp/ck-bulk-first-review-browser-publish.log`, `/tmp/ck-bulk-first-review-regression-approved.log`, `/tmp/ck-bulk-first-review-build.log`, `/tmp/ck-bulk-plan-save-browser.log`, `/tmp/ck-bulk-dispatcher-warehouse-browser.log`, `/tmp/ck-bulk-office-login-browser.log`. Deployment evidence is reported with the exact published SHA.

## Remaining external limitation

The executor previously received outbound-proxy `CONNECT 403` when contacting the test domain. It is not bypassed. Exact-SHA Cloudflare deployment evidence is checked through GitHub. Regional response-delivery latency remains a separate investigation awaiting the user's hotspot comparison; this feature does not claim to fix that network issue.
