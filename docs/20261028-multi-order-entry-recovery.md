# Multi-order dispatch entry recovery — local candidate

Baseline: exact tree of staging B `09cb97e3ed558fdc1b594ec784e60d9f2b1ad449`. No upload-recovery, document-grouping or reverted performance candidate is included. This change is prepared for the staging branch and Worker only.

## Reproduced defects

1. Create two grouped work plans, dispatch them as one single-worker batch, cancel both assignments, return through the actual field home/order menu and scan the original plan. B resolves the cancelled task as the current task. The UI follows its historical batch ID and shows the ended batch, with no new multi-order selector. The plan's process assignments are already released, and the backend candidate API still considers it eligible.
2. Fail the built `bulk-work-batch.js` request while loading the full field application. On B the ordinary first-dispatch form silently omits multi-order selection and still enables single-order submission. A fresh grouped plan instead displays `CKBulkOrderSelection is not defined` after partially building its form.

These are reproducible application defects, not proof of which condition occurred in the live session. The feature modules and unified bulk-department rules were not removed by the performance rollback.

## Minimal changes

- Resolving a grouped **plan** treats an all-terminal, cancelled fallback task as history when the plan is no longer owned by that batch. The historical task list remains available; explicitly opening the old task still returns the old record. This is a read change: it does not change plan status, cancel jobs, reopen clocks, alter group assignments, migrate execution or delete audit rows.
- Missing multi-order code blocks first-dispatch submission, displays a bilingual warning and offers a bounded retry of the exact same-origin, fingerprinted script URL already present in the built page. Success redraws the current plan; failure does not submit a different operation. Retry is user-initiated, has an eight-second deadline and does not change authentication or network configuration.
- The existing in-form selector starts expanded, states that more orders can be scanned, and explains an empty list. Candidate scrolling is bounded so a long list does not push staff controls hundreds of rows down the page. The existing latest-500 candidate limit remains; an older eligible plan can still be added by its number.

Single-operator enforcement, per-order outcomes/review, review waiting time, first-whole-order release, existing task ownership, attendance/department permissions and historical timing are unchanged. No schema or persisted server-side format changes are introduced. Existing courier-compatible return versions remain compatible; returning to B reproduces these UI defects but does not require a data restore.

## Verification

- Exact B and candidate both run the actual built applications and actual staging entry in workerd, native local D1/R2 and Workers Assets. Chromium logs in through the real badge form and clicks the home/order menus. No injected session or JavaScript navigation helper bypasses these entry points.
- Full native flows cover ordinary mixed-source plans, external-order selection, grouped plans, existing batches, returning/re-entering, grouped cancellation/re-dispatch, real restricted daily-badge login and reviewed bulk permission, resting dispatcher, no eligible peers, multiple peers, the latest-500 boundary, failed script loads and retry. Candidate runs use 390 and 1440 pixel viewports. Re-dispatch preserves all cancelled task records; no extra assignment occurs during component recovery.
- Node regression suite: 1350 passed, zero failures or skips. The new regression checks cancelled history, read-only resolution and a new legal batch without revived historical tasks.
- The existing complete batch-browser regression passes: first review releases the operator, later per-order review preserves a new job, outputs remain independent, group files remain scoped, the dashboard counts batch labor once, and single-order/multiple-worker behavior remains valid.
- Exact-B return verification restarts the deployed B tree `e74b3dc2b844ea52a29655ca1da5b464c941e008` in workerd against the same native D1/R2 data after candidate scenarios. The prior plans/batches remain readable, all ten selected business/audit tables are unchanged, and a retained object keeps its bytes. Preferred return is B commit `09cb97e3ed558fdc1b594ec784e60d9f2b1ad449`, Worker `f658030f-e6f9-4c04-b105-2e971e600ef0`; application return only, without restoring old D1/R2 data. The existing courier restriction against pre-A return versions still applies.
- Built-page screenshots of missing/recovered selectors and script errors were inspected. All screenshots, logs, generated assets, local databases and synthetic session details stay outside Git.

Commands use local dependencies excluded from the repository:

```sh
node worker-v2/build-sop-staging.mjs
CK_JSDOM_MODULE=/path/to/jsdom/lib/api.js node --test tests/*.test.mjs
CODEX_PRIMARY_RUNTIME_NODE_MODULES=/path/to/playwright/node_modules node tests/bulk-work-batch-browser.mjs
CK_MULTI_EXPECT_FIXED=1 CK_MULTI_VIEWPORT=390 CK_NATIVE_MODULES=/path/to/miniflare-and-esbuild/node_modules CK_PLAYWRIGHT_MODULES=/path/to/playwright/node_modules node tests/multi-order-entry-workerd.mjs
CK_MULTI_EXPECT_FIXED=1 CK_MULTI_VIEWPORT=1440 CK_NATIVE_MODULES=/path/to/miniflare-and-esbuild/node_modules CK_PLAYWRIGHT_MODULES=/path/to/playwright/node_modules node tests/multi-order-entry-workerd.mjs
```

Set `CK_MULTI_RETURN_B=/path/to/exact-B-checkout` on a candidate native run to verify the return against the same data. The fixture checks the B tree and rejects modified source.

For a baseline comparison, build the exact B checkout, run the same native fixture with B as the current working directory, and omit `CK_MULTI_EXPECT_FIXED`. Live-site access is restricted; this validation did not inspect or mutate real business records and does not claim live recovery.
