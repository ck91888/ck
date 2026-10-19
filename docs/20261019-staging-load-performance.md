# Staging loading investigation, 2026-10-19

Scope: test branch `upgrade/sop-safe-rollout-20260917`, Worker `ck-v2-api-sop-staging`, existing test domain `sop-test.ck91888.cn`. Baseline `a65427135fab9843b9f8549172220a44b2d54eec`. No production deployment, main merge, DNS/security/infrastructure change, database migration, or live business-data change is included.

## Evidence and boundaries

All new measurements below use local Chromium, the actual staging entry/Worker code, and an in-memory SQLite D1 adapter with synthetic records. They are **not mainland-China measurements** and do not reproduce its network route, HTTP/2/3, edge compression or D1 location. Request counts, gzip bytes and injected-latency results are separate from observed user timings.

The [earlier investigation](20261015-office-login-connectivity-investigation.md) records an old user screenshot: an unidentified approximately 3.6 KB API response took 22.34 s, including 5.11 s waiting and 17.23 s downloading. Small static scripts were also slow in another capture. These are historical user observations, not this release's before/after test. They do not identify the carrier, DNS, TLS, Cloudflare, browser or SQL as the root cause. The executor's earlier CONNECT 403 was an outbound access restriction; it was not bypassed and is not evidence of website failure.

| Layer | Verified evidence | Interpretation / remaining uncertainty |
| --- | --- | --- |
| DNS, TCP, TLS, mainland delivery | No new mainland trace or live runtime logs available | Cannot attribute the regional issue to a provider or infrastructure layer |
| Initial HTML/assets | Built login had 26 JS/CSS URLs; collaboration 59; dashboard 59. Collaboration source assets total 2,512,172 bytes; dashboard 2,268,413 bytes | These are uncompressed source sizes, not actual internet transfer sizes. All startup assets are same-origin |
| Large startup libraries | Excel library 944,491 bytes; scanner library 375,554 bytes. Loaded synchronously on full application pages | Potential next optimization, deferred because import/export/camera call sites need a separate compatibility pass |
| Caching | Existing hashed JS/CSS already use `public, max-age=31536000, immutable`. HTML and private data are not given public caching | Warm-browser test reused static assets. Two existing unhashed dynamic modules still transfer 2,220 raw bytes on navigation; not the principal large-resource problem |
| Authentication | Existing warm identity regression uses three DB calls with no repeated DDL. The 60-second authentication deadline covers slow bodies | Schema/session code already had success-scoped initialization caches; no new session cache or automatic write retries |
| Frontend waterfall | Actual synthetic manager dashboard sent `v2_dashboard_realtime_overview` twice on first entry | `showApp()` started it before the attendance wrapper was installed; setup called it again |
| Live task API | 100 active jobs caused 100 sequential worker-name calls, plus job and document-number reads: 102 D1 calls total | Remote wait amplification is proven in code and synthetic measurement; actual D1 latency is unknown |
| Query plans | With 10,000 synthetic inbound plans: date ranges use date indexes; unbounded and `%keyword%` COUNT scans the table; list sorting can use a temporary B-tree | Local complete requests measured 7–16 ms, not evidence of real D1 latency. No speculative index/schema or search-semantic changes |

The inbound audit returned 200 rows for the unfiltered and date-filtered requests, using 21 adapter calls including existing chunked enrichments; the `%999%` example returned 19 rows using nine calls. Existing list limits and pagination UI remain unchanged. The six sequential overview statistics and the complete attendance summary used for two counts are further candidates, not changed here.

## Minimal changes

1. Build the 17 shared stylesheets into one `staging-common.css`, preserving every stylesheet's exact bytes and cascade order. Reject future CSS imports/relative resources until their rewriting is handled explicitly. Keep the existing content hash and public static caching behavior.
2. Login keeps only browser compatibility, rollout configuration and the existing session script. Its appearance still uses the exact same shared styles; it no longer loads six unused business feature scripts. Full application feature scripts, Excel, printing and scanning remain available.
3. Install the dashboard attendance wrapper before calling `showApp()`, eliminating the duplicate initial overview call. Later manual/timed refresh still calls the overview and attendance summary; role gating is preserved.
4. Submit the bounded, original worker-name SQL statements in one D1 `batch`. This preserves DISTINCT handling, name order, blank-name filtering and departed-person exclusion. **100 SQL statements still execute; the optimization reduces remote calls, not SQL count.** Empty lists skip the batch. Next reads still see updated crew; no business response cache is introduced. D1 returns batch results in statement order ([official API documentation](https://developers.cloudflare.com/d1/worker-api/d1-database/#batch)).
5. Add `Server-Timing: app;dur=...` to staging API/file responses and explicitly mark them `private, no-store`. Only elapsed time until the application returns a Response is exposed; no payload, identity, credentials, SQL or tracing storage. Static caching and authentication redirects are preserved. The metric excludes DNS, TLS, client network transit and body download ([header reference](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Server-Timing)).

## Synthetic before/after

| Measurement | Before | After |
| --- | ---: | ---: |
| Cold login static requests | 26 | 4 |
| Cold login raw static bytes | 218,411 | 171,246 |
| Cold login locally gzipped bytes | 65,189 | 35,398 |
| Initial login gate median, 3 runs, fixed 150 ms per local response | 1,007 ms | 446 ms initial; 551 ms final verification |
| Login → collaboration cumulative static requests, including dynamic modules | 61 | 45 |
| First manager-dashboard overview calls | 2 | 1 |
| Live-task API D1 calls, 100 jobs | 102 | 3 |
| Worker-name SQL statements, 100 jobs | 100 | 100 |
| Live-task API, synthetic 20 ms per D1 call, 100 jobs | 2,080 ms | 69 ms |
| Live-task response bytes, same 100-job fixture | 61,370 | 61,370 |

The latency examples isolate request/call scheduling, not real bandwidth or mainland performance. The initial login comparison ran before the CPU-heavy regression suite; the final verification after that suite measured 551 ms, illustrating local timing variance. Concurrent regression measurements were intentionally not used to claim an improvement. CSS bundling lowers request count with only a small total-app byte reduction. It does not remove the large libraries from full application startup.

## Validation and reproduction

- `node worker-v2/build-sop-staging.mjs`, JavaScript syntax and `git diff --check`.
- `CK_JSDOM_MODULE=<temporary-installed-jsdom>/lib/api.js node --test tests/*.test.mjs`: **1,164 passed, zero failures or skips**. The initial run exposed missing environment-only jsdom; it was installed under `/tmp` and the complete suite rerun. No dependency or generated assets are committed.
- `node tests/dashboard-load-performance.test.mjs`: 0/1/100/105 tasks, original name query equivalence, excluded completed jobs, unchanged limit, no warm writes, fresh subsequent reads. Use `CK_PERF_DB_DELAY_MS=20` and optional `CK_PERF_REPORT=/tmp/report.json` for the latency experiment; `CK_PERF_REPO` selects a separate baseline checkout.
- `node tests/staging-load-browser.mjs`: actual staging session login, operator confirmation, manager dashboard, material module, warm navigation/reload, duplicate-request check, fresh manual refresh and logout. `CK_PERF_VISUAL=1` passed 1440/1280 desktop native 100/125/150% zoom and 390/320 mobile login/operator checks; all 16 screenshots were visually reviewed, with intact input and action controls and no new horizontal clipping. Evidence stays under `/tmp`.
- `node tests/office-login-resilience-browser.mjs`: genuine wrong-password rejection, stalled/malformed response, unknown committed login recovery, stale identity race, actual 22.5-second slow login/identity/operator response, no automatic mutation retry and no business rows created.
- `node tests/work-execution-visual-browser.mjs`: actual built office list/editor and field dispatch, long synthetic bilingual content, all eight native viewport/zoom combinations; 24 screenshots generated and visually reviewed. Existing responsive rows, bilingual labels, editor footer and field crew controls fit without new clipping/overlap. Stylesheet content equality and resource hashes have dedicated regression checks. Baseline/optimized login screenshots at 1440 and 390 pixels were pixel-identical.

Release marker: `20261019-staging-load`. This document describes the tested candidate; release completion must be verified using the exact commit's `Workers Builds: ck-v2-api-sop-staging` successful check and version ID. Do not infer live deployment from this document or a local build.

## Minimum real-client check after release

On one affected mainland computer, retain the normal browser/session and record the full page URL and whether opening, navigation or a particular query is slow. Reload normally once, then repeat once to observe warm-cache behavior; do not enable “Disable cache” for this comparison. In Network, record the slow request's action **name only**, total time, Waiting/TTFB, Content Download and the new `Server-Timing` application duration. Screenshots may show only the Timing panel/header value with personal information cropped; no password, cookie, payload or HAR export is needed.

If application duration is short but Waiting/download remains long, investigate the client/delivery path. If application duration dominates, identify that action and inspect its measured server/database work. A phone-hotspot comparison on the same computer/browser can distinguish an access-network contribution; it does not establish which upstream provider is responsible. Until this evidence exists, the domestic-loading root cause and real speedup remain unverified.
