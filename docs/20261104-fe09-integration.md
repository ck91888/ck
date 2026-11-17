# FE09 problem-image preview cache integration

Local base: `f994cdbdb34b81d9053318859ca603464c84386a`. Original saved FE09: `71057b0aa32e1a95fb870022fd6eb27cdc08b06c`. Local branch: `feature/integrate-fe09-cache-20261104`. This work does not authorize publication. FE03/FE04 and the reset patch are excluded.

## Scope and behavior

Only `shared/issue-images.js` changes product behavior. It is injected into the staging collaboration entrance `/002/` by the existing unchanged builder. Problem-detail originals and run attachments use the same component. The unchanged global `showLightbox` hook also forwards inbound/outbound vehicle/general detail images to its manual viewer; their eager native thumbnails are outside this cache. It does not own, clear, upload, compress, or replace pending-upload photos, material files, source attachments, or download bytes. Server authentication and attachment department checks are unchanged.

The original cache trim ignored files whose declared size was at most 262144 bytes. It therefore retained all small encodings and ignored understated metadata, even when their decoded image surfaces were large. The saved fix charges every retained URL its actual Blob bytes plus one width × height × 4 RGBA estimate, with a total limit of 64 MiB and eight owned previews. A currently displayed image stays pinned, including when it alone exceeds the estimate limit; closing it restores the limit. Recent-use sequence numbers define deterministic LRU order. Eviction preserves lightweight DOM bindings and prevents automatic observer/refetch loops; a click still reloads.

This integration additionally binds entries, asynchronous work and reuse to the current session/user object and identity fields (scope, ID, name, role, departments, operator context, attendance and badge). Known login/permission changes discard owned URLs and pending preview requests. A locked explicit session cannot open previews. Old responses cannot paint the new identity's viewer. A five-minute reuse age is checked at open/scan/trim/focus/visibility/page-show activity; expired noncurrent previews are released and a subsequent click performs the original authorized GET. This is a local cache lifetime, not an authorization grant or an immediate remote revocation signal. Existing session checks and server access remain authoritative. An already displayed picture remains pinned until close unless its identity is invalidated.

Page hide suspends observers before cleanup; page show resumes scanning. Navigation, decode errors, failures, cancellation and stale responses release only URLs created by this component. Pending-upload URLs and controls are untouched. Original preview concurrency (two), request timeout (120 s), 24 MiB image limit, raster signature checks, image source/download and permission APIs remain unchanged.

The estimate is not actual heap/GPU use, animation-frame memory, duplicate decoding surfaces, outstanding fetch/decode work, or proof of browser OOM. No real OOM was induced or claimed. No live service, real business image/data, blocked interface or reset was accessed.

## Evidence and tests

Evidence root: `/workspace/scratch/ck-fe09-integration-evidence-20261104`. Only local synthetic fixtures are used. Original FE09 source including its uncommitted browser fixture, old logs, original f994 source and its existing evidence remain separate and hash protected.

| Check | Result before final review |
| --- | --- |
| New identity/expiry repro on saved FE09 before the new guard | 0 pass, 6 fail; `identity-before.log` retained |
| Existing cache plus identity/expiry Node unit scenarios | 20 pass, 0 fail, 0 skip |
| Real Chromium cache at 1440/390 | 16 recorded cache scenarios, no JS errors |
| Real Chromium identity/late response/cancel/expiry/lock/page lifecycle at 1440/390 | 12 pass, no JS errors |
| Original real-page preview script | exit 0, original bytes/legacy MIME/HTTP and decode failures/cancel/filter/paging/zoom/mobile assertions retained |
| Original real-page attachment-creation script | exit 0, 14 entry + 8 zoom/mobile scenarios |
| Complete Node suite | 2034 pass, 0 fail, 0 skip; exit 0; 599280 ms |
| Native local workerd D1 private original/legacy aliases/permission isolation/nonmutation | 19 checks pass; exit 0 |
| Final rebuilt-source checks | 20 Node; 16 cache + 12 identity browser scenarios; actual preview; 14 entry + 8 visual creation scenarios; six-PDF grouped upload/retry; all exit 0 |
| Existing inbound/outbound shared manual lightbox | 6 pass at 1440/390; no JS errors; attachment records/bytes unchanged |
| Independent fixed-SHA review | pending |

`tests/issue-preview-cache-browser.mjs` includes the original uncommitted fixture correction: `[hidden]` must really hide its explicitly grid-styled container. The candidate path retains every budget assertion; a separately explicit baseline measurement mode never claims the unfixed baseline meets the new bound. Two original issue browser scripts add only output-directory environment overrides so historical artifacts are not overwritten. Neither change alters product behavior or assertion thresholds.

The final tiny identity-stamp extension includes `user.scope`; after the initial actual-page runs, final built-source Node/identity/cache and both actual-page scripts ran again successfully. The initial results remain separate immutable history; final-prefixed logs identify the final source checks.

Protected `f994` business/config files have a recorded byte-equality manifest in `protected-base-files.json`: uploading, compression, unload photos, company mapping, financial exports, personnel paging, purchase picker host, Worker and deployment configuration. No data migration or backend product change is part of this patch. Historical f994 upload manual RUNTIME and 743/1869 ms observations and the original first-run failures are not reclassified by FE09 testing.

## Commands

Set `CODEX_PRIMARY_RUNTIME_NODE_MODULES=/workspace/scratch/ck-p1-test-runtime/node_modules` and `CK_JSDOM_MODULE=/workspace/scratch/ck-p1-test-runtime/node_modules/jsdom/lib/api.js` in this environment.

- `node worker-v2/build-sop-staging.mjs`
- `node --test tests/issue-preview-cache.test.mjs tests/issue-preview-cache-identity.test.mjs`
- `node tests/issue-preview-cache-browser.mjs` with `CK_PREVIEW_CACHE_ARTIFACTS` set to a new local directory.
- `node tests/issue-preview-identity-browser.mjs`
- `node tests/issue-preview-browser.mjs` with `CK_ISSUE_PREVIEW_ARTIFACTS` set to a new local directory.
- `node tests/issue-create-attachments-browser.mjs` with `CK_ISSUE_CREATE_ARTIFACTS` set to a new local directory.
- `node tests/issue-preview-access-d1.mjs`
- `node tests/issue-preview-generic-browser.mjs`
- `node --test --test-concurrency=2 tests/*.test.mjs`

Full execution records include actual process exit codes. On any failed gate retain its first output, diagnose the specific mechanism, and do not retry merely to obtain a passing run. This local candidate cannot establish domestic network, physical-device, live financial or deployed-version acceptance.
