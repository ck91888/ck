# 003 opt-in photo compression (local only)

Base: `44144a20fbf7c25290ee1b3b1235a90ab5d50bb3`. Branch: `feature/003-photo-compression-20261104`. This candidate is not pushed or deployed. FE09 and the prior candidate/evidence remain separate and hash protected; FE03/FE04, reset, permissions and backend changes are excluded.

## Confirmed gap and safe strategy

`uploadAssetPhoto` and `uploadArrivalPhoto` previously attached the selected File directly. The 003 document did not load the existing photo component. The initial two isolated handler tests fail because no preparation occurs; their original output is retained.

Both existing single-file entrances now offer explicit **现场照片（压缩） / 현장 사진 (압축)** and **原件/扫描件（保留） / 원본·스캔 (원본 유지)** actions. Compression is opt-in, not an inference that a JPEG must be a photo rather than a scan. An unclassified programmatic selection defaults to original. Multiple selections are explicitly rejected without uploading the first or clearing the selection; this patch does not create a new multi-file photo workflow. Existing general material/document upload entrances are unchanged.

Only explicitly selected ordinary JPEGs with matching MIME/extension and conservative header checks enter the unchanged `CKPhotoUpload.prepare`: long edge at most 2048, JPEG quality 0.86, serial decoding, no repeated quality-reduction loop, and never enlarge the encoded file. PNG/WebP/GIF/HEIC/nonimages, transparency/animation, nonstandard JPEG frames, XMP/MPF/other special metadata and appended JPEG contents remain original files. Raw files are not silently retyped or renamed. Original/scan mode never enters the encoder. This is deliberately conservative; not every file labelled JPEG is optimized.

Preparation failure falls back to the original File. Abort or changed identity/target never falls back into a write. The arrival interface retains its existing inclusive 15 MiB limit; an oversized fallback is retained for recovery and is not uploaded. Asset originals do not acquire that arrival-only restriction. A compressed photo uploads only the chosen processed version, not a second original. Source objects and historical photos are not rewritten.

The shared component gains optional preparation/hint/retry-policy callbacks, leaving its previous defaults and the inspection/compression/prepare implementation byte-identical. A capability flag prevents an old cached component from ignoring the new original-preservation policy. The existing resource allowlist already includes its JS/CSS; 003 now loads them explicitly. The hint is rendered as text, not injected HTML.

## Binding, recovery and current protocol limits

Picker context binds the record and existing UI identity. Preparation and send recheck that context; a confirmed late receipt cannot redirect/repaint another record. Confirmation updates only the current photo gallery, preserving entered receipt quantities and notes. Existing permission controls and backend validation remain authoritative and unchanged.

The legacy generic asset/arrival upload endpoint has no reliable idempotent upload receipt key. This patch does not invent a server guarantee or automatically retry it. Duplicate in-flight events are blocked by the existing photo batch. A confirmed pre-write business failure can manually retry the same prepared File. A transport/JSON/5xx/malformed receipt is **unconfirmed**: the File remains pending, resend is disabled, and the user is told to check saved photos before removing the pending entry and deciding what to do. No remote reconciliation or background write is introduced. Pause during preparation cancels it; pause after a write starts waits for its receipt and does not claim the active write was cancelled.

## Stage evidence

Local synthetic evidence root: `/workspace/scratch/ck-003-photo-evidence-20261104`. No real photos, personnel, business records, credentials or online API data are used or exported.

- Initial isolated handlers: 0 pass / 2 fail (missing preparation), retained in `baseline-node.log`.
- Expanded Node coverage: 27 pass / 0 fail, including originals, special formats, fallback, limit, repeated events, cancellation, retry, ambiguous results, identity/permission changes and late receipts.
- Complete Node: 2061 pass / 0 fail / 0 skip, exit 0 (`full-node.log`, `full-node.json`).
- Existing compression browser regression: passed, original assertions preserved (`photo-quality-browser.json`).
- Actual 003 built-page/Worker flows at 1440/390: 32 pass, exit 0 (`actual-003-controlled-receipt.log`). A zero-height empty-gallery fixture wait was corrected; the original failed run is retained.
- Four actual stored/private-downloaded asset/arrival JPEGs: 3462248 -> 403880 bytes, 2048x1536. EAN/QR 4/4 and native-pixel fine-text OCR 4/4 (`stored-003-quality.json`). These are synthetic quality observations, not guarantees for real documents.
- BLOCKER: pre-header response destruction reproducibly yields two HTTP POSTs, two attachments and two objects on both base 44144a2 and this stage (`transport-replay-analysis.json`, both diagnostic-lost-reply JSONs). The application sends once; the lower transport layer replay boundary is not instrumented further. This stage is not releasable. The controlled one-server-commit abort test only covers the application's uncertain-result guard and does not fix that native replay.
- Independent fixed-SHA review: pending the two-type service idempotence fix.

The existing historical upload manual RUNTIME, 743/1869 ms observations and prior failures remain historical unknowns; this patch does not resolve or relabel them. No claim is made about domestic real-network speed, physical mobile devices, real-world photo quality, or deployment state.
