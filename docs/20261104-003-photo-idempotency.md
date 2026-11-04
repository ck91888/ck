# Local 003 photo upload receipts

Base: reviewed `44144a20fbf7c25290ee1b3b1235a90ab5d50bb3`, plus the opt-in photo preparation checkpoint. Local-only branch `feature/003-photo-upload-idempotency-20261104`. Nothing pushed or deployed; no online data accessed.

## Confirmed risk and scoped correction

The original forced pre-header disconnection produced two server multipart POSTs, two attachments and two objects on both original 44144a2 and compression stage 1ec0e0a. One observed browser request does not prove one server write. Original evidence remains in `transport-replay-analysis.json` and the two diagnostic JSONs; controlled one-commit abort tests are distinct.

Only asset/asset_photo and material_shipment/arrival_photo from the new 003 controls opt into `003-photo-v1` on the existing upload action. Each selected item retains one UUID and one prepared File through manual recovery and transparent transport replay. New clients send an envelope whose asset + arrival_photo combination the original Worker rejects before writing, including in compatibility mode. A new Worker recognizes and normalizes the envelope before the existing authorization. It does not add a permission, role, path, key, session rule or arbitrary upload target. No fallback to the unprotected legacy wire protocol occurs.

The additive receipt table binds that UUID to the received bytes, filename, MIME, actual target/category and existing actor/office context. A versioned SHA256 chunk-manifest fingerprint uses bounded 1MiB buffers rather than loading a preserved large asset original all at once. This is not described as the raw file's SHA256. Context binding is hashed; credentials/cookies are not stored. Same ID with a different binding rejects before R2 writes. Office labels retain their existing self-entered audit meaning.

One deterministic new object key is conditionally created (R2 `etagDoesNotMatch: '*'`), verified against receipt metadata and never blindly overwritten. Final attachment publication and completed receipt are one D1 batch; arrival pending status is checked again in that transaction. Concurrent/replayed requests yield the same ID, metadata, original author/time and receipt. A completed receipt can be read after subsequent receipt completion without adding a new photo. No stock, shipment quantities, labor, original object or historical attachment is changed.

R2 and D1 do not have a distributed transaction. A failure after object creation leaves one owned object tracked by a pending receipt, which the same request can resume. It does not claim the object write rolled back, and does not delete objects during ambiguous or concurrent recovery. A pending request whose target has become ineligible may retain that tracked object; cleanup/reconciliation is outside this patch. New IDs for independent selections remain independent, even if bytes match.

Legacy clients without the explicit protocol remain compatible and retain the original non-idempotent behavior. This limitation is not hidden by rejecting every old page or rewriting old records. The new controls preserve unknown-result files and permit manual recovery only with the same retained UUID/File; they do not start background retries.

## Local evidence and current stage

Evidence directory: `/workspace/scratch/ck-003-photo-evidence-20261104` (synthetic only; no transfer).

- Stage 1 service/API test: a synthetic asset omitted the preexisting required category; original failure retained in `receipt-stage1-node.log`. Corrected only that fixture field.
- Frontend/service/API Node targeted: 30 pass / 0 fail, actual exit 0, `receipt-stage2-node.log`. Includes no automatic retry, exact same-ID/File recovery and unchanged office/field/anonymous/legacy authorization.
- Native workerd D1 SQL: eight groups pass / zero fail, exit 0, `receipt-stage1-d1.log`. Concurrent requests, content/target/actor conflicts, R2-before/after faults, D1 publication rollback and cold recovery, target-close race and unowned-object protection.
- New page against old Worker: eight combinations pass / zero fail, exit 0, `receipt-old-server.log`; no attachments or objects written.
- Native complete workerd/D1/R2: 10 pass / 0 fail, actual exit 0 (`receipt-workerd-root-fixed.log`). Initial startup failed because the generated bundle was outside the default module root; only the fixture rootPath/modulesRoot was corrected. Original failure remains in `receipt-workerd.log`.
- Actual 003 browser first run stopped at initial page.goto load (original 15s limit) before uploading; `receipt-actual-browser.log` is retained. This is not a passing upload result, nor a diagnosed product error. An isolated follow-up is pending.
- Full Node is running. Independent fixed-SHA review is pending. No release conclusion yet.

The prior stage's actual stored/private-downloaded bytes passed barcode/QR and fine-text OCR at desktop/mobile, 4/4 each (3462248 -> 403880 bytes, 2048x1536). This synthetic result does not guarantee real scan readability, domestic network speed or physical device behavior. Original 743/1869ms/manual RUNTIME and earlier failures are retained, not relabeled.
