# Recovering an uncertain inbound creation — 2026-10-15

The previous inline inbound form held a request ID only in page memory, keyed by the current payload. After the server saved an inbound plan, its work plan and two group reservations, a lost response left the form open. Changing a range changed the payload and generated another request ID, allowing another complete set of records. A real local Worker/browser reproduction confirmed two source plans and four outbound IDs. It did not establish the provenance of the user's four live orders.

This change starts from deployed PR 7 head `c246d556b3a79e8661010c97f13ff6719756989c`, preserving the attendance, natural box-mark range, personal task list, badge handover, bulk-work review and login-resilience changes already published there. The release target is only `upgrade/sop-safe-rollout-20260917` and Worker `ck-v2-api-sop-staging`.

## What the operator sees

The inline inbound form retains its draft, including the original input values, source-feedback context, cargo groups, reservations and selected File bytes. The browser stores these in IndexedDB, partitioned by authenticated account, scope and office operation context. Reopening or reloading restores the draft. Unsupported or failed durable file storage prevents sending a new file-bearing submission and leaves the selected inputs and files on screen.

Once a business create is sent, its request ID, create-intent ID and request body are frozen. The form shows a result-lookup action. Editing a DOM field, closing the form, reloading or clicking twice cannot turn an uncertain submission into a second payload. A failed lookup retains the original submission and files; it does not send a create request. Confirmed success opens the original plan for editing. Confirmed rejection unlocks the retained fields and gives the corrected attempt new IDs.

If a lookup reports that the original submission is absent or its processing lease has expired, the operator may explicitly retry the original unchanged submission. This is a separate manual action; no business-write replay runs on opening, querying or on a timer. An explicit new-plan action creates a distinct draft and keeps the previous uncertain draft recoverable. A business request already in flight may finish while the operator edits a new draft, without replacing its inputs or navigating away from it.

Raw inbound-material follow-up failures retain the original selected files. Continuing those materials uses the original saved plan, skips confirmed uploads and never replays a linked-outbound create. The current work-chain workflow continues to attach group and common work materials through its existing atomic grouped create.

## Server guarantee

Staging creates carrying `create_intent` use `ck_inbound_create_receipts`. Each unique request/intent is bound to the authenticated office actor and operation context, plus a canonical fingerprint of the business body. The server rejects a changed body or attempts to replace a protected request by stripping its intent metadata. A genuinely different create intent remains legal; multiple legitimate bookings for the same group/date are not forbidden by semantic uniqueness constraints.

`v2_inbound_plan_create_status` is an authenticated read. It returns the original committed result, an explicit rejected/not-saved result, pending processing, or unknown. It performs no business create. An unavailable or foreign receipt does not reveal another operator's result.

The receipt's success update, idempotency claim, inbound rows, work rows, both outbound rows and create audit commit in one D1 batch. A guard in the same transaction checks the active run token. A manually resumed expired request obtains a new token, so a delayed older execution cannot later commit a second bundle. If a transaction response is lost after commit, the receipt recovers the original IDs. Rejections are marked only while the execution still owns its pending token; the guard also prevents a delayed transaction from committing after that rejection.

The table is initialized lazily for staging receipt requests. It is included in the existing manager-only, staging-only archive/reset table allowlist so an explicitly requested future test reset will archive it with the related test plans. This work does not run a reset, migrate historic orders, or remove any reported live order.

## Verification and evidence

All API and browser fixtures use in-memory SQLite, a synthetic R2 map and loopback-only HTTP. Real customer data is not used.

- Seven real Worker tests cover exactly two group orders; unchanged replay and read-only recovery; changed payload and stripped metadata rejection; explicit business rejection and correction; rollback in the middle of the bundle; a lost post-commit response; concurrent double submission; an expired replay fencing a delayed execution; and office-context isolation.
- The browser acceptance uses actual built `/002/` pages and the real Worker at 1440 and 390 pixels. It checks lost committed responses, unchanged and changed inputs, query failure, close/reopen, full reload with exact same-name File contents, opening the original saved plan, definite rejection/correction, explicit identical replay after no request reached the server, distinct new plans while old drafts remain, failed durable file storage, and material-only continuation.
- Existing cargo-create/upload recovery, standalone plan-save recovery and feedback-source creation checks run against the same build. Source creation retains original unload workers, clocks, quantities and exact photo/material bytes. Late old responses cannot erase or redirect a new draft.
- DOM checks now wait for draft restoration to enable the add-work control before clicking. Their original language/action/row assertions remain unchanged.
- After an executor restart, a pre-existing attendance-management fixture failed before 09:00 KST because its supposedly completed 08:00–09:00 segment was still in the future. That one test now uses a fixed noon clock. Its 60-minute work, unchanged totals and unchanged raw-history assertions remain; attendance business code is unchanged.
- Existing personal-task/badge-handover, box-mark range and slow-login browser acceptance is rerun to check the deployed baseline behavior.

Persistent logs and release evidence are in `/workspace/ck-inbound-create-receipt-evidence-1015/`, including `regression-final.log`, browser logs, the publication manifest and uploaded file hashes. New browser screenshots and structured results are copied to its `ck-inbound-create-receipt-evidence/` subdirectory. The executor restart discarded earlier temporary logs; the tests were rerun and the current evidence copied into the workspace. The earlier duplicate reproduction is also described in `docs/20261015-field-owned-home-acceptance.md`. Backend checkpoint: `6495a3583fb02a176204ae2183fa5d71ca36fe11`; initial UI checkpoint: `7ee185ae9d339b38ccce42ae1edd4ccab5b8f0d5`.

Final results: full regression **1,145 passed, zero failures or skips**; new real-browser recovery acceptance **12 passed** (six scenarios at each width), with zero browser exceptions. Existing cargo-create resilience passes four scenarios, standalone plan-save resilience nine, feedback-source creation ten, personal-task/badge-handover four, and natural/padded box-mark ranges three. Slow-login acceptance also passes actual 22.5-second login, identity and operator-confirmation bodies under the existing 60-second deadline. The seven backend receipt cases and 17 maintenance cases pass together. Implementation/review checkpoint: `193c64748ed901fed37a923d895ea13ee0968b3e`.

## Remaining attribution limit

The live four-order report remains unmodified. Establishing its actual cause requires those orders' source inbound/work IDs, group snapshots, request IDs and creation audit. The earlier Library image could not be downloaded as pixels, and no visual inspection of that separate attachment or authenticated live-database attribution is claimed. Direct public test-site access from this executor last returned HTTP 403, so release verification uses the exact-commit Cloudflare deployment check and the isolated browser evidence.
