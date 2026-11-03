# 2026-11-03 audit P1 transaction fixes

Baseline: reviewed staging commit `42d76a669c4b3f5166251af5a931aa1163415897`.
Work branch: `feature/audit-p1-transactions-20261103`. Synthetic local data only;
no staging deployment, main change, historical correction, or live database writes.

## Stage 1: material transaction replay

The same request can pass the old receipt lookup twice. If one request waits before
its stock transaction and a retry commits first, the older request retries against
the newer stock version and deducts again. Baseline regression expects 7 from
opening 10 / issue 3 but observes 4.

Material stock, ledger and the durable idempotency receipt now share one D1 batch.
A conflicting receipt aborts that batch and the loser returns the committed receipt.
CAS failures recheck the receipt before retrying. Receipt persistence failures roll
back stock and ledger instead of being ignored. Existing receipt rows and business
history remain intact; no schema or data migration is needed. Other generic
`withIdem` call sites are outside this stage.

Validation:

- `node --test tests/material-txn-atomic.test.mjs`: 11 passed, 0 failed, 0 skipped.
- Same file with `CK_P1_NATIVE_D1=true` and a local Miniflare/workerd runtime:
  11 passed, 0 failed, 0 skipped. No Cloudflare account or remote D1 is used.
- Related material/asset audit, purchase flow and purchase retry regressions:
  initial stage run 30 passed, 0 failed, 0 skipped (included the first 4 new tests).
- Covers held concurrent requests, distinct requests, issue/use/return/inbound/
  adjustment/stocktake, stock and receipt failures, lost-response replay and
  request keys from other actions.

Stage 2, ownership transfer versus completion, is still in progress. This stage
is not a release candidate. Full regression and independent fixed QA remain due.

## Stage 2: completion ownership and permission races

Confirmed baseline races through authenticated Worker APIs, also reproduced on
local native workerd D1: an inbound tally, outbound loading trip or unloading
trip can wait before its result batch, transfer to B, reject a fresh request from
A, yet still let A's older request publish the result and close the crew. Native
external bulk and multi-order external bulk have the same gap. Ordinary SOP
completion and grouped execution already reject the stale revision and remain
regression controls.

The native validation now retains server-owned record revision and identity
evidence. The completion batch checks that revision and live original
owner/member/share rules before publishing results, closing workers or changing
linked plans/orders. The existing collaborator guard keeps its specific error
and valid collaborators, managers and independently authorized paths keep their
original rules. Field attendance/grant evidence uses the existing versioned
transaction guard. No grant is accepted from request fields.

The guard covers atomic inbound/generic native completion, bulk completion,
pick/outbound completion, truck loading and unloading. Manual finalization now
puts its result and job closure in one guarded batch as well. A stable native
result claim still reconciles duplicate successful completions; manual
finalization additionally compares the validated job status. The only additive
schema is an empty transient CHECK table; checks insert/delete within the same
transaction, without changing historical records or labor.

Stage checks at this checkpoint:

- Material concurrent request suite, expanded with both first transactions held
  and released together: Node SQLite 12/0/0; local native D1 12/0/0.
- Completion ownership suite: Node SQLite 16/0/0; local native D1 16/0/0. Includes
  ordinary/external/group/multi-order, owner ABA, manager, manual, shared rights
  and exit, lost-response duplicate completion, concurrent pause, field rest ABA,
  injected rollback, and all three logistics paths with complete business
  snapshots and a successful completion by B after rejection of A.
- Related initial regressions: 76 passed, 0 failed, 1 optional DOM test skipped
  without the jsdom runtime setting. Full runs below enable that runtime.
- First full run: 1510 passed / 34 failed / 0 skipped. 32 failures were missing
  generated assets; 2 exposed an overly broad status comparison on already
  committed completion retries. Both causes were corrected without weakening
  assertions. The constructed build then passed 1551/0/0.
- A final full run including the last added field-state regression is due after
  this checkpoint; the final development report supplies its exact result and
  reviewed candidate SHA. This checkpoint is not deployment authorization.

Deferred to the next batches: audit P2 reset cleanup of orphan return/dispatch
state, 04:00 pending-review report discrepancy, and material deletion versus new
reference races. No implementation of those issues is included here. No photo,
frontend, attendance accounting policy, deployment configuration or real
attendance correction is changed in this batch.

## Stage 3: fixed QA shared-authorization ABA blocker

Fixed QA found that leaving a collaboration and accepting a new pair ID could
reauthorize an old in-flight completion. The first successful share lookup now
retains its relationship ID in server-owned request evidence. Both the native
completion guard and the existing shared transaction guard require that exact
pair to remain active. Ordinary/grouped/multi-order paths use the same bound
evidence. Original owner/manager/member/delegate/reviewer permissions remain
independent; no client fields supply grant evidence. No schema changes.

Before: four external/ordinary/group/multi-order ABA scenarios failed. After:
these four pass; fresh actions under the new pair still succeed. Node sharing,
draft access and completion regressions: 44/0/0. Native local D1 replacement-pair
and active-shared controls: 6/0/0. Existing race injection hooks and assertions
remain intact. The material content-binding and manual-replay QA blockers are
still being implemented at this checkpoint; no release is authorized.
