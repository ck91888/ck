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
