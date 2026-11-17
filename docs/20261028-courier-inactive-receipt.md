# Inactive inbound links and courier receipt (stage B)

Previously courier receipt rejected a parcel whenever its retained inbound reference pointed to a cancelled or deleted plan. That reference also protects history and unsafe reconstruction; deleting it would lose that protection and could change historical arrival counts.

Receipt and current-link reads now use a live projection excluding cancelled, deleted and missing plans. The raw safety reference remains. Cancellation/soft deletion atomically freezes existing receipt and batch facts in the existing history table; old invalid or orphan references are frozen in the receipt transaction before any new receipt. Existing receipts retain their identity, owner, batch and event, so repeats do not count again. Arrival synchronization rechecks inactive status before writing quantities or milestones.

This does not permit reconstruction of plans with execution, results, stock/accounting, or outbound facts. Safe cancelled-plan replacement retains the frozen history, connects its successor, and archives only never-dispatched requirements once. No business history is deleted, no cancelled plan is revived, and ordinary courier receipt creates no extra job results or outbound orders.

## Safe rollback

Stage A was deployed and confirmed before enabling this writer:

- Commit: `81580c5b4833ccaf20823b96c3dae425fd51c25c`
- Tree: `a85c833380060afec674e3e8b88a818a23bbec2c`
- Worker: `ck-v2-api-sop-staging`
- Safe return version: `ce768250-e4f7-444b-93a9-15d793434a02`
- Successful check: https://github.com/ck91888/ck/runs/122611236605

If rollback is needed after stage B, select this version or a later compatible release. Do not select older e23/9e99 versions: they cannot correctly read overlapping historical snapshots or safely replace their retained claims. Switch only the program version; do not restore, clear or migrate business data backward. The stage-B database trigger remains after returning to A; A handles its history and audit correctly, but A's old admission rule may again block receipts for inactive links.

## Isolated validation

Synthetic API cases cover first and duplicate receipts, cancellation and soft deletion, missing historical plans, active replacement, unsafe reconstruction, ownership checks, transaction rollback/retry, and cancellation between receipt persistence and arrival synchronization.

`tests/courier-inactive-link-workerd.mjs` runs the actual staging API in workerd with native local D1 and independent concurrent HTTP requests for scanning, cancellation, and recreation. `tests/courier-compatible-return-workerd.mjs` writes stage-B data, stops the runtime, starts the exact stage-A source against the same persistent isolated D1 database, then verifies progress, failed/retried replacement, competing creation, immutable receipts and subsequent cancellation audit. Set `CK_COURIER_RETURN_ROOT` to an unmodified checkout with the stage-A tree above.

`tests/courier-inactive-link-browser.cjs` uses local authenticated office/field pages at 1365 and 390 pixels: bilingual success and duplicate feedback, lost response followed by refresh/retry, preserved scan input and batch counts. No live business record is used. Live customer acceptance remains a separate check.

Final validation: all 1349 Node tests passed; native workerd concurrency, native D1 rollback/retry, exact-A persistent-D1 return, both browser sizes, staging build and deployment dry run passed. Desktop and mobile screenshots were visually reviewed and kept outside the repository.

The inbound system-cutoff recovery fix and pre-performance-update request behavior remain. No performance candidate, document grouping, network, authentication or production configuration is included.
