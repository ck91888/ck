# Inbound work requirements and order-completion reporting (staging)

This update extends the original 002 inbound form, 001 task handling and shuju dashboard. Production remains untouched.

## Acceptance scenarios

1. Create inbound with no work rows: inbound alone is saved. Later open inbound detail and add requirements.
2. Add textual work rows, no outbound: inbound and canonical work requirements are saved together.
3. Add multiple work rows and optional outbound rows inside each: the whole bundle is atomic. Invalid allocations leave no orphan inbound. Each outbound references the original requirement. Pending work blocks loading; review confirms the allocations. No second work task is created by the outbound.
4. From Work Requirements, choose existing inventory, enter the supply-chain order number and optionally add outbound rows.

Example: fictional inbound 50 cartons, three text requirements for carton marks 1–15 palletize, 16–30 loose outbound, 31–50 B2C putaway. Text is not automatically interpreted as stock movements or precise carton allocations; staff confirm scope. B2C supply-chain putaway remains in its own operational system.

Print the canonical work order from its detail. Dispatch and complete a task in original 001, report once per work order, then review. In the requirement detail upload the pallet-detail Excel and mark CS forwarding. The original file, parsed rows, version and forwarding record are retained. Uploading a replacement clears the previous forwarding acknowledgement. SKU quantities never overwrite reviewed carton counts.

## Dashboard

Current registered participants and current tasks update while the page is visible. Completed reports and reviewed rankings are separate. Each metric is split equally across distinct actual task participants, including participants who worked and left before completion. The manager receives no share unless actually participating. Different departments, operations, metrics and units are ranked separately. No per-pallet progress reporting.

The roster covers staff recorded in the execution system, not a verified attendance roster. No active record does not prove idleness. Native job results are labeled original-flow completed, not falsely labeled independently reviewed.

External WMS data is Excel-only. Automatic push/live external piece counts are not implemented. To implement import safely, obtain anonymized actual exports for each system, map task/order keys, employee IDs, action, actual quantity, unit and completion timestamp, then deduplicate by system + task + action + source-row identity. Never add imported and manually reported output for the same task; choose its authoritative quantity source. Suggested import points: after each operating batch and end of day. Display import timestamp.

## Verification

API regression covers bundled creation, allocation limits/rollback, idempotency, inventory reference requirement, pending-work loading block, equal-share totals, pending vs reviewed counts, and detail version/forwarding behavior. DOM tests cover original module initialization and both creation entry points. Browser acceptance is performed after staging deployment. No real customer files are committed to the repository.
