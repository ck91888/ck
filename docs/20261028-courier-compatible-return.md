# Courier compatibility return point (stage A)

This staging-only change prepares readers and safe plan replacement for immutable cancellation snapshots that a later stage will write. It does not release inactive links, create a snapshot trigger, or change receipt admission yet.

When historical rows coexist with a retained claim, progress uses the frozen history once. Safe replacement preserves that snapshot and resolves its successor without a primary-key collision. Existing execution, accounting, stock, outbound, and unstarted-requirement guards remain in force. Cancellation audit can attribute previously unassigned snapshot rows.

Validation: 1333 Node tests passed; local workerd D1 replacement rollback and same-request retry passed. Synthetic future-snapshot cases cover partial receipt, later receipt, old/new progress, unsafe reconstruction, concurrent replacements and single requirement archival. Staging build and deployment dry run passed. No live business records were tested or changed by these checks.

The subsequent receipt-release stage must retain this compatibility behavior. After that stage writes snapshots, rollback must select the successfully deployed stage-A Worker (record its exact version from the deployment result), or a later compatible version. Earlier versions can double-count historical progress or reject replacement with a uniqueness conflict. No database rollback or cleanup is part of this procedure.

The tree retains the inbound system-cutoff recovery fix and the pre-performance-update request behavior. Performance, document-grouping, network, authentication, and production settings are unchanged.
