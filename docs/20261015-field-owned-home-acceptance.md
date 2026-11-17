# Field tasks owned by the current dispatcher — 2026-10-15

Previously the field home mixed department-wide bulk work-plan tasks, native jobs accessible through live membership, and eligible handover candidates under one in-flight list. The confirmed default is now personal responsibility: only tasks owned by the currently authenticated dispatcher appear in the home list and the normal bulk/external task lists.

The field UI sends `view: mine`. `worker-v2/sop.js` applies an additional `owner_id = authenticated user id` predicate before sorting/limiting job rows and before the frontend can group batch children. Inner work-plan lists use the same owner constraint. A forged owner/worker field cannot change this identity. Membership, delegation, a shared department, office-manager visibility or an absent original owner does not make a task belong to another dispatcher's personal list. Current permissions are still checked before this additional filter.

The heading explains the transfer workflow: the current owner opens their own task and scans the recipient dispatcher's badge. Existing `CKTransferOwner` is reused, retaining badge resolution, attendance, department/authorization checks, revision checks, reason and transfer audit. A successful ordinary work-plan transfer now returns to the field home. The original task, work periods, output and ownership history remain; ownership moves, the task leaves the old owner's list and appears in the recipient's. Old-owner writes fail backend ownership checks. Batch transfer continues to move all unfinished children and the root atomically without touching its one labor clock.

Emergency takeover remains available through the existing explicit original-document/scan and self-claim flow, subject to the existing owner-away, own attendance and department checks. Candidates are not mixed into the default personal list. Existing detailed read/access and membership rules are not widened; explicit discovery paths remain available for legitimate handover. The implementation does not silently remove or cancel any task, nor add an automatic claim on opening.

## Identity and stale responses

The personal list response identifies its authenticated viewer. The homepage reader keys its content to the current user/attendance/badge, immediately clears cards when identity changes and rejects old responses. A mismatched server viewer cannot populate another user's page. Field-session identity heartbeat changes now show the existing login gate and preserve the page behind it instead of silently relabeling a loaded page with another user's identity. Scanning the intended badge uses the existing reload flow. Normal logout still clears the active lead and reloads; office authentication and slow-login deadlines remain unchanged.

## Isolated verification

All records are synthetic; SQLite is in memory and browser traffic is localhost-only. No real business task/order was used or modified.

- Four new actual Worker/API cases cover owner-only lists versus membership/shared departments and forged request identities; on-duty badge transfer, rejection of an unchecked-in recipient, unchanged work clocks and preserved audit; old-owner write rejection; explicit off-duty emergency self-claim that is absent from the default list until transfer; and whole-batch ownership transfer with one unchanged root clock.
- Homepage DOM tests cover a late previous-user response, clearing cards on the login gate, and a server-viewer mismatch before the local identity heartbeat catches a cookie switch. The focused suite passes 15 cases.
- Full regression passes 1,138 cases, zero failures or skips. Existing access, attendance, range, batch and slow-login cases remain covered.
- `tests/field-owned-home-browser.mjs` runs the real built `/001/` page and real Worker at 1440 and 390 pixels. Dispatcher A sees only A's task and B only B's, even when A works in B's task. A enters their own task, scans B's badge, supplies a reason and confirms; A's home becomes empty and B's contains both tasks, with unchanged worker rows and old-owner mutation denied. The displayed dispatcher label comes from B's resolved identity, not the badge's free-text name suffix.
- The same browser cases switch the cookie identity while A's list response is held. Old cards clear behind the login gate; the late A response is discarded. Scanning B reloads with B's heading and only B's tasks. No browser exceptions.
- Build, syntax checks and `git diff --check` pass.

During the final concurrent run an existing loading DOM check asserted its duplicate-scan message after a fixed 35ms delay. The selected order count was still one, but the queued second scan had not updated the message yet. Its helper now waits for the visible dispatch control to become ready after the scan queue; the duplicate-count/message assertions are unchanged. No loading business code was changed.

Local evidence: `/tmp/ck-field-owned-home-focused.log`, `/tmp/ck-field-owned-home-regression-final.log`, `/tmp/ck-field-owned-home-browser.log`, `/tmp/ck-field-owned-home-build.log`, `/tmp/ck-field-owned-home-evidence/` (synthetic screenshots and result JSON). Initial implementation checkpoint: `344e6973c29d656d3a638300a0dd9d1811fe1785`.

## Independent duplicate-outbound investigation

The reported four outbound orders were investigated read-only and not altered. A separate real-form local reproduction shows that two groups with one booking each create two orders; an unchanged retry after a lost committed response keeps the same request ID and two orders. Editing the range while the result remains unknown changes the create payload and rotates the page-memory intent ID, allowing a second full inbound/work-plan/two-order creation. The reproduction yields four different order IDs and two source plans. This is a confirmed failure mode, not a confirmed attribution of the user's live orders.

The complete private diagnostic is `/tmp/ck-outbound-duplicate-diagnosis.md`, machine evidence `/tmp/ck-outbound-duplicate-audit/result.json`, reproduction log `/tmp/ck-outbound-duplicate-browser-audit.log`; related existing create/receipt/reservation tests pass 37 cases. Actual attribution still needs the four live orders' source inbound/need IDs, group snapshots, request IDs and creation audit. The Library screenshot was read as metadata/text, but pixel download failed; no completed visual inspection is claimed. No authenticated live database access was available. No duplicate-order fix or semantic booking uniqueness constraint is included in this release; legitimate multiple bookings remain supported.

Release target is only PR 7 test branch `upgrade/sop-safe-rollout-20260917`, on top of attendance/range release `d09f84d4061f8c6ab8f02fb223617b9a75c74b9f`, and Worker `ck-v2-api-sop-staging`. Main, production, original orders/tasks and historical evidence remain unchanged. No schema migration/reset or deployment configuration change is required.
