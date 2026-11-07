# Attendance shift accounting — 2026-10-15 acceptance

The confirmed attendance rule applies to daily workers and employees, including all historical dates. Every report is recalculated on reads; original check-in/out, work periods, actual break periods and audit history remain unchanged. There is no bulk update or historical-data migration.

## Effective start and default rest

All classification uses Korea time, independent of the browser or server timezone.

| Actual check-in | Effective start |
| --- | --- |
| 08:30–09:20 | 09:00 |
| 09:30–10:20 | 10:00 |
| 12:30–13:20 | 13:00 |
| Outside those windows | Actual timestamp |

The endpoint minute is inclusive: 09:20:59.999 matches, 09:21:00 does not. Rest target depends on actual check-in: before 12:00 is 90 minutes; 12:00 or later is 40 minutes, including arrivals outside the three windows.

Only after checkout, automatic rest is `min(available unassigned, max(0, target - actual registered rest))`. Existing recorded rest is credited first; excess actual rest is retained. Insufficient unassigned time is shown as a shortfall. The calculation never creates a negative value, reduces actual work or inserts a physical break interval. Before checkout, automatic rest is zero. Re-reading/repeating checkout cannot accumulate rest; manager correction that clears checkout removes automatic rest until the next checkout.

## Preserved evidence and boundaries

Actual work or real rest before the effective start is retained and flagged. Its occupied interval is unioned with the nominal effective interval: for 08:30–10:00 attendance and real work 08:40–09:10, all 30 work minutes remain; effective 09:00–10:00 plus 20 evidenced earlier minutes yields 80 presence minutes. This exception is visible in details, flags and CSV rather than silently losing real work.

Cross-day or invalid records remain explicitly pending verification and retain existing accounting instead of guessing another shift or quota. Conflicting work/rest or unverified open work suppress automatic rest pending verification. Existing same-day check-in/out storage is one span per person/day; it does not model separate exit/re-entry shifts. The rule does not create another person or quota to compensate for that limitation.

Physical employee admission still uses actual check-in, so a derived 09:00 effective start cannot admit work before a real 09:20 punch. Existing dashboards of actual task work still count real worker segments; default rest is not subtracted from work or production. Payroll is unchanged.

## Consistent reports and deployment scope

`worker-v2/attendance-shift-policy.js` is the shared derived calculation used by daily summary, employee summary and employee detail. `shared/attendance-ui.js` and `shared/employee-attendance.js` retain raw punch columns and real timelines, and separately show effective start, actual rest, automatic rest, target and shortfall. Both CSVs append the same nine accounting columns; original punch columns remain intact. The daily summary explains the historical read calculation and conversion of unassigned time.

`SOP_ATTENDANCE_SHIFT_ACCOUNTING=true` is enabled only in `worker-v2/wrangler.sop-staging.toml`. It requires the staging environment. There is no effective-date cutoff: every valid historical date is recalculated, and an old `SOP_ATTENDANCE_SHIFT_FROM` value cannot silently limit coverage. Production, absent/false feature flags and invalid dates keep the prior calculation. No database, authentication, production Worker or production configuration was changed.

## Isolated verification

All data was synthetic and stored in process-local SQLite memory databases. No remote business writes, resets or historical edits were performed.

- Fifteen targeted policy/API tests pass: six window boundaries, Korea timezone, before/after noon outside windows, real-rest top-up, repeated reads/checkouts, no top-up before checkout, early departure, insufficient unassigned time, overlap/conflict, pre-effective work preservation, cross-day suppression, feature gating, correction/reopen and raw employee admission guards. A real API case dated 2024-06-07 recalculates daily and employee totals even with a deliberately future legacy since-date, while source rows remain identical after reads. Employee detail matches summary.
- Full regression passes 1,132 tests with zero failures or skips, including the already released natural-width mark-range fix.
- Staging build and syntax checks pass.
- `tests/attendance-shift-browser.mjs` runs the real staging entry and built `/shuju/` pages with synthetic authenticated sessions and localhost-only requests at 1440 and 390 pixels. Raw 08:50 daily and 09:20 employee punches remain distinct from effective 09:00; both show 540 presence, 60 actual +30 automatic rest, and unchanged 420/390 work minutes. Historical 11:50 outside-window daily attendance shows 90 rest (20 actual +70 automatic); exact-noon employee attendance shows 40 rest (15 actual +25 automatic), preserving 225 real work minutes. Lists, details, CSVs and physical break timelines agree. No business rows change on viewing/export; no browser exceptions.

## Checkpoints and release

Original draft checkpoint `744c29a1948442d60939d3e163837c779e2bd610` is preserved. Confirmed-rule checkpoint `c35d132e8f48d1e573fe0de09a2fd5fca4fe3cc7` includes the separately released carton-range fix. Publication is restricted to PR 7 test branch `upgrade/sop-safe-rollout-20260917` and Worker `ck-v2-api-sop-staging`; it follows range release `8290ee874e1504b6c3ac05c78c4cd83601ac4520`. Main remains unchanged. The separate field-task visibility investigation made no business-code or permission changes.

Local evidence: `/tmp/ck-attendance-shift-final-targeted.log`, `/tmp/ck-attendance-shift-all-history-regression.log`, `/tmp/ck-attendance-shift-final-build.log`, `/tmp/ck-attendance-shift-final-browser.log`, `/tmp/ck-attendance-shift-evidence/` (synthetic screenshots, both CSVs, result JSON).
