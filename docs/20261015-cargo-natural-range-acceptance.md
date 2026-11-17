# Natural-width carton mark ranges — 2026-10-15

The existing parser rejects `LU-1-LU-55` and `LU1-LU55` with “范围前缀和数字位数须一致”. It splits at the first dash even when the dash belongs to the mark prefix, then requires equal endpoint digit lengths for every range.

The parser now identifies complete numeric endpoints with the same prefix, preserving internal dashes, and allows natural digit growth when neither endpoint is padded. `LU-1-LU-55`, `LU1-LU55` and shortened ends expand to 55 exact marks. A lone `LU-1` remains one mark. Explicit `~`, `～`, `–` and `—` separators remain supported.

Leading zeroes remain significant: `LU-009-LU-011` yields `LU-009`, `LU-010`, `LU-011`; `LU-01` is distinct from `LU-1`. If a range mixes padded endpoint widths, a clear message asks for consistent padding. Mismatched prefixes and ambiguous multiple separators explain how to write complete endpoints with `~`.

Original range text is retained in saved group and outbound reservation data. Existing historical groups, outputs, reservations and source files are not migrated or rewritten. Frontend group/outbound forms send range text to the existing shared backend parser, so there is no separate browser-only parser to weaken or bypass. Positive quantities, reviewed output capacity, source group membership, duplicate marks, duplicate outbound subranges, version guards and expansion size limits remain enforced.

Validation used synthetic in-memory SQLite and local R2 fixtures only:

- Five new parser/normalization/actual reservation tests pass. The focused group, reservation and shipping suite passes 37 cases, including rollback after overlap, out-of-group, invalid quantity and over-capacity attempts.
- Full regression passes 1,117 tests with zero failures or skips.
- Staging build and syntax checks pass.
- `tests/cargo-natural-range-browser.mjs` exercises actual built office and field pages with real Worker handlers. All three natural/padded examples save through new inbound + work group + outbound reservation forms, retain exact original text and 55 mark identities after reopening, fit 1440/390 layouts, and find the last carton from the field page. No output capacity is fabricated before review; no browser exceptions occur.

The user-supplied Library screenshot reference was resolved and read; its extracted error matches the baseline reproduction. Pixel materialization failed in this executor, so the screenshot itself was not visually inspected. The exact failing inputs were independently reproduced against the unchanged deployed-source worktree.

Release is restricted to PR 7's `upgrade/sop-safe-rollout-20260917` test branch and `ck-v2-api-sop-staging`. This separate patch excludes the saved attendance draft. No main, production, real business records, reset or authentication changes.

Local logs: `/tmp/ck-cargo-natural-range-targeted.log`, `/tmp/ck-cargo-natural-range-regression.log`, `/tmp/ck-cargo-natural-range-browser.log`. Synthetic UI evidence: `/tmp/ck-cargo-natural-range-evidence/`.
