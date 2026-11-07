# Unified detail and work-plan print acceptance

Candidate built on staging commit `3a4d9d9`, combining the isolated unified detail, independent work-sheet print/compact-list and entrance smoke commits. Main and production are untouched.

Check detail chooses authority before rendering once. Current date/revision quantities and all SOP add/change/remove/move/scan/resolve/round/close/reopen actions remain authoritative. Barcode comparison, classification, pallet aggregates and latest timestamps use complete SQL scan aggregates across every log, including old versions, removed rows and abnormal attempts. Current valid counts are displayed separately and never reconstructed from historical logs. Duplicate attempts are shown separately from overflow. Missing pallet/person/time evidence reads 未记录 / 미기록. Pallet summaries expand by barcode; raw scan records are read through department-authorized pages of up to 200, independent from full aggregate totals.

Issue original data, attachments, timeline and field handling controls are contained inside the same workflow detail as current requirements/accounting/actions. The duplicate legacy status is omitted. Navigation epochs, current-view checks and cleanup stop hidden late detail reads from destroying the currently visible work-plan.

Work-plan printing records an independent per-sheet request by requirement version/content fingerprint. Unknown history reads 打印未记录. Blocked windows, rendering failures and print exceptions do not record success; uncertain responses retry the same request without opening another print window. The browser can record initiating print, including a cancelled dialog, but cannot prove paper output. Prints do not change business state, revision or dispatch. Success clears prior failed messages. Phone screen previews now fit the viewport while print media remains A4. Batch summaries exclude cancelled sheets.

List rows use three compact lines, independent work/print badges and folded provenance. Ordinary PC rows measured 129px; long content may grow. Four independent date filters, return state, current source/camera entry behavior and inbound density are preserved.

Validation: complete merged isolated suite 1057/1057 (including DOM30/30), merged actual print browser3 flows, final stale-navigation4/4 and staging-only package dry-run passed. Full scan backend tests cover20001 records beyond the old5000/20000 log caps, early pallet/timestamp evidence, bounded paging, empty evidence, permission checks and unchanged business rows. Final large check browser/native zoom acceptance and release terminal evidence are recorded separately.

Evidence directory: `/workspace/ck-unified-final-release-20261013`. All browser/API tests use localhost, synthetic memory databases and fictional identities. Physical Safari/camera/scanner/paper output and real business data were not exercised.
