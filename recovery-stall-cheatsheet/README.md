# Recovery-read stall cheat sheet

A one-page reference for the transactional snapshot recovery read
(`KafkaPartitionPersistence`) — its poll loop, the two kinds of turn
(advancing / waiting), the two exits (read complete / stall deadline), and the
four scenarios (no-wait takeover-abort, awaiting an open transaction, hanging
transaction, truncation), plus the verbatim log/error strings.

Not wired into the docs site; kept here as a standalone reference.

## Files

- `recovery-stall-cheatsheet.html` — source (self-contained; inline CSS).
- `recovery-stall-cheatsheet.pdf` — one A4-landscape page.
- `recovery-stall-cheatsheet.png` — same, rasterised.

## Regenerate

Render the HTML with headless Chromium:

```sh
chromium --headless=new --no-pdf-header-footer \
  --print-to-pdf=recovery-stall-cheatsheet.pdf recovery-stall-cheatsheet.html

chromium --headless=new --hide-scrollbars --force-device-scale-factor=2 \
  --window-size=1123,794 \
  --screenshot=recovery-stall-cheatsheet.png recovery-stall-cheatsheet.html
```
