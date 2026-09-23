# Spec Chat integration

Vendored unchanged from [0xTomDaniel/spec-chat](https://github.com/0xTomDaniel/spec-chat)
at commit `5d1a2b77150d3799ea47fce4777f6109dc31ee82` (Apache-2.0; see `spec-chat.LICENSE`).
`runtime.js` comes from `docs/specs/.viz/runtime.js`; `tools/review-serve.py`
comes from upstream `tools/review-serve.py`. ECharts is not needed by this page.

The atlas uses upstream embed mode to preserve its existing presentation and
avoid injecting heading badges. Stable `data-anchor` attributes identify page
blocks. Review controls overlay the document; the sidebar does not reflow it.

For a browser that supports File System Access, open `docs/exomachina.html`,
choose **Connect review folder**, and grant the Exomachina folder (or its
`docs` folder). File mode derives the sibling `exomachina.html.review` spool
from the filename. The document content still works offline.

For annotation without a folder picker, run from the repository root:

```sh
python3 tools/review-serve.py docs 7160
```

Open `http://127.0.0.1:7160/exomachina.html`. Serve **docs**, since embed mode's
`exomachina.html.review` path is relative to this collection root. Do not use
a generic static server: Spec Chat needs the annotation API. The helper binds
loopback by default. Stop it with Ctrl+C when done.

Press bare **C**, select a page element, write a comment, then hand off the
batch. Review spools are gitignored. This integration does not start an agent
watcher; processing a handoff requires the authoring agent to resume review.
Embed mode preserves the page by disabling automatic heading badges, Git-focus
styling, and the stale-page reload banner; refresh manually after document edits.
