# Decision-round distribution screening

22 September 2026. This is an artifact and license-metadata screen for the
tested topology, not a final clearance of an Exomachina release bundle.
The operator chose to keep Dagu eligible while reviewing a GPL-compliant
separate-server bundle; GPL status alone is not a selection veto.

| Candidate | Exact trial topology | Observed rights signal | Remaining release decision |
| --- | --- | --- | --- |
| Dagu Community v2.17.0 | Shipped Dagu CLI/server binary started as a separate local helper; no import or link to its experimental Go package. | The tagged [LICENSING.md](https://github.com/dagucloud/dagu/blob/v2.17.0/LICENSING.md) identifies GPL-3.0-or-later, permits commercial activity under GPL terms, and distinguishes separate CLI/server operation from embedding the Go API. The pinned archive includes `LICENSE` and `LICENSING.md`. No paid feature was used in this trial. | Specify the actual Exomachina bundle, source/notice delivery, modifications, and helper integration; review that package against GPL obligations. Do not equate the separate-process distinction with automatic permission for any proprietary distribution model. |
| Effect Workflow + Cluster v3 trial | Node helper using pinned `effect`, `@effect/cluster`, `@effect/workflow`, `@effect/sql`, `@effect/sql-sqlite-node`, and native `better-sqlite3`. | The candidate `package-lock.json` lists 62 non-root packages with license metadata: 50 MIT, 6 ISC, 3 Apache-2.0, 1 BSD-3-Clause, 1 `(MIT OR WTFPL)`, and 1 `(BSD-2-Clause OR MIT OR Apache-2.0)`; none lacks a `license` field. Direct package files were screened in the earlier trial. | Verify actual installed artifacts, chosen license alternatives, notices, and native binaries in the final installer. A lockfile license field is evidence for screening, not a complete artifact audit. |
| Strands Graph v1.57.0 | Python Strands harness package, already required by the product architecture. | Installed package metadata in the [round-two trial](../round-two/strands_graph/result.md) identified Apache-2.0. | Audit the pinned wheel and transitive packages in the final one-install bundle. This cost also applies when another outer engine runs inside the Strands harness. |

The [GNU GPL FAQ](https://www.gnu.org/licenses/gpl-faq.en.html#GPLInProprietarySystem)
distinguishes distributing a GPL program alongside another program from
combining them into one work; separate processes alone do not decide that
boundary. Exomachina's actual one-command launcher, installation layout, API
coupling, customer rights, source offer and notices therefore need review as a
whole. Until then, **GPL-compliant bundling is a working posture, not a legal
clearance or automatic exclusion**. A hosted deployment without transfer and
a customer-installed bundle are different distribution cases and must not be
collapsed into one answer.

The separate [Dagu GPL bundle review](dagu/gpl-bundle-review.md) records the
release-facing source, notice and recipient-rights obligations against the
actual copied trial bundle. That bundle contains the tagged Dagu license files
but does not yet deliver the Corresponding Source for its binary.

The Dagu archive screened locally is the v2.17.0 Darwin arm64 archive already
pinned by [its round-two report](../round-two/dagu/result.md); no new download or
release version was substituted. The Effect counts above were read from
`decision-round/effect/package-lock.json`, not inferred from a project-wide
license statement. Neither screen decides what rights apply to a future
multi-platform installer or product edition.
