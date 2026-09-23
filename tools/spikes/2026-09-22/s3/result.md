# S3 result: useful native revision binding; factory policy remains unproved

**Kestra's bounded native publication/binding mechanics passed. Complete S3
remains unpassed.** One trial on OSS v2.0.3 used native PostgreSQL-backed execution,
without Docker, model calls or additional task plugins. Four parent executions
and five child executions ran; three parents were held at a publication pause,
and the reference flow's two child branches overlapped. This is not a load test.

| Experiment | Observed result |
| --- | --- |
| Update changed child YAML containing `revision: 1` | API created revision **2**; historical revision **1** retained its original body |
| Resume old parent explicitly bound to child revision 1 after publication of v2 | Parent stayed revision 1 and returned **`child-v1`** |
| Resume old parent without an explicit child revision | Parent stayed revision 1 but returned **`child-v2`** |
| Admit parent after publishing parent v2 | Parent revision 2 returned **`child-v2`** |
| Execute native reference composition | Parallel research children, synthesis, one repair/re-review, Director pause, approved resume, synthetic delivery and **SUCCESS** |
| Validate and publish a delivery-only flow without review | Both accepted; bypass deliberately **not executed** |

The same running server handled the changed native composition without a worker
refresh. Native revisions prevented this ordinary update path from overwriting
historical child content. However, a parent revision does not freeze an unpinned
child lookup: a publisher must require explicit child revisions or resolve and
freeze that binding. These results do not establish protection against deletion,
direct database edits or other unsupported mutation paths.

[Runtime summary](evidence/runtime-summary.json) records the asserted outcomes.
[Changed-child response](evidence/attempt-child-revision-1-mutation.json) and
[retained child](evidence/retained-child-v1.json) prove the revision distinction.
Actual evaluated outputs are retained for [pinned old](evidence/pinned-v1-observed-output.json),
[unpinned old](evidence/latest-v1-observed-output.json) and
[new parent](evidence/pinned-v2-observed-output.json).
[Bypass validation](evidence/validate-review-bypass.json) and
[publication](evidence/publish-review-bypass.json) show native acceptance.
[Reference history](evidence/reference-result.json) and
[Director pause](evidence/reference-director-paused.json) retain the execution trace.

The review and delivery tasks are deterministic placeholders, not independently
authorized services or effecting delivery. The Factory Module still needs a
restricted publication policy and authoritative exact-revision acceptance/delivery
checks. Plugin implementation changes, remote capability/model/policy changes,
retry-contract mutation, dynamic destination selection, recursive dependency
closure, concurrency races and revocation were not qualified. S3 performed no
restart or storage-failure test; those belong to the separate S0/S1 evidence.

An initial HTTP/management-port collision failed before publication and was
corrected. After all executions succeeded, an output-collector KeyError was
corrected to use Kestra v2's read-only output-expression API; existing results
were inspected without rerunning the behavioral experiment. Both isolated
runtime processes were stopped afterward. See [README.md](README.md) for pins,
reproduction and archive provenance.
