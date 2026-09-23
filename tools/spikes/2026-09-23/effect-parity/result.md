# Effect Workflow parity spike, 2026-09-23

**Finding:** The earlier Effect trial's two fixed linear shapes were a trial
limit, not an established Effect graph limit. A new product-owned interpreter
over the arbitration vocabulary published and ran the visible graph and then a
fresh withheld four-branch arrangement without changing its frozen executor,
validator, A2A bridge, or dependency files. The withheld A2B check passed.
This narrows the composition evidence gap with Dagu and Temporal; it does not
establish full A1–A5 parity or settle the engine ranking.

The evaluator received and independently verified [freeze.json](freeze.json),
inventory SHA-256
`3d051010b0cccb006f1185e6982216249230efae11675635008bdef70b2c9d21`,
**before** disclosing the A2B arrangement. The five frozen files are
`helper.mjs`, `bridge.py`, `definition.py`, `package.json`, and
`package-lock.json`. `freeze.py verify` returned the same inventory digest
before and after A2B. Only authored definition templates and test drivers were
added after disclosure. The [visible observation](observed.json) and
[withheld observation](a2b-observed.json) retain bounded summaries. Their
`base` paths point to local temporary directories containing raw native Effect
SQLite state, product state, service SQLite state, logs, and materialized
packages; they are not bundled distribution artifacts.

## What ran

The visible v3 child started two separate real Strands/A2A capability harness
processes concurrently, validated their typed results at a join, synthesized
r1, obtained an independent negative Quality verdict, repaired to r2, obtained
a positive Quality verdict, committed one exact product acceptance, and
released once. A separate run of the **same v3 package** rejected r1–r3,
waited in a native Effect child workflow, and reached an authorized abort with
no acceptance or release. A v2 child already waiting retained its child ID,
definition digest, and r3 state through v3 publication and Effect helper
restart. Four negative publication cases rejected a review bypass, wrong join,
bad repair bound, and executable field before changing the catalog. The
Director decision endpoint rejected a forged actor and stale owner epoch.

The fresh withheld A2B arrangement used four **separate** capability harness
processes and named assignments: `source_alpha`, `source_beta`,
`counter_primary`, and `counter_crosscheck`. The driver paused the
`counter_primary` service until the other three assignments completed, then
resumed it. Product event insertion order showed that branch finishing last
and the typed join completing afterwards. The join retained four distinct
branch SHA-256 values, both source claim sets (four claims), and both
counterevidence objection sets (four objections). Four distinct A2A Task IDs
were observed. A typed `join.requires_scope` Boolean route ran **before**
synthesis.

The immutable v4a package digest
`434a6de88c894db01f4fdb945ce51184b91efef5bc5b51b640d0ed425c1aaa3b`
had `counter_primary=clear`, `counter_crosscheck=requires_scope`; the join's
OR was true. Run input represented the fixture's variable repair outcome, so
the **same published v4a graph** produced both r1 reject → r2 accept/release
and r1/r2/r3 reject → nested Director wait → authorized abort. The exhaustion
path recorded zero authoritative acceptances and zero releases. The separate
v4b package digest
`f56a64e82c22279e89a911aa6281179c70d4eaa7bda73f3b67dbd748dacf1379`
had both counters clear; its Boolean route led to direct r1 Quality acceptance
and release. Old v2 remained bound to its original child digest through both
publications and a helper restart. Effect's native `cluster_messages` held
eight distinct workflow entity IDs for four parents and four children.

This explicitly avoids the prior Temporal hidden-test mistake of fixing the
outcome in the authored document. `resolution_after_repairs` is bounded run
input here, simulating agent-produced repair variability; it is not a claim
that a deterministic fixture models real agent quality. The child package and
service bindings are digest-pinned in the run. The A2A services are external
black boxes from the factory's point of view. The helper does not create or
deploy their agent internals.

## Engine credit and maintained work

Effect `@effect/workflow` 0.19.1 with `@effect/cluster` 0.60.2 supplied durable
workflow identities, Activity recording, concurrent `Effect.all` execution,
native child workflow execution/wait, deferred Director wait/signal, and SQLite
backing. The product supplied document validation and publication, every graph
node's interpreter logic, A2A Task/identity checks, typed join and synthesis
calls, exact acceptance ledger, release reconciliation, and helper HTTP API.
Its Python bridge launches a subprocess for each fixture call; that boundary
is a trial implementation cost, not an Effect requirement.

Frozen source is 771 lines: 273 lines in `definition.py` (copied verbatim from
the Temporal candidate's product-owned validator), 333 lines in `helper.mjs`
(publisher, interpreter, product ledger, API and lifecycle), and 165 lines in
`bridge.py` (A2A and release delivery/lookup). If that validator were truly
shared between candidates, Effect-specific additions would be 498 lines;
shipping two independent copies would maintain all 771. The two A2B Python
probe drivers are evidence code, not runtime product code. The common Strands/A2A
services and fixture functions are counted once outside Effect. The product
SQLite ledger records acceptance, status and decision state; it is not a
second independent scheduler or graph-progress queue. Supervisor, installer,
and service manager remain unbuilt in this parity spike. The post-freeze
Director facade below adds a separately maintained product Adapter; it was not
part of the frozen A2B engine-facing inventory. That facade is 259 more
product-owned Python lines, bringing this trial's maintained runtime code to
1,030 lines if the validator is copied, or 757 Effect-specific lines if the
validator is truly shared. The two follow-on probes are further evidence code.

## Bounded post-freeze Director, uncertainty, and footprint follow-on

After the blind A2B result, a separate [Strands Director
facade](director_server.py) projected the frozen Effect parent/child run through
a real A2A factory Task. Its [probe](director_probe.py) retained the original
Task ID while an exhausted child waited at r3, then through both Director and
Effect helper restarts. The Director's durable identity survived its restart.
A stale r1 decision sent through the A2A Task was rejected; an exact current-r3
decision aborted the child and parent, completed the original Task with no
accepted artifact, and recorded zero acceptances and releases. A separate
original Task completed with the r2 accepted artifact, Quality Task ID, and
one release receipt. This was integrated **after** freeze; it must not be
counted as part of the withheld A2B pass.

The same probe put a test-network shim between the frozen bridge and the real
participating release receiver. The shim forwarded the release, waited for the
receiver to commit, and dropped the HTTP response. The frozen bridge recovered
the stable receipt by lookup. The receiver recorded one submission attempt and
one effect, while the Director's original A2A Task completed with the accepted
result. This is one A5 participating release lost-reply seam; it does not prove
opaque receiver behavior or the other A4 crash points. The
[observation](director-observed.json) retains the Task and receipt IDs and
counts; raw service and native Effect state remain at its `base` path.

The [resource probe](resource_probe.py) measured the complete local **six
process** bundle using the same macOS `footprint -f bytes --noCategories`
physical-footprint command used for the Dagu trial. It included the frozen
Effect helper, Strands Director, two Strands capability harnesses, independent
Strands Quality, and participating release receiver. The [observed
measurements](resources-observed.json) were **597,517,128 B at zero nested
waits**, **613,098,552 B at two**, and **657,073,376 B at ten**. Ten original
A2A Tasks and ten nested child waits remained live. The pinned Node dependency
directory occupied 85,052 KiB; temporary runtime state at ten waits occupied
10,092 KiB. This bundle has no production supervisor, reconciler process, or
opaque receiver, whereas Dagu's earlier nine-process baseline included those.
The host and measurement method match, but topology and workload details do
not; these numbers cannot establish a comparative idle-footprint winner.

## Limits relevant to the decision

The blind A2B test used a guarded helper HTTP Director endpoint. The separate
post-freeze A2A facade above closes that integration gap for the two exercised
paths, while adding owned code and a process. Neither stage injected the full
arbitration A4 kill matrix during in-flight A2A, post-Quality remote commit,
post-acceptance continuation, or conflicting stale owner. The frozen bridge
checks pinned `/health` identity and A2A Task metadata, but it does not compare
the Quality Task's verdict payload byte-for-byte with its action-lookup
verdict. A malicious or inconsistent lookup could therefore evade that
cross-check; this is a source-audit finding, not an injected exploit proof.
The trials also omit opaque unknown effects, all six A3 publication negatives,
concurrent decision races,
multi-host ownership, or a clean-machine distributable install. The separate
common fixture proves its receivers' deduplication and lookup, not that this
Effect integration passes those A5 fault cases. This integration uses direct
product SQLite writes around workflow execution, so crash seams between the
two stores need specific recovery proof before treating it as production-safe.

Therefore Effect is now a credible **composition and nested A2A** comparator.
The successful unseen composition and post-freeze Task/release recovery narrow
the original evidence gap materially. They do not establish that Effect beats
Temporal or Kestra: Temporal has broader tested crash recovery, and Kestra's
native declarative graph avoids this product-owned interpreter. Effect's
six-process footprint also gives no sound basis to assume a lighter complete
install. A matched A4/A5, authority, and distribution comparison is still
needed before ranking Effect below Kestra solely for graph authoring. Both
Effect and Temporal still require a maintained product-owned declarative
interpreter under these evaluated designs.

Reproduce from the Exomachina root with the pinned S2 Python environment and
installed pinned Node dependencies:

```sh
tools/spikes/2026-09-22/s2/.venv/bin/python tools/spikes/2026-09-23/effect-parity/probe.py
tools/spikes/2026-09-22/s2/.venv/bin/python tools/spikes/2026-09-23/effect-parity/withheld_probe.py
tools/spikes/2026-09-22/s2/.venv/bin/python tools/spikes/2026-09-23/effect-parity/director_probe.py
tools/spikes/2026-09-22/s2/.venv/bin/python tools/spikes/2026-09-23/effect-parity/resource_probe.py
python3 tools/spikes/2026-09-22/arbitration/common/freeze.py verify tools/spikes/2026-09-23/effect-parity/freeze.json
```
