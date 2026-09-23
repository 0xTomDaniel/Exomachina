# Decisive arbitration fixture and evidence contract

This trial compares **Dagu and Temporal first**. Kestra and Effect are
conditional comparators if the first two fail or product-owned work remains
unclear. The fixture does not choose an engine. Use the same visible behavior in
each candidate's supported authoring model and report `observed pass`,
`observed failure`, `partial`, or `not tested` per check. No candidate gets
engine credit for the common receivers' deduplication, Quality policy, or
product code. The [semantic vocabulary](VOCABULARY.md) is the predeclared
authoring surface; it is not a required shared DSL.

## A1 — visible product-shaped graph

Publish a v3 parent and pinned `verified_research` child while an old v2 run is
already waiting. V2 keeps its exact definition and full transitive child,
capability, policy, and service-revision closure. New work binds v3. Do this
without restarting/replacing worker or engine code to publish the new graph.
Retain native definition bytes, closure digest, publication result, run
bindings, and old/new worker identities.

The v3 child starts `source_evidence@1` and `counter_evidence@1` assignments
concurrently against **separate real Strands/A2A harness processes**. Retain
started/completed times and A2A Task/action IDs. Delay or reorder a branch so
the engine cannot advance the join early. Validate the two distinct result
types and exact digests; a malformed or wrong-type branch does not join.
Synthesize a versioned artifact with both claims and objections.

The success case has a digest-valid r1 with unresolved objections. Independent
Quality returns a negative verdict bound to r1. One repair creates r2, Quality
returns a positive verdict bound to r2, the product authoritatively accepts
**only current r2**, then releases it once. The parent receives the pinned
child's public accepted result and release receipt, without depending on the
child's internal nodes. The exhaustion case rejects r1, r2, and r3 after two
repairs. The child persists a Director wait; the parent waits for its child.
After a current-revision, authorized Director **abort**, child and parent end
`aborted` with zero authoritative acceptances and zero releases. Record the
repair count and wait/decision history. Expired, stale, or unauthorized
Director commands cannot change this result.

The same customized Strands harness package hosts capability and Quality
instances with distinct durable identities. The Director is also a Strands
harness process and exposes a factory A2A Task. `quality_server.py` is a
deterministic policy fixture, not a real model-quality or provider proof. Its
negative result is a valid artifact with unresolved counterevidence, so the
repair path is genuine. A Quality response is an **observation**; it does not
itself commit product acceptance. The accepted candidate must match the
current run, definition, revision, digest, attempt, authorized Quality service
identity, and policy. Verify that identity from the pinned service/A2A Task,
not a caller-supplied `reviewer` string. If Temporal uses Update, its validator
must enforce the same authorization/current-revision check before recording.

## A2 — freeze, then unseen valid composition

Before the evaluator reveals the additional valid graph arrangement, capture
the implementation/source inventory with `freeze.py capture` and hand the
manifest and its SHA-256 to the evaluator. The evaluator records receipt of
that manifest **before** revealing the input. A timestamp in a local file
alone does not prove ordering. Implementers may then publish a new native
definition using only the already declared vocabulary. They must not modify
the frozen validator/executor/reconciler/supervisor code or install a new
worker version. Verify the inventory with `freeze.py verify`. Do not put the
unseen input, its seed, or an anticipated hard-coded shape in this repo before
the freeze. Record the new native document, validation result, run trace, and
actual code inventory comparison.

## A3 — publication and authority negatives

Submit six candidate definitions separately. Each must be rejected before it
can admit a run, with an actionable reason, and leave the previously published
catalog/closure unchanged:

| Negative | Required rejection |
| --- | --- |
| Review bypass | A path to release or public success without current independent Quality and authoritative acceptance. |
| Wrong join type | A branch of the wrong declared result type, missing predecessor, or type-invalid route into synthesis. |
| Bad repair bound | A zero/negative, missing, non-integer, or greater-than-two repair cap, or any unbounded cycle. |
| Mutable active closure | Overwrite/rebind an active version or use a `latest` child/capability reference that can change under a waiting run. |
| Unapproved capability | An unknown identity/contract/revision or endpoint outside the approved binding. |
| Unsafe arbitrary code | Executable expression, shell command, unrestricted HTTP destination, or arbitrary dynamic program in a published definition. |

At runtime, reject a forged Quality identity and a stale r1 approval after r2
becomes current. A rejected command cannot advance the engine or count as an
acceptance. Preserve the submitted native definition/command, diagnostic,
catalog digest before/after, and run state.

## A4 — autonomous recovery and owner fencing

Inject **separate** failures at (1) an in-flight A2A assignment, (2) a Quality
verdict committed remotely before the engine records it, (3) authoritative
acceptance committed before engine continuation acknowledgement, (4) repair
or nested Director wait, and (5) a conflicting stale owner after replacement.
For the mid-execution engine kill, stop Dagu while a command runs; stop
Temporal Server **and PostgreSQL** while an Activity runs. Other candidates
use equivalent engine/store kills. Restart through the candidate's normal
supervisor and reconcile autonomously. The driver may inject faults, inspect,
and submit the specified authorized Director decision; it may not manually
retry, flush, resume, or repair a run after a kill. Frozen product reconcilers
and configured native retries are allowed. Keep the trial within the declared
factory lifetime and use a retry window long enough for the intended recovery.

Show original factory A2A Task and native run identity, retained definition
closure, restored waits, remote action lookup, one current acceptance, and one
logical release. A stale owner cannot commit a conflicting transition. A
receiver-side duplicate effect count of one does **not** prove the two owners
issued only one attempt; report both counts.

## A5 — uncertainty, counts, and owned work

On a participating A2A receiver, commit an assignment then drop its reply.
Persist intent before sending, restart the receiver, discover the action by
stable ID and binding, then continue without a second logical effect. On the
participating release receiver, similarly reconcile a lost release reply by
stable release ID. The fixture-specific action/receipt lookups are not general
A2A guarantees. The separate opaque HTTP receiver has no discovery or
deduplication promise. After its commit/lost reply, keep the submission in a
**durable unresolved** state across restart and do not blindly resubmit. This
is not an ordinary workflow failure or proof that the remote effect did not
happen.

Report separate counters for remote submission attempts, receiver effects,
negative and positive Quality verdicts, authoritative product acceptances,
release submission attempts, and release effects/receipts. The old decision
fixture's `accepted_count` counts a receiver action, including a rejected
Quality verdict; it cannot stand in for product acceptance. Preserve raw
receiver records, A2A Tasks, native engine events, and the product's acceptance
evidence. State whether acceptance lives in native history or product storage;
no external acceptance ledger is mandated.

Count maintained **product-owned** code separately as validator/publisher,
executor/interpreter, A2A/delivery reconciler, and supervisor/lifecycle.
Identify shared code only once and report candidate-specific additions. Also
identify every independently maintained durable scheduler or queue. Record
source/lock/runtime versions, exact packaged binaries and licenses, processes,
stores, ports, warm/active whole-install resources under a matched workload,
startup/shutdown owner, and clean-machine/distribution gaps. A code count alone
does not rank reliability or ownership cost.

## Shared service use

Run `service_probe.py` with the pinned S2 Python environment to prove the
common services independently of a candidate engine. It launches two real
capability harnesses, independent Quality, and participating/opaque release
receivers, exercises branch overlap, negative/positive Quality, lost replies,
and deduplication. Its `fixture_only` result is **not** candidate success.
`fixture.py` makes stable commands and validates typed results; the A2A wire
client lives in `decision-round/common/client.py`. `release_server.py` and
`receiver_client.py` provide the release test contract. The `oracle.py`
`verify-service` command checks retained common evidence. Candidate implementers
may export normalized run evidence for the oracle's run checks; native history
and logs remain the authority for timing and recovery claims.
