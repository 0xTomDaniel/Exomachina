# A2A v1 baseline, factory mediation, and hand-off records

Decision adopted by the operator on 7 October 2026. Terms follow the
[project glossary](../CONTEXT.md). This record states the agreed direction and
the contracts implementation must meet; it does not claim that any part is
implemented or qualified. The
[factory dashboard integration specification](specs/factory-dashboard-integration.md)
and [`prototype/temporal-factory/INTERFACES.md`](../prototype/temporal-factory/INTERFACES.md)
carry the dashboard and wire-level details.

## Context

The prototype was built on A2A 0.3 (`a2a-sdk` 0.3.26) for rapid spiking. A2A
v1.0 is published, and the long-term goal is that any v1-compliant agent service
can take part in a factory. Separately, a real Live job showed that the floor
cannot show work moving from start to finish: Observation records each Task but
not what it produced or what the next Task consumed, so most belt items can only
be inferred from graph order.

Facts from the [A2A v1.0 specification](https://a2a-protocol.org/v1.0.0/specification/)
that shape this decision:

- A Task requires only `id` and `status`; `artifacts` is optional. Results
  SHOULD be returned as Artifacts. Messages SHOULD NOT be used to deliver task
  outputs and MUST NOT be treated as reliable delivery of critical information.
  `SendMessage` MAY return a direct Message for simple interactions.
- An Artifact requires an `artifactId` unique within its Task and at least one
  Part. A Part holds exactly one of `text`, `raw`, `url`, or `data`, with
  optional `mediaType` and `filename`.
- Streamed artifacts arrive as `TaskArtifactUpdateEvent`s with `append` and
  `lastChunk`.
- v1 renames methods (`SendMessage`, `SendStreamingMessage`), wraps responses as
  `{task}` or `{message}`, uses `TASK_STATE_*` enum values, removes the `kind`
  discriminator, and advertises `supportedInterfaces[]` with an `A2A-Version`
  header. Required extensions are rejected with `ExtensionSupportRequiredError`.

## Decisions

### 1. A2A v1.0 is the only supported protocol

Every Exomachina A2A server and client speaks v1.0 only. There is no 0.3
interface, dual advertisement, compatibility shim, or version-negotiation
fallback. Agent Cards advertise only v1 `supportedInterfaces`; clients send the
v1 version header and reject a peer that cannot serve v1.

- Observation stays version-neutral. Its own state vocabulary and event types
  do not change with the wire protocol; the A2A Adapter maps `TASK_STATE_*`
  values to Observation states at the boundary. Recordings and the S33 secrecy
  check therefore do not change with the migration itself.
- The factory's own A2A extensions (for example
  `exo-explicit-factory-binding-v1`) are declared as v1 extensions on the Agent
  Card. A required extension the peer did not activate is rejected with
  `ExtensionSupportRequiredError`.
- The migration uses a new pinned Python environment with `a2a-sdk` 1.x. The
  shared 2026-09-22 spike environment that the operator's running stack uses is
  not modified. Restarting the operator's local factory onto v1 requires the
  operator's approval at that time.
- Evidence captured on 0.3 remains historical. Basic-prototype rows that depend
  on Live submission (B-rows) are requalified on v1.

### 2. The factory mediates every exchange

Agent services never address each other. The factory is a client to every agent
service it assigns work to and an A2A server to its own caller. It is mediating
middleware, not a transparent proxy: it composes each new Task rather than
relaying bytes. Every hop passes through the same hooks:

| Hook | When | Responsibility |
| --- | --- | --- |
| Before dispatch | Before `SendMessage` to the assigned agent | Compose the Task from upstream hand-offs and the node's brief; check budget, capacity, and policy; record the consumed hand-off (decision 4). |
| On update | Each status or artifact update | Track progress and streamed artifacts; handle `TASK_STATE_INPUT_REQUIRED` and `TASK_STATE_AUTH_REQUIRED` through the Director or human escalation. |
| On complete | Terminal completed state | Normalize the output, validate it against the node's output contract (decision 3), record the produced hand-off, and record usage. |
| On failure | Failed, rejected, cancelled, or contract failure | Choose repair, retry, or a Director decision under the factory's operating rules. |

Because every piece of evidence is recorded by the factory's own hooks, a third
party agent needs nothing beyond v1 compliance to take part, and its private
content and implementation stay private.

### 3. Each node binding declares its output contract

The factory definition declares an `output` mode on each node binding. It is
validated when the definition is published.

| `output` | Meaning | Completion rule |
| --- | --- | --- |
| `artifacts` (default) | Strict. Results arrive as A2A Artifacts. | A completed Task with no artifact fails the node's contract at that node (`output.missing`). It never produces an empty hand-off. |
| `message` | Opt-in, labelled exception for agents that answer with a direct Message or only a status message. | The factory records a hand-off holding one item of source `message`. It is shown and reported as lower reliability. |
| `none` | Side-effect node (for example the HTTP release receiver). | Completion is the terminal state plus the node's own evidence, such as a receipt. The node has no outgoing material edge (decision 5). |

A hand-off therefore never travels empty. Empty sockets exist only while a
carrier is still filling inside its station.

### 4. The factory records content-free hand-off records

The factory records two facts at its own boundary. Agents never write them, and
no agent cooperation beyond v1 compliance is needed.

- **Produced** (on complete): the hand-off identity and its revision for that
  node, plus the producing run, assignment, attempt, and node. It holds one item
  per artifact, or one item for a `message` output. Each item records:
  - its source (`artifact` or `message`);
  - its part kinds (`text`, `data`, `raw`, `url`);
  - its `mediaType`;
  - its byte length (null for `url` parts);
  - its ready time;
  - a keyed digest.
- **Consumed** (before dispatch): the consuming run, assignment, attempt, and
  node, and the hand-off identities and item digests it was composed from.
- **Item ready** (only when an agent streams): one item's ready time, recorded
  when its `lastChunk` arrives, so a live carrier fills as artifacts complete.

These facts are content-free. They never include:

- text, data, or bytes;
- artifact names, descriptions, or `artifactId`s;
- filenames, URLs, or metadata.

Item digests are keyed with a per-factory secret that is never exposed. That
lets observers match an item across hops without making short, guessable outputs
recoverable from a public hash. A report artifact keeps the existing plain
sha256 chain (`artifact.revised` → `quality.verdict` → `delivery.receipt`),
because delivery receipts must be verifiable by the receiver. Its hand-off item
also references that `artifact_revision` and `artifact_sha256`.

Snapshots retain hand-off records with their times, so a retained run replays
its full path. Adding these event types widens the Observation allowlist, so S33
is requalified when they land.

### 5. Material and control edges

Definition edges declare a kind: `material` (the default) or `control`.

- A **material edge** carries a hand-off. It is the only kind of edge drawn as a
  belt with items on it.
- A **control edge** only sequences work ("then run C"). It carries no hand-off
  and is drawn as a thin line with no items.
- A side-effect node (`output: none`) may have incoming material edges but only
  outgoing control edges. When a later node needs an earlier node's output, the
  definition declares a material edge directly from the producer, so that belt
  bypasses the side-effect node.
- Route nodes' transitions to repair, Director waits, release, and terminal
  states are declared as control edges, replacing today's "undeclared" hop
  labels.
- A gate node such as Quality consumes a carrier and returns a verdict. The
  factory forwards the same hand-off downstream with the verdict attached as a
  seal; the gate does not mint a new work product.

### 6. Floor presentation: carriers and gems

The belt item is the factory's hand-off, called the **carrier**. It shows what
travelled, never its content.

- **Shape** is the declared output kind of the producing node, taken from the
  pinned definition. It is constant across jobs and keeps per-step shapes in
  Live as in Demo.
- **Gems** are the actual items on the carrier, set into sockets on its rim.
  Each gem's cut and colour show its content type; cut is the primary signal so
  colour is not required:

  | Item | Gem |
  | --- | --- |
  | Text or document | Sapphire, round cut |
  | Structured data | Emerald, square cut |
  | Binary file | Amethyst, marquise cut |
  | Link (`url` part) | Topaz, triangle cut |
  | Message output (`output: message`) | Clear diamond |

- **Gem quality is the evidence level:**
  - a flawless gem with a glint means the hand-off is recorded and its digests
    match from producer to consumer;
  - a chipped, cloudy gem means it is inferred from graph order (pre-record
    history only).

  Rings around a carrier keep their existing meanings: rejected, exhausted,
  exit, progress, and selection.
- **Filling:** a carrier waits in its station with dark empty sockets while its
  items stream. Each gem drops in with a brief sparkle at its ready time, and
  the carrier leaves when the hand-off is produced.
- **More than four items** shows four gems and a "+N" badge. At low zoom the
  gems collapse to the badge.
- **Fan-in:** carriers merge at a join into one carrier holding every gem.
- **Gate verdicts** appear as a seal on the carrier (accepted or rejected); the
  seal is not a gem.
- **Revision label:** the carrier keeps it (R1, R2, …).
- **Inspector:** each item's source, part kinds, `mediaType`, size, short
  digest, and ready time. It never shows content.

Belt dwell remains the observed gap between a hand-off being produced and its
consumption; the 0.8 s minimum visible hop remains the only presentation
adjustment.

## Delivery sequence

1. **A2A v1 migration:** servers, clients, model agents, Quality, fixtures, and
   dashboard parsing. It is verified in the new environment, followed by the
   operator-approved restart.
2. **Hand-off records:**
   - definition `output` modes and edge kinds;
   - the three Observation facts;
   - snapshot retention;
   - S33 requalification.
3. **Floor carriers and gems:**
   - material and control edges;
   - side-effect nodes;
   - gate seals;
   - removal of the inferred "Task output · artifact not recorded" items for
     runs that have records.

## Not decided here

- Directory discovery, authentication, and payment for agents operated by third
  parties on the internet.
- Rotation and escrow of the per-factory digest key. The prototype uses one key
  per factory instance, stored in its instance home.
- Whether `raw` parts larger than a bound are fetched by reference rather than
  inline.
