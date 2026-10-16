# Shared decision-round fixture

This fixture holds the remote capability and independent Quality processes constant
while Effect, Dagu, and Strands Graph are compared. Both processes run the same
customized Strands harness module with distinct role, identity, state, and A2A
endpoint. Their model is deterministic: the trial tests orchestration, not output
quality or a live model provider.

Launch each process with `harness_server.py --state DIR --role capability|quality
--port PORT`. The environment is the pinned S2 Strands+A2A project. The fixture
uses A2A protocol 0.3.0 and loopback-only test credentials.

An A2A `message/send` data command for the capability is:

```json
{"op":"assign","action_id":"RUN:research","run_id":"RUN","definition_digest":"SHA","brief":"research brief","drop_ack":false}
```

The completed A2A Task contains one structured artifact with `revision: "r2"`,
`sha256`, `author`, and `content`. `drop_ack: true` on a *new* assignment commits
the action and task mapping, then kills the receiver before HTTP acknowledgement.
Restart it against the same state and reconcile with
`GET /fixture/actions/{action_id}`. That query is a fixture-specific application
extension; A2A 0.3.0 does not define lookup by caller action ID.

A separate Quality process receives an A2A `message/send` command:

```json
{"op":"review","action_id":"RUN:review","run_id":"RUN","definition_digest":"SHA","artifact":{"revision":"r2","sha256":"SHA","author":"CAPABILITY_ID","content":"..."}}
```

Its structured artifact records `accepted`, `revision`, `sha256`, and `reviewer`.
The result is a fixture review decision. Each candidate's product ledger must
still accept only the *current exact artifact revision*, reject self-review and
duplicate acceptance, and distinguish acceptance from downstream delivery.

`GET /fixture/actions/{action_id}` returns the persisted action, run/digest
binding, task ID, artifact, accepted effect count, and submission-attempt count.
The receiver's unique action key prevents a second accepted *fixture* effect;
it does not prove exactly-once behavior for arbitrary A2A peers.
