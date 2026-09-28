# Architecture principles

Design principle adopted 28 September 2026. This document states the intended
architecture; it does not claim that every replacement path is implemented or
qualified. Terms follow the [project glossary](../CONTEXT.md).

## Keep process, provider, and implementation independently replaceable

A factory separates three decisions:

| Boundary | What may change | What consumers rely on |
| --- | --- | --- |
| **Process** | The factory definition: dependencies, routing, required gates, and where prescribed or adaptive decisions occur. | The advertised outcome, capability contract, acceptance criteria, and operating limits. |
| **Provider** | The agent service selected to fulfill an assignment, including an internal service or independently operated supplier. | The assignment's inputs, result shape, public guarantees, permitted context and actions, and completion criteria. |
| **Implementation** | A provider's models, harnesses, worker count, internal factory, and conventional software. | The provider's public capability contract, including any promised model, data-handling, evidence, cost, or timing constraints. |

Changing one boundary should not inherently require changing the other two.
For example, a provider can replace its harness while retaining its public
contract; a factory can bind a qualified alternative provider without rewriting
its process; and a new factory version can reorganize work while reusing existing
providers and their implementations.

This is separation of responsibilities, not a promise that arbitrary replacements
are compatible. A protocol-compatible endpoint may still fail a capability's
behavioral or operating requirements. Replacements require the appropriate
qualification and authorization. A change to a public guarantee requires a
contract change and any resulting consumer changes.

### Consequences for factory composition

- Define nodes around capability assignments and their contracts, rather than
  coupling the process to a particular model, harness, terminal, or internal
  worker layout. Multiple assignments may use the same service.
- Use A2A at agent-service boundaries to exchange assignments, task state and
  artifacts. Keep process rules and acceptance semantics explicit above that
  protocol. A remote completion report does not itself establish acceptance.
- Preserve the normal agent-service interface when a provider uses a factory
  internally. Consumers need contract-relevant guarantees; they need not adopt
  that provider's internal graph or number of workers.
- Keep ordinary software operations, API calls, waits, and required gates useful
  within a factory. A separate remote agent service is not required for each
  internal operation.
- Permit adaptive work inside an assignment while keeping its permitted scope,
  resources, evidence and completion criteria explicit. A change to the enclosing
  process uses the factory's publication and authority rules.

### Preserve version binding and active work

Independent replaceability does not authorize silent changes to active runs.
Record and retain the selected factory version, provider bindings, contract
versions and required implementation attestations or build identifiers. Do not
invent visibility into an opaque provider's private build or model.

Publish and qualify changes for future admissions. An active assignment may
change provider or behavior only through an explicit permitted recovery or
migration decision that records prior attempts, unresolved effects, the new
binding and its authority. Reconcile ambiguous remote outcomes before a retry
or replacement could duplicate effects. Existing artifact acceptance remains
bound to its revision and criteria.

The existing [prototype version binding and qualification limits](../prototype/temporal-factory/README.md)
and [maintenance authority rules](factory-maintenance.md) continue to apply.
This principle does not select a new engine, require a runtime migration, or
establish automatic in-flight substitution.

### Review questions

1. Can a qualified provider replace the current binding while preserving the
   process and assignment contract?
2. Can a provider replace its internal harness or worker arrangement while
   preserving contract-relevant behavior?
3. Can a new process version reuse current capabilities without requiring every
   provider to redeploy?
4. Are the limits to each replacement explicit, with evidence and an authorized
   path for active work?

These are architecture review criteria, not claims of completed qualification.
