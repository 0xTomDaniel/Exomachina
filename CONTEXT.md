# Exomachina

Exomachina describes how independent agent capabilities cooperate through reusable factories. This glossary names the work, evidence, and authority visible to their participants.

## Language

**Capability**:
A publicly described kind of work that an agent service can undertake for a caller.
_Avoid_: Model, harness

**Agent service**:
An independently callable participant that accepts assignments and is accountable for its advertised capability contract. Its internal workers and methods are private to it.
_Avoid_: Factory step, conversation

**Capability contract**:
The agreement defining a capability’s required inputs, result shape, public guarantees, and conditions for completing an assignment.
_Avoid_: Agent Card, capability description

**Factory service**:
An agent service configured to use a factory internally to fulfill its advertised capability contract. Its Director is accountable for factory decisions; callers see the normal A2A service identity and accepted outcome.
_Avoid_: Dedicated factory process, workflow engine, Director conversation, separate factory endpoint

**Factory**:
A reusable process describing how capabilities cooperate to achieve an outcome under explicit acceptance and operating rules.
_Avoid_: Agent team, organizational hierarchy

**Factory version**:
A particular published definition of a factory that can be selected for a new run.
_Avoid_: Running process, latest factory

**Factory run**:
One undertaking of a chosen factory version for a specific brief.
_Avoid_: Factory version, execution attempt

**Assignment**:
A logical unit of work delegated to a capability with a brief, permitted context, expected results, and completion criteria.
_Avoid_: Attempt, remote task

**Attempt**:
One recorded effort to fulfill an assignment. An assignment may have several attempts with different outcomes.
_Avoid_: Assignment, factory run

**Artifact revision**:
A specific version of a work product and the evidence associated with it.
_Avoid_: Latest output, conversation summary

**Acceptance**:
A recorded decision that a particular artifact revision meets its required criteria.
_Avoid_: Task completion, delivery acknowledgement

**Director**:
An authorized human or agent accountable for a factory's objective, priorities, resource limits, and decisions escalated beyond its ordinary transitions.
_Avoid_: Mandatory human approver, internal worker

**Production**:
The factory responsibility for planning and fulfilling authorized work, from ready inputs and qualified capabilities to delivery of accepted results.
_Avoid_: Every factory responsibility, acceptance authority

**Quality**:
The factory responsibility for independently assessing outputs and proposed improvements against protected acceptance criteria.
_Avoid_: Author self-approval, optional research evaluator

**Engineering**:
The factory responsibility for restoring reliable operation and continuously improving how work is performed. It includes maintenance and may undertake optional research campaigns.
_Avoid_: Separate mandatory maintenance hierarchy, unrestricted self-modification

**Execution ledger**:
The authoritative history of a factory run’s assignments, attempts, evidence, decisions, and transitions.
_Avoid_: Agent memory, chat history, board view

**Maintenance and Engineering**:
The maintenance and improvement work owned by Engineering.
_Avoid_: Ordinary task retry, separate scheduler

**Factory incident**:
A recorded interruption or degradation of a factory’s expected operation, with an accountable recovery owner and evidence of its outcome.
_Avoid_: Every task failure, improvement hypothesis

**Remediation**:
An authorized action intended to restore acceptable operation under the factory’s current operating rules.
_Avoid_: Unrestricted self-modification, acceptance bypass

**Improvement candidate**:
A proposed change to a factory or participating capability, accompanied by its hypothesis, base version, and evaluation evidence.
_Avoid_: Published version, accepted artifact

**Research campaign**:
An optional, bounded undertaking that proactively searches for better factory behavior against a declared objective and evaluation method.
_Avoid_: Incident response, perpetual unbudgeted experimentation

**Promotion**:
A recorded decision to make an evaluated improvement available for a defined scope of future work.
_Avoid_: Experiment success, changing an active run
