# Authoring brief: verified research v2

Author a new schema 1 template with the standard digest-pinned root and one nested `verified_research` child. Give both definitions revision `v5`. The child must use exactly three parallel capability assignments: `source_alpha` and `source_beta` produce `source_evidence@1`, and `counter_alpha` produces `counter_evidence@1` with `scope_status: "clear"`. Join all three named predecessors.

Route on the typed `join.route_status` field. A clear join produces a resolved candidate; a join requiring scope produces an unresolved candidate. Send either candidate to independent Quality, then route its accepted verdict. Acceptance goes to the approved release receiver and completion. Rejection enters one bounded repair, synthesizes an `after_repair` candidate, and returns to Quality. After the repair budget is exhausted, wait for the Director's abort authorization and abort. Every typed route value needs an explicit edge.

Include the optional caller string `question`, with allowed actor `fixture-operator` and `may_affect_acceptance: false`. The graph does not use `from_run` synthesis or require `outcome_mode`. Use only the pinned approved service names and the declarative grammar supplied by `describe_vocabulary`. Validate the draft, revise from errors, and submit the valid template.
