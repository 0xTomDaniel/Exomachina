# Authoring lane handoff

## Files changed

- `src/authoring.py`: bounded vocabulary and materialization, Strands tool author, validation feedback, session round cap, automatic approval record, provider detection, and a deterministic tool-calling model.
- `tests/test_authoring.py`: scripted revision, v1/v2 digest difference, cap, rejected submissions, and provider detection.
- `definitions/authoring-brief-v2.md`: the requested three-branch v2 graph brief.
- `evidence/authoring-scripted-session.json`: complete scripted tool results, rounds, digests, and provider status.
- `handoff/authoring.md`: this handoff.

## Verification and exact commands

From the repository root:

```sh
PY=/Users/tomdaniel/Documents/Ember_Cognition_Inc/Software/Exomachina/tools/spikes/2026-09-22/arbitration/temporal/.venv/bin/python; cd prototype/temporal-factory && $PY -B -m unittest tests.test_authoring -v
```

Ran twice, both exit 0. Final output:

```text
test_round_cap_blocks_corrected_submission ... ok
test_scripted_revises_validation_error_and_approves_new_package ... ok
test_submit_rejects_unknown_binding_and_arbitrary_node ... ok
test_vocabulary_and_unavailable_live_model ... ok
Ran 4 tests in 0.026s
OK
```

Smoke command (exit 0): `$PY -B - <<'PY'` with fixture bindings, `AuthoringSession(StrandsGraphAuthor(ScriptedAuthoringModel()), approved_bindings=b).run('Create v2')`. Output: `approved 2`, an invalid round with `route must cover each typed value`, a valid round, and tool order `describe_vocabulary`, `validate_draft`, `submit_draft`.

Evidence command (exit 0): `$PY -B - <<'PY'` from `prototype/temporal-factory`, loading `tests.test_authoring.bindings`, the v1 template, and the v2 brief; running `AuthoringSession`; writing `evidence/authoring-scripted-session.json`. Output: `status: approved`, `rounds: 2`, `tool calls: describe_vocabulary, validate_draft, submit_draft`, v1 digest `f68e253dec0b61abba65115c731eec173f93317fb2dd0a3c1274526052575b44`, v2 digest `6459129e34f7d7ac0469f9a3facf0c0f66fa6e1a48c6b3792bd4efd7326d5422`.

Read/inspection commands: `pwd && git status --short && cat prototype/temporal-factory/INTERFACES.md && cat prototype/temporal-factory/briefs/authoring.md` passed; `cat prototype/temporal-factory/src/definition.py`, `cat prototype/temporal-factory/src/harness_server.py`, and `cat prototype/temporal-factory/definitions/v1-template.json` passed. `sed -n '300,410p' prototype/temporal-factory/src/definition.py`, `rg -n 'def __init__\(' .../strands/models/{anthropic,bedrock,openai}.py`, and `sed -n` of those model constructors passed. Initial `ls` and `cat prototype/temporal-factory/src/author.py` reported missing planned/new or lane-1 files. An exploratory `from strands.models import AnthropicModel, BedrockModel, OpenAIModel` returned `ModuleNotFoundError: No module named 'anthropic'`, consistent with provider detection; no package installation was attempted. Final `git status --short`, `wc -c` of lane files, and `sed -n '1,100p' evidence/authoring-scripted-session.json` passed. Other modified and untracked files in the status belonged to concurrent lanes and were not edited here.

## Scripted transcript

1. `describe_vocabulary` returned the bounded node grammar, typed route values, and approved binding names.
2. `validate_draft` on draft `967e62f216a978447ab5d35a0e9eaf5053367382e4292dbfd5fbc34a8e3160de` returned `route must cover each typed value`, path `child.nodes.route_scope.cases`, missing case `requires_scope`.
3. The model read that missing case from the returned error, added its `draft_unresolved` edge, and called `submit_draft` on draft `0c3a78eb18e84909f68f3e82d2fe7eca6b63eb110e6bbd2ef1becfe68aabbaa1`. Validation passed, producing package digest `6459129e34f7d7ac0469f9a3facf0c0f66fa6e1a48c6b3792bd4efd7326d5422` and an automatic bounded approval record.

## Gaps and blocked actions

- Live model path: `untested: ANTHROPIC_API_KEY absent; AWS credentials/configuration absent; OPENAI_API_KEY absent`. The scripted model is labeled `live: false`.
- No command or automatic approval was rejected. No trial state was created under `/tmp`.
