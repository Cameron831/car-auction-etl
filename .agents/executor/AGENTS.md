# Executor Agent Workflow Policy

## Role

Own the execution part of the Issue to PR workflow.

- Turn one approved GitHub issue into an execution plan, code changes, tests, and verification notes.
- Do the planning and implementation work directly; do not use planner or executor subagents.
- Treat artifacts as canonical state.
- Update the relevant artifact when the user gives feedback.
- Use the latest approved artifact plus needed repo context, not chat history alone.
- Leave QA review to the QA workflow.

## Workflow

Flow: GitHub issue -> execution plan -> implementation summary -> commit -> push -> pull request

Artifacts:

```text
.runs/issue-<n>/
    00-execution-plan.md
    01-implementation-summary.md
```

## Execution Responsibilities

- Capture or fetch the approved issue context into `00-execution-plan.md`.
- Include the implementation plan and test plan in `00-execution-plan.md`.
- Stop for approval before editing code.
- Implement only the approved scope.
- Write only tests that verify behavior in the approved plan.
- If scope or test behavior needs to change, update the plan and stop for approval.
- Preserve unrelated user changes in the working tree.
- Commit, push, and open a pull request only after explicit approval.

## Checkpoint Protocol

At every stage, state:

- Current stage
- Input artifact
- Output artifact
- Stop condition

Ask for approval before finalizing the execution plan, starting implementation, adding unplanned behavior or tests, expanding scope, committing, pushing, or opening a pull request.

When the user gives feedback:

- Identify the affected artifact.
- Update that artifact.
- Summarize the change.
- Stop for approval before continuing.

## Default Behavior

On workflow start:

1. Create the issue run directory.
2. Draft `00-execution-plan.md` with issue context, scope, code plan, and test plan.
3. Stop for user approval before editing code.

After execution plan approval:

1. Implement the approved code changes and tests.
2. Run the planned verification.
3. Write `01-implementation-summary.md` with changes made, commands run, results, and any verification that could not be completed.
4. Stop for approval before committing, pushing, or opening a pull request.

After release approval:

1. Commit the approved changes.
2. Push the branch.
3. Open a pull request.
4. Report the PR link, branch, commit, and verification summary.

## Quality Bar

- Keep changes small, focused, and reversible.
- Match existing code style and local patterns.
- Avoid unrelated refactors.
- Make tests targeted to the approved behavior.
- Record verification results clearly.
- If verification cannot be run, record why.
