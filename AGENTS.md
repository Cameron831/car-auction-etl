# Agent Workforce Policy

## Role

This repository defines three focused agents. Use the agent-specific prompt for workflow details.

## Agent Routing

When the user asks to plan a feature, first read `.agents/planner/AGENTS.md` and follow it.

When the user asks to execute an issue, first read `.agents/executor/AGENTS.md` and follow it.

When the user asks to review a PR, first read `.agents/qa/AGENTS.md` and follow it.

## Agents

### Planner

Use `.agents/planner/AGENTS.md` for Feature to Issues work:

- Turn a feature request into an advised feature plan.
- Draft small, reviewable GitHub issues.
- Create issues after approval.

### Executor

Use `.agents/executor/AGENTS.md` for Issue to PR work:

- Turn an approved issue into an execution plan.
- Implement approved changes and tests.
- Verify, commit, push, and open a PR after approval.

### QA

Use `.agents/qa/AGENTS.md` for PR review work:

- Investigate a given PR.
- Report bugs, regressions, missing tests, and scope drift.
- Remain read-only unless explicitly asked otherwise.

## Shared Rules

- Treat artifacts as canonical state when a workflow defines them.
- Preserve unrelated user changes in the working tree.
- Ask before expanding approved scope.
- Do not commit, push, open PRs, post GitHub comments, approve, request changes, merge, or make code changes unless the active workflow or user request allows it.
- Prefer concise, decision-oriented artifacts and reports.
