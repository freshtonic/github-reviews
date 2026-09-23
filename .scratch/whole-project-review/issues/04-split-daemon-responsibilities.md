# Split the daemon module into cohesive responsibilities

Status: needs-triage

## Finding

`src/daemon.rs` owns command handlers, daemon lifecycle, signal handling, polling, discovery, scheduling, review preparation and dispatch, retries, and lease heartbeat management. The module is large and changes for several unrelated reasons.

This is a judgement-call finding rather than a documented-standard violation.

## Expected outcome

Assess seams for separating CLI operations, polling/discovery, and review execution while keeping the current behavior and tests intact. Prefer deep modules with narrow interfaces over a mechanical file split.
