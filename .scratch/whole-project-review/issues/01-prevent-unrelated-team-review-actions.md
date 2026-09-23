# Prevent unrelated team review requests from becoming actionable

Status: needs-triage

## Finding

When `/user/teams` fails, the daemon enables `trust_notification_for_teams`. The timeline decoder can then accept any currently requested team, and actionability receives the pull request's requested teams as though they were the viewer's memberships.

This can make a review request for an unrelated team actionable, contrary to ADR-0005's requirement that the request target the current user or one of their teams.

Evidence: `src/daemon.rs`, `src/github.rs`, and `docs/adr/0005-define-actionable-review-requests.md`.

## Expected outcome

Failure to enumerate team membership must not authorize review actions for unrelated teams. Preserve only team identity proven by a relevant notification, or defer team review requests until membership can be established. Add regression coverage for a requested team the viewer does not belong to.
