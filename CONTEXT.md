# GitHub Reviews

A polling and dispatch context that turns actionable GitHub review requests into deliveries to an external review command.

## Language

**Review Request**:
A request for the current GitHub user, or a team they belong to, to review a pull request. Different team members may act on the same request independently.
_Avoid_: Notification, assignment

**Registered Repository**:
A GitHub repository whose origin is mapped to one local worktree root. The daemon consumes review requests only for registered repositories and runs their review commands from the mapped worktree.
_Avoid_: Project, clone, watched directory

**Repository Registry**:
The shared mapping from GitHub repository origins to local worktree roots, maintained by `github-reviews register` and `github-reviews unregister`.
_Avoid_: Workspace list, repository filter

**Actionable Review Request**:
A review request for an open, non-draft, unmerged pull request in a registered repository whose author is not the current user, and that has no effective opinionated review from the current user, has changed head since that user's `CHANGES_REQUESTED` review, or has been explicitly re-requested after that user's approval.
_Avoid_: Matching notification, task

**Tracked Pull Request**:
A pull request discovered through a review-request notification and periodically rechecked directly so head changes can be detected even when the notification thread does not change.
_Avoid_: Watched notification, subscription

**Opinionated Review**:
An effective `APPROVED` or `CHANGES_REQUESTED` review. Comments and pending reviews are not opinionated, and dismissed reviews are no longer effective.
_Avoid_: Comment, review activity

**Head Change**:
A difference between the pull request's current head commit and the commit associated with the current user's latest effective `CHANGES_REQUESTED` review. An unavailable prior commit is treated conservatively as changed.
_Avoid_: Diff change, update

**Changes Requested**:
The GitHub review verdict indicating that the reviewer requires changes before approval.
_Avoid_: Rejected, failed review

**Review Command**:
The external program configured with `--review-command` and invoked for an actionable review request from the registered repository's local worktree root. It receives a stable, tool-owned JSON object describing the request on standard input.
_Avoid_: Handler, action, callback

**Automated Review Command**:
A review command that runs an agent with read-only access to the registered repository and authority to apply Review Operations directly to the exact pull request in its input envelope. It reports what it did on standard output and fails unless every operation it chose succeeds.
_Avoid_: Review bot, reviewer

**Review Operation**:
A review-specific GitHub mutation chosen by an automated reviewer: submitting a verdict, creating a general or inline comment, replying to a review thread, or resolving or unresolving a thread. Repository administration and pull-request lifecycle changes are outside this authority.
_Avoid_: GitHub action, repository operation

**Review Action**:
A durable decision to run a review command for a particular pull request head and triggering request or review. A successful action is not repeated unless the head or triggering request changes.
_Avoid_: Delivery, notification event, job

**Parked Review Action**:
A review action that exhausted its three automatic review-command attempts. It remains inactive until a newer action supersedes it or an operator resets it with `github-reviews retry`.
_Avoid_: Dead letter, abandoned review
