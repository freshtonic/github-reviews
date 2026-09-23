# Use notifications for discovery and PR state for tracking

`github-reviews` uses GitHub review-request notifications to discover pull requests, then persists and periodically rechecks active PRs directly; approved PRs become dormant until an explicit new request reactivates them. GitHub does not guarantee that a head push updates a notification thread, so notification polling alone cannot reliably detect when a pull request has changed since a `CHANGES_REQUESTED` review.
