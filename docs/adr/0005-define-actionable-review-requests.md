# Define actionable review requests

A review request is actionable only when its repository is registered, its pull request is open, non-draft, unmerged, and authored by someone other than the current user, and the request targets either the user or one of their teams. The review command runs when the user has no effective opinionated review, when the head changed after their effective `CHANGES_REQUESTED` review, or when an explicit new request follows their approval; comments, pending reviews, and dismissed reviews do not satisfy the opinionated-review test.
