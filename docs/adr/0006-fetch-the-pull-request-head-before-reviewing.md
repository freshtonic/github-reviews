# Fetch the pull request head before running the review command

Immediately before each review-command attempt, `github-reviews` fetches the pull request's current base ref and the base repository's synthetic `refs/pull/<number>/head` ref from `origin`, which works for both same-repository and fork pull requests. It fetches objects without checking out or creating named local refs, verifies both commits against the API's expected base and head SHAs, and re-evaluates the action if either side moved during preparation.
