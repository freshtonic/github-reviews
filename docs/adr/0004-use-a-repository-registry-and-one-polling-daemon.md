# Use a repository registry and one polling daemon

One `github-reviews` daemon polls for all registered repositories and controls global concurrency and rate limits. `github-reviews register` maps the current clone's GitHub origin to its local worktree root in the shared SQLite database, while `unregister` removes that mapping; review commands run from the registered local path. This avoids both one daemon per clone and a static configuration file maintained separately from the clones themselves.
