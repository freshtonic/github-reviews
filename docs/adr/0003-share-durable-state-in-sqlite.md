# Share durable state in SQLite

All `github-reviews` invocations share one SQLite database at `~/.config/github-reviews/state.sqlite3`, rather than keeping per-process state. The database atomically coordinates discovered pull requests, polling watermarks, review actions, retries, and process leases so concurrent invocations do not lose or accidentally repeat work.
