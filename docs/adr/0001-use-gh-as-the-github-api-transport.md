# Use `gh` as the GitHub API transport

`github-reviews` delegates GitHub API requests and authentication to the installed `gh` CLI instead of implementing HTTP transport and token discovery itself. This adds a runtime dependency on `gh`, but reuses its established authentication and host handling and keeps credentials out of this tool's interface.
