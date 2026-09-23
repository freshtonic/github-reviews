# Handle supervisor termination gracefully

Status: needs-triage

## Finding

`README.md` recommends running the daemon under launchd, systemd, or another supervisor. Those supervisors normally stop processes with `SIGTERM`, but the configured `ctrlc` dependency and daemon handler cover Ctrl-C without enabling termination-signal handling.

`SIGTERM` can therefore bypass worker joins and lease release and may leave independently grouped review commands running.

## Expected outcome

Handle the normal supervisor termination signal through the same graceful shutdown path as the first Ctrl-C. Document signal behavior and add coverage that review commands are joined and the daemon lease is released.
