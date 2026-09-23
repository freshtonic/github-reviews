# Define forced-abort attempt semantics

Status: needs-triage

## Finding

A second Ctrl-C cancels an in-progress Automated Review Command through the infrastructure-deferral path, which refunds the attempt. Repeated forced aborts can therefore exceed the documented three-attempt limit even though a command may already have performed partial Review Operations.

## Expected outcome

Decide and document whether an operator-forced abort consumes an attempt. Ensure persistence and retry behavior match that decision and safely account for commands that may have partially mutated GitHub before cancellation.
