# Document the Review Command protocol

Status: needs-triage

## Finding

`README.md` and `CONTEXT.md` promise a stable, versioned JSON object on standard input, but provide neither a schema nor a representative payload. Users must reverse-engineer `src/model.rs` or the bundled examples to build a compatible Review Command.

## Expected outcome

Document every input field, enum/reason value, versioning expectations, working directory, standard-input framing, output behavior, exit-status semantics, timeout/cancellation behavior, and a representative JSON payload. Link the bundled Automated Review Command examples as implementations of that protocol.
