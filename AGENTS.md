# modal-mcp-server — Agent Instructions

Python MCP server that exposes 44 tools for managing Modal apps, containers,
secrets, volumes, environments, Dicts, Queues, and network file systems. The
FastMCP server and all tool implementations live in `src/modal_mcp/server.py`;
they delegate to the locally authenticated Modal CLI and return standardized
response dictionaries. `tests/test_server.py` covers command construction and
responses without contacting Modal.

## Feanix skills

Portable agent skills live in [feanix-tech/feanix-skills](https://github.com/feanix-tech/feanix-skills).
If a skill is not loaded in your harness, read its `skills/<name>/SKILL.md`
before the relevant work:

- **decisions** — consult the Feanix ADR catalog before architectural changes.
- **shift-left-security** — secure subprocess, credential, secret, path, and
  destructive-resource-management changes; run its pre-commit scan gate.

## Architecture decisions (ADRs)

The binding catalog is
[feanix-tech/feanix-decisions](https://github.com/feanix-tech/feanix-decisions),
but no catalog exists for this repo yet. Using the `decisions` skill, check for
cross-system precedent before architectural changes. When the first repo-level
decision warrants a record, create its catalog through a feanix-decisions PR
from `templates/adr-template.md`, and cite the ADR id in this repo's PR.

## Commands

```bash
uv sync
uv run pytest tests/ -v
uv run src/modal_mcp/server.py
```

Python 3.11+ and a Modal CLI configured with valid credentials are required.
Deploying an app additionally requires the target project to use `uv` and have
`modal` installed in its environment.

## No debugging in Modal

WE DO NOT DEBUG IN PIPELINES. Before changing a tool, trace the MCP input → CLI
argv → `run_modal_command` → response path and cover it locally with a mocked
CLI unit test. Reproduce failures from captured stdout/stderr; do not iterate by
deploying apps or mutating live Modal resources to discover what a change does.

## Security and invariants

- Keep subprocess calls argv-based; never introduce `shell=True` or construct a
  shell command from MCP input.
- Never log, return, or embed credentials. `create_modal_secret` must continue
  to execute real `KEY=value` arguments while exposing only `<REDACTED>` values
  in logs, errors, and returned `command` fields.
- Preserve the response contract: success responses use `success: True` plus
  operation data/message; failures use `success: False` and `error`, with
  stdout/stderr included when present; streaming log timeouts are successful
  responses with `timed_out: True` and captured output.
- Preserve optional `environment` routing where supported and keep destructive
  actions explicit in their CLI argv.
- Unit tests must mock `run_modal_command` or `subprocess.run`; they must never
  contact Modal or modify real resources.

