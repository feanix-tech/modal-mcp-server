# Modal MCP Server

An MCP server that provides comprehensive access to the [Modal](https://modal.com) platform — covering app deployment, container management, volumes, secrets, environments, dicts, queues, and network file systems.

## Installation

1. Clone this repository:
```bash
git clone https://github.com/feanix-tech/modal-mcp-server.git
cd modal-mcp-server
```

2. Install dependencies using `uv`:
```bash
uv sync
```

## Configuration

Add the following to your MCP client configuration (e.g. `~/.cursor/mcp.json` for Cursor, or the equivalent for your editor):

```json
{
  "mcpServers": {
    "modal-mcp-server": {
      "command": "uv",
      "args": [
        "--project", "/path/to/modal-mcp-server",
        "run", "/path/to/modal-mcp-server/src/modal_mcp/server.py"
      ]
    }
  }
}
```

Replace `/path/to/modal-mcp-server` with the absolute path to your cloned repository.

## Requirements

- Python 3.11 or higher
- `uv` package manager
- Modal CLI configured with valid credentials
- For `deploy_modal_app`: the target project must use `uv` and have `modal` installed in its venv

## Supported Tools

The server exposes **44 tools** across 9 categories. Most tools that target a specific environment accept an optional `environment` parameter.

### Deployment

| Tool | Description |
|------|-------------|
| `deploy_modal_app` | Deploy a Modal application from an absolute file path |

### App Management

| Tool | Description |
|------|-------------|
| `list_modal_apps` | List deployed/running/recently stopped apps |
| `get_modal_app_logs` | Fetch logs for an app (with configurable timeout) |
| `stop_modal_app` | Stop a running app |
| `get_modal_app_history` | Show deployment history for an app |
| `rollback_modal_app` | Redeploy a previous version of an app |

### Container Management

| Tool | Description |
|------|-------------|
| `list_modal_containers` | List all currently running containers |
| `get_modal_container_logs` | Fetch logs for a container (with configurable timeout) |
| `exec_modal_container` | Execute a command inside a running container |
| `stop_modal_container` | Stop a running container |

### Secret Management

| Tool | Description |
|------|-------------|
| `list_modal_secrets` | List all published secrets |
| `create_modal_secret` | Create a secret with key-value pairs (supports `force` overwrite) |

### Volume Management

| Tool | Description |
|------|-------------|
| `list_modal_volumes` | List all volumes |
| `list_modal_volume_contents` | List files/directories in a volume |
| `copy_modal_volume_files` | Copy files within a volume |
| `put_modal_volume_file` | Upload a local file/directory to a volume |
| `get_modal_volume_file` | Download files from a volume |
| `remove_modal_volume_file` | Delete a file or directory from a volume |
| `create_modal_volume` | Create a new persistent volume |
| `delete_modal_volume` | Delete a volume |
| `rename_modal_volume` | Rename a volume |

### Environment Management

| Tool | Description |
|------|-------------|
| `list_modal_environments` | List all environments in the workspace |
| `create_modal_environment` | Create a new environment |
| `delete_modal_environment` | Delete an environment (irreversible) |
| `update_modal_environment` | Rename or change the web suffix of an environment |

### Dict Management

| Tool | Description |
|------|-------------|
| `list_modal_dicts` | List all named Dicts |
| `create_modal_dict` | Create a new Dict |
| `delete_modal_dict` | Delete a Dict and all its data |
| `clear_modal_dict` | Clear all entries from a Dict |
| `get_modal_dict_value` | Get the value for a specific key |
| `list_modal_dict_items` | List entries in a Dict (supports `n` limit or `show_all`) |

### Queue Management

| Tool | Description |
|------|-------------|
| `list_modal_queues` | List all named Queues |
| `create_modal_queue` | Create a new Queue |
| `delete_modal_queue` | Delete a Queue and all its data |
| `clear_modal_queue` | Clear a Queue (optionally a specific partition) |
| `peek_modal_queue` | Peek at the next N items without removing them |
| `get_modal_queue_length` | Get the length of a Queue (supports partition and total) |

### Network File System (NFS) Management

| Tool | Description |
|------|-------------|
| `list_modal_nfs` | List all network file systems |
| `create_modal_nfs` | Create a new NFS |
| `delete_modal_nfs` | Delete an NFS |
| `list_modal_nfs_contents` | List files/directories in an NFS |
| `put_modal_nfs_file` | Upload a file/directory to an NFS |
| `get_modal_nfs_file` | Download a file from an NFS |
| `remove_modal_nfs_file` | Delete a file or directory from an NFS |

## Response Format

All tools return a dict with a standardized structure:

```python
# Success — JSON operations (list, history, etc.):
{"success": True, "volumes": [...]}  # key varies by tool

# Success — mutation operations (create, delete, etc.):
{"success": True, "message": "...", "command": "..."}

# Success — streaming commands (logs) that hit the timeout:
{"success": True, "timed_out": True, "stdout": "...", "stderr": "..."}

# Failure:
{"success": False, "error": "...", "stdout": "...", "stderr": "..."}
```

## Development

Run the test suite:

```bash
uv run pytest tests/ -v
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.