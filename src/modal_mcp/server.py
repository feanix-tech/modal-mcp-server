"""MCP server for deploying Modal applications."""
import logging
import os
from typing import Any, Optional, List, Dict
import subprocess
import json

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("modal-deploy")

def run_modal_command(command: list[str], uv_directory: Optional[str] = None, timeout: Optional[int] = None) -> dict[str, Any]:
    """Run a Modal CLI command and return the result.

    Args:
        command: The command and arguments to run.
        uv_directory: If provided, prefix the command with ``uv run --directory=<dir>``.
        timeout: Optional timeout in seconds.  When the timeout expires the
            process is killed and any output captured so far is returned with
            a ``timed_out`` flag.  Useful for streaming commands like
            ``modal app logs`` that never terminate on their own.
    """
    try:
        # uv_directory is necessary for modal deploy, since deploying the app requires the app to use the uv venv
        command = (["uv", "run", f"--directory={uv_directory}"] if uv_directory else []) + command
        logger.info(f"Running command: {' '.join(command)}")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout
        )
        return {
            "success": True,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "command": ' '.join(command)
        }
    except subprocess.TimeoutExpired as e:
        return {
            "success": True,
            "timed_out": True,
            "stdout": (e.stdout or "") if isinstance(e.stdout, str) else (e.stdout or b"").decode("utf-8", errors="replace"),
            "stderr": (e.stderr or "") if isinstance(e.stderr, str) else (e.stderr or b"").decode("utf-8", errors="replace"),
            "command": ' '.join(command)
        }
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "error": str(e),
            "stdout": e.stdout,
            "stderr": e.stderr,
            "command": ' '.join(command)
        }

def handle_json_response(result: Dict[str, Any], error_prefix: str) -> Dict[str, Any]:
    """
    Handle JSON parsing of command output and return a standardized response.
    
    Args:
        result: The result from run_modal_command
        error_prefix: Prefix to use in error messages
        
    Returns:
        A dictionary with standardized success/error format
    """
    if not result["success"]:
        response = {"success": False, "error": f"{error_prefix}: {result.get('error', 'Unknown error')}"}
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    
    try:
        data = json.loads(result["stdout"])
        return {"success": True, "data": data}
    except json.JSONDecodeError as e:
        response = {"success": False, "error": f"Failed to parse JSON output: {str(e)}"}
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response

@mcp.tool()
async def deploy_modal_app(absolute_path_to_app: str) -> dict[str, Any]:
    """
    Deploy a Modal application using the provided parameters.

    Args:
        absolute_path_to_app: The absolute path to the Modal application to deploy.

    Returns:
        A dictionary containing deployment results.

    Raises:
        Exception: If deployment fails for any reason.
    """
    uv_directory = os.path.dirname(absolute_path_to_app)
    app_name = os.path.basename(absolute_path_to_app)
    try:
        result = run_modal_command(["modal", "deploy", app_name], uv_directory)
        return result
    except Exception as e:
        logger.error(f"Failed to deploy Modal app: {e}")
        raise

@mcp.tool()
async def list_modal_volumes() -> dict[str, Any]:
    """
    List all Modal volumes using the Modal CLI with JSON output.

    Returns:
        A dictionary containing the parsed JSON output of the Modal volumes list.
    """
    try:
        result = run_modal_command(["modal", "volume", "list", "--json"])
        response = handle_json_response(result, "Failed to list volumes")
        if response["success"]:
            return {"success": True, "volumes": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal volumes: {e}")
        raise

@mcp.tool()
async def list_modal_volume_contents(volume_name: str, path: str = "/") -> dict[str, Any]:
    """
    List files and directories in a Modal volume.

    Args:
        volume_name: Name of the Modal volume to list contents from.
        path: Path within the volume to list contents from. Defaults to root ("/").

    Returns:
        A dictionary containing the parsed JSON output of the volume contents.
    """
    try:
        result = run_modal_command(["modal", "volume", "ls", "--json", volume_name, path])
        response = handle_json_response(result, "Failed to list volume contents")
        if response["success"]:
            return {"success": True, "contents": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal volume contents: {e}")
        raise

@mcp.tool()
async def copy_modal_volume_files(volume_name: str, paths: List[str]) -> dict[str, Any]:
    """
    Copy files within a Modal volume. Can copy a source file to a destination file
    or multiple source files to a destination directory.

    Args:
        volume_name: Name of the Modal volume to perform copy operation in.
        paths: List of paths for the copy operation. The last path is the destination,
              all others are sources. For example: ["source1.txt", "source2.txt", "dest_dir/"]

    Returns:
        A dictionary containing the result of the copy operation.

    Raises:
        Exception: If the copy operation fails for any reason.
    """
    if len(paths) < 2:
        return {
            "success": False,
            "error": "At least one source and one destination path are required"
        }

    try:
        result = run_modal_command(["modal", "volume", "cp", volume_name] + paths)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        
        if not result["success"]:
            response["error"] = f"Failed to copy files: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully copied files in volume {volume_name}"
            
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
            
        return response
    except Exception as e:
        logger.error(f"Failed to copy files in Modal volume: {e}")
        raise

@mcp.tool()
async def remove_modal_volume_file(volume_name: str, remote_path: str, recursive: bool = False) -> dict[str, Any]:
    """
    Delete a file or directory from a Modal volume.

    Args:
        volume_name: Name of the Modal volume to delete from.
        remote_path: Path to the file or directory to delete.
        recursive: If True, delete directories recursively. Required for deleting directories.

    Returns:
        A dictionary containing the result of the delete operation.

    Raises:
        Exception: If the delete operation fails for any reason.
    """
    try:
        command = ["modal", "volume", "rm"]
        if recursive:
            command.append("-r")
        command.extend([volume_name, remote_path])
        
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        
        if not result["success"]:
            response["error"] = f"Failed to delete {remote_path}: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted {remote_path} from volume {volume_name}"
            
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
            
        return response
    except Exception as e:
        logger.error(f"Failed to delete from Modal volume: {e}")
        raise

@mcp.tool()
async def put_modal_volume_file(volume_name: str, local_path: str, remote_path: str = "/", force: bool = False) -> dict[str, Any]:
    """
    Upload a file or directory to a Modal volume.

    Args:
        volume_name: Name of the Modal volume to upload to.
        local_path: Path to the local file or directory to upload.
        remote_path: Path in the volume to upload to. Defaults to root ("/").
                    If ending with "/", it's treated as a directory and the file keeps its name.
        force: If True, overwrite existing files. Defaults to False.

    Returns:
        A dictionary containing the result of the upload operation.

    Raises:
        Exception: If the upload operation fails for any reason.
    """
    try:
        command = ["modal", "volume", "put"]
        if force:
            command.append("-f")
        command.extend([volume_name, local_path, remote_path])
        
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        
        if not result["success"]:
            response["error"] = f"Failed to upload {local_path}: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully uploaded {local_path} to {volume_name}:{remote_path}"
            
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
            
        return response
    except Exception as e:
        logger.error(f"Failed to upload to Modal volume: {e}")
        raise

@mcp.tool()
async def get_modal_volume_file(volume_name: str, remote_path: str, local_destination: str = ".", force: bool = False) -> dict[str, Any]:
    """
    Download files from a Modal volume.

    Args:
        volume_name: Name of the Modal volume to download from.
        remote_path: Path to the file or directory in the volume to download.
        local_destination: Local path to save the downloaded file(s). Defaults to current directory.
                         Use "-" to write file contents to stdout.
        force: If True, overwrite existing files. Defaults to False.

    Returns:
        A dictionary containing the result of the download operation.

    Raises:
        Exception: If the download operation fails for any reason.
    """
    try:
        command = ["modal", "volume", "get"]
        if force:
            command.append("--force")
        command.extend([volume_name, remote_path, local_destination])
        
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        
        if not result["success"]:
            response["error"] = f"Failed to download {remote_path}: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully downloaded {remote_path} from volume {volume_name}"
            
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
            
        return response
    except Exception as e:
        logger.error(f"Failed to download from Modal volume: {e}")
        raise

# --- App Management ---

@mcp.tool()
async def list_modal_apps(environment: Optional[str] = None) -> dict[str, Any]:
    """
    List Modal apps that are currently deployed/running or recently stopped.

    Args:
        environment: Optional Modal environment to list apps from.

    Returns:
        A dictionary containing the list of apps.
    """
    try:
        command = ["modal", "app", "list", "--json"]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list apps")
        if response["success"]:
            return {"success": True, "apps": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal apps: {e}")
        raise


@mcp.tool()
async def get_modal_app_logs(app_identifier: str, environment: Optional[str] = None, timeout: int = 30) -> dict[str, Any]:
    """
    Show logs for a Modal app. Streams logs while the app is active.

    Args:
        app_identifier: App name or ID (e.g., "my-app" or "ap-123456").
        environment: Optional Modal environment.
        timeout: Maximum seconds to collect logs before returning (default 30).
            The command streams indefinitely, so a timeout is used to capture
            a snapshot of the available output.

    Returns:
        A dictionary containing the app logs.
    """
    try:
        command = ["modal", "app", "logs", app_identifier]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command, timeout=timeout)
        return result
    except Exception as e:
        logger.error(f"Failed to get Modal app logs: {e}")
        raise


@mcp.tool()
async def stop_modal_app(app_identifier: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Stop a running Modal app.

    Args:
        app_identifier: App name or ID (e.g., "my-app" or "ap-123456").
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the stop operation.
    """
    try:
        command = ["modal", "app", "stop", app_identifier]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to stop app: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully stopped app {app_identifier}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to stop Modal app: {e}")
        raise


@mcp.tool()
async def get_modal_app_history(app_identifier: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Show deployment history for a currently deployed Modal app.

    Args:
        app_identifier: App name or ID (e.g., "my-app" or "ap-123456").
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the app's deployment history.
    """
    try:
        command = ["modal", "app", "history", "--json", app_identifier]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to get app history")
        if response["success"]:
            return {"success": True, "history": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to get Modal app history: {e}")
        raise


@mcp.tool()
async def rollback_modal_app(app_identifier: str, version: Optional[str] = None, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Redeploy a previous version of a Modal app. The app must be in a "deployed" state.

    Args:
        app_identifier: App name or ID (e.g., "my-app" or "ap-123456").
        version: Optional target version to rollback to (e.g., "v3"). Defaults to previous version.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the rollback operation.
    """
    try:
        command = ["modal", "app", "rollback", app_identifier]
        if version:
            command.append(version)
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to rollback app: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully rolled back app {app_identifier}" + (f" to {version}" if version else "")
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to rollback Modal app: {e}")
        raise


# --- Container Management ---

@mcp.tool()
async def list_modal_containers(environment: Optional[str] = None) -> dict[str, Any]:
    """
    List all currently running Modal containers.

    Args:
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the list of running containers.
    """
    try:
        command = ["modal", "container", "list", "--json"]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list containers")
        if response["success"]:
            return {"success": True, "containers": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal containers: {e}")
        raise


@mcp.tool()
async def get_modal_container_logs(container_id: str, timeout: int = 30) -> dict[str, Any]:
    """
    Show logs for a specific Modal container, streaming while active.

    Args:
        container_id: The container ID.
        timeout: Maximum seconds to collect logs before returning (default 30).
            The command streams indefinitely, so a timeout is used to capture
            a snapshot of the available output.

    Returns:
        A dictionary containing the container logs.
    """
    try:
        result = run_modal_command(["modal", "container", "logs", container_id], timeout=timeout)
        return result
    except Exception as e:
        logger.error(f"Failed to get Modal container logs: {e}")
        raise


@mcp.tool()
async def exec_modal_container(container_id: str, command: List[str]) -> dict[str, Any]:
    """
    Execute a command in a running Modal container.

    Args:
        container_id: The container ID.
        command: The command and arguments to execute inside the container.

    Returns:
        A dictionary containing the result of the command execution.
    """
    try:
        cmd = ["modal", "container", "exec", container_id, "--"] + command
        result = run_modal_command(cmd)
        return result
    except Exception as e:
        logger.error(f"Failed to exec in Modal container: {e}")
        raise


@mcp.tool()
async def stop_modal_container(container_id: str) -> dict[str, Any]:
    """
    Stop a currently-running Modal container and reassign its in-progress inputs.

    Args:
        container_id: The container ID.

    Returns:
        A dictionary containing the result of the stop operation.
    """
    try:
        result = run_modal_command(["modal", "container", "stop", container_id])
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to stop container: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully stopped container {container_id}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to stop Modal container: {e}")
        raise


# --- Secret Management ---

@mcp.tool()
async def list_modal_secrets(environment: Optional[str] = None) -> dict[str, Any]:
    """
    List all published Modal secrets.

    Args:
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the list of secrets.
    """
    try:
        command = ["modal", "secret", "list", "--json"]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list secrets")
        if response["success"]:
            return {"success": True, "secrets": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal secrets: {e}")
        raise


@mcp.tool()
async def create_modal_secret(secret_name: str, key_values: Dict[str, str], environment: Optional[str] = None, force: bool = False) -> dict[str, Any]:
    """
    Create a new Modal secret with key-value pairs.

    Args:
        secret_name: Name for the new secret.
        key_values: Dictionary of key-value pairs for the secret (e.g., {"API_KEY": "abc123"}).
        environment: Optional Modal environment.
        force: If True, overwrite the secret if it already exists.

    Returns:
        A dictionary containing the result of the create operation.
    """
    try:
        command = ["modal", "secret", "create", secret_name]
        for key, value in key_values.items():
            command.append(f"{key}={value}")
        if environment:
            command.extend(["--env", environment])
        if force:
            command.append("--force")
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to create secret: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully created secret {secret_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to create Modal secret: {e}")
        raise


# --- Volume Management (create/delete/rename) ---

@mcp.tool()
async def create_modal_volume(volume_name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Create a named, persistent Modal Volume.

    Args:
        volume_name: Name for the new volume.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the create operation.
    """
    try:
        command = ["modal", "volume", "create", volume_name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to create volume: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully created volume {volume_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to create Modal volume: {e}")
        raise


@mcp.tool()
async def delete_modal_volume(volume_name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Delete a named, persistent Modal Volume.

    Args:
        volume_name: Name of the volume to delete. Case sensitive.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the delete operation.
    """
    try:
        command = ["modal", "volume", "delete", "--yes", volume_name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to delete volume: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted volume {volume_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to delete Modal volume: {e}")
        raise


@mcp.tool()
async def rename_modal_volume(old_name: str, new_name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Rename a Modal Volume.

    Args:
        old_name: Current name of the volume.
        new_name: New name for the volume.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the rename operation.
    """
    try:
        command = ["modal", "volume", "rename", "--yes", old_name, new_name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to rename volume: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully renamed volume {old_name} to {new_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to rename Modal volume: {e}")
        raise


# --- Environment Management ---

@mcp.tool()
async def list_modal_environments() -> dict[str, Any]:
    """
    List all environments in the current Modal workspace.

    Returns:
        A dictionary containing the list of environments.
    """
    try:
        result = run_modal_command(["modal", "environment", "list", "--json"])
        response = handle_json_response(result, "Failed to list environments")
        if response["success"]:
            return {"success": True, "environments": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal environments: {e}")
        raise


@mcp.tool()
async def create_modal_environment(name: str) -> dict[str, Any]:
    """
    Create a new environment in the current Modal workspace.

    Args:
        name: Name for the new environment.

    Returns:
        A dictionary containing the result of the create operation.
    """
    try:
        result = run_modal_command(["modal", "environment", "create", name])
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to create environment: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully created environment {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to create Modal environment: {e}")
        raise


@mcp.tool()
async def delete_modal_environment(name: str) -> dict[str, Any]:
    """
    Delete an environment in the current Modal workspace.
    This deletes all apps in the environment irrevocably.

    Args:
        name: Name of the environment to delete. Case sensitive.

    Returns:
        A dictionary containing the result of the delete operation.
    """
    try:
        result = run_modal_command(["modal", "environment", "delete", "--confirm", name])
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to delete environment: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted environment {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to delete Modal environment: {e}")
        raise


@mcp.tool()
async def update_modal_environment(current_name: str, new_name: Optional[str] = None, web_suffix: Optional[str] = None) -> dict[str, Any]:
    """
    Update the name or web suffix of a Modal environment.

    Args:
        current_name: Current name of the environment.
        new_name: Optional new name for the environment.
        web_suffix: Optional new web suffix for the environment (empty string for no suffix).

    Returns:
        A dictionary containing the result of the update operation.
    """
    try:
        command = ["modal", "environment", "update", current_name]
        if new_name is not None:
            command.extend(["--set-name", new_name])
        if web_suffix is not None:
            command.extend(["--set-web-suffix", web_suffix])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to update environment: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully updated environment {current_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to update Modal environment: {e}")
        raise


# --- Dict Management ---

@mcp.tool()
async def list_modal_dicts(environment: Optional[str] = None) -> dict[str, Any]:
    """
    List all named Modal Dicts.

    Args:
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the list of Dicts.
    """
    try:
        command = ["modal", "dict", "list", "--json"]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list dicts")
        if response["success"]:
            return {"success": True, "dicts": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal dicts: {e}")
        raise


@mcp.tool()
async def create_modal_dict(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Create a named Modal Dict object.

    Args:
        name: Name for the new Dict.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the create operation.
    """
    try:
        command = ["modal", "dict", "create", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to create dict: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully created dict {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to create Modal dict: {e}")
        raise


@mcp.tool()
async def delete_modal_dict(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Delete a named Modal Dict and all of its data.

    Args:
        name: Name of the Dict to delete.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the delete operation.
    """
    try:
        command = ["modal", "dict", "delete", "--yes", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to delete dict: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted dict {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to delete Modal dict: {e}")
        raise


@mcp.tool()
async def clear_modal_dict(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Clear the contents of a named Modal Dict by deleting all of its data.

    Args:
        name: Name of the Dict to clear.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the clear operation.
    """
    try:
        command = ["modal", "dict", "clear", "--yes", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to clear dict: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully cleared dict {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to clear Modal dict: {e}")
        raise


@mcp.tool()
async def get_modal_dict_value(name: str, key: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Get the value for a specific key in a Modal Dict.

    Args:
        name: Name of the Dict.
        key: Key to look up.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the value for the key.
    """
    try:
        command = ["modal", "dict", "get", name, key]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        return result
    except Exception as e:
        logger.error(f"Failed to get Modal dict value: {e}")
        raise


@mcp.tool()
async def list_modal_dict_items(name: str, n: int = 20, show_all: bool = False, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Print the contents of a Modal Dict.

    Args:
        name: Name of the Dict.
        n: Limit the number of entries shown. Defaults to 20.
        show_all: If True, ignore n and print all entries (may be slow).
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the Dict items.
    """
    try:
        command = ["modal", "dict", "items", "--json", name]
        if show_all:
            command.append("--all")
        else:
            command.append(str(n))
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list dict items")
        if response["success"]:
            return {"success": True, "items": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal dict items: {e}")
        raise


# --- Queue Management ---

@mcp.tool()
async def list_modal_queues(environment: Optional[str] = None) -> dict[str, Any]:
    """
    List all named Modal Queues.

    Args:
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the list of Queues.
    """
    try:
        command = ["modal", "queue", "list", "--json"]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list queues")
        if response["success"]:
            return {"success": True, "queues": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal queues: {e}")
        raise


@mcp.tool()
async def create_modal_queue(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Create a named Modal Queue.

    Args:
        name: Name for the new Queue.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the create operation.
    """
    try:
        command = ["modal", "queue", "create", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to create queue: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully created queue {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to create Modal queue: {e}")
        raise


@mcp.tool()
async def delete_modal_queue(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Delete a named Modal Queue and all of its data.

    Args:
        name: Name of the Queue to delete.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the delete operation.
    """
    try:
        command = ["modal", "queue", "delete", "--yes", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to delete queue: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted queue {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to delete Modal queue: {e}")
        raise


@mcp.tool()
async def clear_modal_queue(name: str, partition: Optional[str] = None, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Clear the contents of a Modal Queue by removing all of its data.

    Args:
        name: Name of the Queue to clear.
        partition: Optional partition name to clear. Clears the default partition if not specified.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the clear operation.
    """
    try:
        command = ["modal", "queue", "clear", "--yes", name]
        if partition:
            command.extend(["--partition", partition])
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to clear queue: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully cleared queue {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to clear Modal queue: {e}")
        raise


@mcp.tool()
async def peek_modal_queue(name: str, n: int = 1, partition: Optional[str] = None, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Print the next N items in a Modal Queue or queue partition (without removal).

    Args:
        name: Name of the Queue.
        n: Number of items to peek at. Defaults to 1.
        partition: Optional partition name. Uses the default partition if not specified.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the peeked items.
    """
    try:
        command = ["modal", "queue", "peek", name, str(n)]
        if partition:
            command.extend(["--partition", partition])
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        return result
    except Exception as e:
        logger.error(f"Failed to peek Modal queue: {e}")
        raise


@mcp.tool()
async def get_modal_queue_length(name: str, partition: Optional[str] = None, total: bool = False, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Get the length of a Modal Queue partition or the total length of all partitions.

    Args:
        name: Name of the Queue.
        partition: Optional partition name. Uses the default partition if not specified.
        total: If True, compute the sum of queue lengths across all partitions.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the queue length.
    """
    try:
        command = ["modal", "queue", "len", name]
        if partition:
            command.extend(["--partition", partition])
        if total:
            command.append("--total")
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        return result
    except Exception as e:
        logger.error(f"Failed to get Modal queue length: {e}")
        raise


# --- NFS (Network File System) Management ---

@mcp.tool()
async def list_modal_nfs(environment: Optional[str] = None) -> dict[str, Any]:
    """
    List all Modal Network File Systems.

    Args:
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the list of network file systems.
    """
    try:
        command = ["modal", "nfs", "list", "--json"]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = handle_json_response(result, "Failed to list network file systems")
        if response["success"]:
            return {"success": True, "network_file_systems": response["data"]}
        return response
    except Exception as e:
        logger.error(f"Failed to list Modal NFS: {e}")
        raise


@mcp.tool()
async def create_modal_nfs(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Create a named Modal Network File System.

    Args:
        name: Name for the new network file system.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the create operation.
    """
    try:
        command = ["modal", "nfs", "create", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to create NFS: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully created network file system {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to create Modal NFS: {e}")
        raise


@mcp.tool()
async def delete_modal_nfs(name: str, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Delete a named, persistent Modal Network File System.

    Args:
        name: Name of the network file system to delete.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the delete operation.
    """
    try:
        command = ["modal", "nfs", "delete", "--yes", name]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to delete NFS: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted network file system {name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to delete Modal NFS: {e}")
        raise


@mcp.tool()
async def list_modal_nfs_contents(nfs_name: str, path: str = "/", environment: Optional[str] = None) -> dict[str, Any]:
    """
    List files and directories in a Modal Network File System.

    Args:
        nfs_name: Name of the network file system.
        path: Path within the NFS to list. Defaults to root ("/").
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the NFS contents.
    """
    try:
        command = ["modal", "nfs", "ls", nfs_name, path]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        return result
    except Exception as e:
        logger.error(f"Failed to list Modal NFS contents: {e}")
        raise


@mcp.tool()
async def put_modal_nfs_file(nfs_name: str, local_path: str, remote_path: str = "/", environment: Optional[str] = None) -> dict[str, Any]:
    """
    Upload a file or directory to a Modal Network File System.

    Args:
        nfs_name: Name of the network file system.
        local_path: Path to the local file or directory to upload.
        remote_path: Path in the NFS to upload to. Defaults to root ("/").
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the upload operation.
    """
    try:
        command = ["modal", "nfs", "put", nfs_name, local_path, remote_path]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to upload to NFS: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully uploaded {local_path} to {nfs_name}:{remote_path}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to upload to Modal NFS: {e}")
        raise


@mcp.tool()
async def get_modal_nfs_file(nfs_name: str, remote_path: str, local_destination: str = ".", environment: Optional[str] = None) -> dict[str, Any]:
    """
    Download a file from a Modal Network File System.

    Args:
        nfs_name: Name of the network file system.
        remote_path: Path to the file in the NFS to download.
        local_destination: Local path to save the downloaded file. Defaults to current directory.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the download operation.
    """
    try:
        command = ["modal", "nfs", "get", nfs_name, remote_path, local_destination]
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to download from NFS: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully downloaded {remote_path} from {nfs_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to download from Modal NFS: {e}")
        raise


@mcp.tool()
async def remove_modal_nfs_file(nfs_name: str, remote_path: str, recursive: bool = False, environment: Optional[str] = None) -> dict[str, Any]:
    """
    Delete a file or directory from a Modal Network File System.

    Args:
        nfs_name: Name of the network file system.
        remote_path: Path to the file or directory to delete.
        recursive: If True, delete directories recursively.
        environment: Optional Modal environment.

    Returns:
        A dictionary containing the result of the delete operation.
    """
    try:
        command = ["modal", "nfs", "rm"]
        if recursive:
            command.append("-r")
        command.extend([nfs_name, remote_path])
        if environment:
            command.extend(["--env", environment])
        result = run_modal_command(command)
        response = {
            "success": result["success"],
            "command": result["command"]
        }
        if not result["success"]:
            response["error"] = f"Failed to delete from NFS: {result.get('error', 'Unknown error')}"
        else:
            response["message"] = f"Successfully deleted {remote_path} from {nfs_name}"
        if result.get("stdout"):
            response["stdout"] = result["stdout"]
        if result.get("stderr"):
            response["stderr"] = result["stderr"]
        return response
    except Exception as e:
        logger.error(f"Failed to delete from Modal NFS: {e}")
        raise


if __name__ == "__main__":
    mcp.run()
