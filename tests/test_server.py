"""Unit tests for the Modal MCP server.

All tool functions are tested by mocking `run_modal_command` so no real
Modal CLI calls are made.  The tests verify:
  - correct CLI commands are assembled (including flags / optional args)
  - JSON-returning tools parse and reshape the response correctly
  - plain-text-returning tools pass through the raw result
  - error / failure paths propagate the expected structure
"""
import json
import subprocess
from typing import Any
from unittest.mock import patch

import pytest

from modal_mcp.server import (
    # helpers
    run_modal_command,
    handle_json_response,
    # deploy
    deploy_modal_app,
    # volume file ops
    list_modal_volumes,
    list_modal_volume_contents,
    copy_modal_volume_files,
    remove_modal_volume_file,
    put_modal_volume_file,
    get_modal_volume_file,
    # volume management
    create_modal_volume,
    delete_modal_volume,
    rename_modal_volume,
    # app management
    list_modal_apps,
    get_modal_app_logs,
    stop_modal_app,
    get_modal_app_history,
    rollback_modal_app,
    # container management
    list_modal_containers,
    get_modal_container_logs,
    exec_modal_container,
    stop_modal_container,
    # secret management
    list_modal_secrets,
    create_modal_secret,
    # environment management
    list_modal_environments,
    create_modal_environment,
    delete_modal_environment,
    update_modal_environment,
    # dict management
    list_modal_dicts,
    create_modal_dict,
    delete_modal_dict,
    clear_modal_dict,
    get_modal_dict_value,
    list_modal_dict_items,
    # queue management
    list_modal_queues,
    create_modal_queue,
    delete_modal_queue,
    clear_modal_queue,
    peek_modal_queue,
    get_modal_queue_length,
    # nfs management
    list_modal_nfs,
    create_modal_nfs,
    delete_modal_nfs,
    list_modal_nfs_contents,
    put_modal_nfs_file,
    get_modal_nfs_file,
    remove_modal_nfs_file,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

MOCK_PATH = "modal_mcp.server.run_modal_command"


def _ok(stdout: str = "", stderr: str = "") -> dict[str, Any]:
    """Return a successful run_modal_command result."""
    return {
        "success": True,
        "stdout": stdout,
        "stderr": stderr,
        "command": "modal test",
    }


def _ok_json(data: Any) -> dict[str, Any]:
    """Return a successful result whose stdout is JSON-encoded *data*."""
    return _ok(stdout=json.dumps(data))


def _timed_out(stdout: str = "", stderr: str = "") -> dict[str, Any]:
    """Return a timed-out run_modal_command result (streaming commands)."""
    return {
        "success": True,
        "timed_out": True,
        "stdout": stdout,
        "stderr": stderr,
        "command": "modal test",
    }


def _fail(error: str = "boom", stderr: str = "") -> dict[str, Any]:
    """Return a failed run_modal_command result."""
    return {
        "success": False,
        "error": error,
        "stdout": "",
        "stderr": stderr,
        "command": "modal test",
    }


# ---------------------------------------------------------------------------
# run_modal_command
# ---------------------------------------------------------------------------

class TestRunModalCommand:
    @patch("modal_mcp.server.subprocess.run")
    def test_timeout_captures_output(self, mock_subprocess_run: Any) -> None:
        mock_subprocess_run.side_effect = subprocess.TimeoutExpired(
            cmd=["modal", "app", "logs", "my-app"],
            timeout=5,
            output=b"line1\nline2",
            stderr=b"warn",
        )
        result = run_modal_command(["modal", "app", "logs", "my-app"], timeout=5)
        assert result["success"] is True
        assert result["timed_out"] is True
        assert result["stdout"] == "line1\nline2"
        assert result["stderr"] == "warn"

    @patch("modal_mcp.server.subprocess.run")
    def test_timeout_handles_none_output(self, mock_subprocess_run: Any) -> None:
        mock_subprocess_run.side_effect = subprocess.TimeoutExpired(
            cmd=["modal", "app", "logs", "x"],
            timeout=5,
        )
        result = run_modal_command(["modal", "app", "logs", "x"], timeout=5)
        assert result["success"] is True
        assert result["timed_out"] is True
        assert result["stdout"] == ""
        assert result["stderr"] == ""

    @patch("modal_mcp.server.subprocess.run")
    def test_no_timeout_by_default(self, mock_subprocess_run: Any) -> None:
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=["modal", "volume", "list"], returncode=0, stdout="ok", stderr=""
        )
        run_modal_command(["modal", "volume", "list"])
        mock_subprocess_run.assert_called_once()
        call_kwargs = mock_subprocess_run.call_args[1]
        assert call_kwargs["timeout"] is None

    @patch("modal_mcp.server.subprocess.run")
    def test_invalid_timeout_returns_failure_without_spawning_process(
        self,
        mock_subprocess_run: Any,
    ) -> None:
        result = run_modal_command(["modal", "app", "logs", "my-app"], timeout=0)

        assert result["success"] is False
        assert "Invalid timeout" in result["error"]
        assert result["command"] == "modal app logs my-app"
        mock_subprocess_run.assert_not_called()

    @patch("modal_mcp.server.subprocess.run")
    def test_file_not_found_returns_standardized_failure(
        self,
        mock_subprocess_run: Any,
    ) -> None:
        mock_subprocess_run.side_effect = FileNotFoundError("[Errno 2] No such file or directory: 'modal'")

        result = run_modal_command(["modal", "volume", "list"])

        assert result["success"] is False
        assert "No such file or directory" in result["error"]
        assert result["stdout"] == ""
        assert result["stderr"] == ""

    @patch("modal_mcp.server.subprocess.run")
    def test_called_process_error_uses_redacted_display_command(
        self,
        mock_subprocess_run: Any,
    ) -> None:
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["modal", "secret", "create", "my-secret", "KEY=actual-value"],
            output="",
            stderr="error",
        )

        result = run_modal_command(
            ["modal", "secret", "create", "my-secret", "KEY=actual-value"],
            display_command=["modal", "secret", "create", "my-secret", "KEY=<REDACTED>"],
        )

        assert result["success"] is False
        assert "KEY=<REDACTED>" in result["error"]
        assert "KEY=actual-value" not in result["error"]
        assert result["command"] == "modal secret create my-secret KEY=<REDACTED>"


# ---------------------------------------------------------------------------
# handle_json_response
# ---------------------------------------------------------------------------

class TestHandleJsonResponse:
    def test_success(self) -> None:
        result = _ok_json({"key": "value"})
        resp = handle_json_response(result, "prefix")
        assert resp == {"success": True, "data": {"key": "value"}}

    def test_command_failure(self) -> None:
        result = _fail("cmd error", stderr="details")
        resp = handle_json_response(result, "prefix")
        assert resp["success"] is False
        assert "prefix" in resp["error"]
        assert resp["stderr"] == "details"

    def test_invalid_json(self) -> None:
        result = _ok(stdout="not json")
        resp = handle_json_response(result, "prefix")
        assert resp["success"] is False
        assert "parse JSON" in resp["error"]


# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------

class TestDeploy:
    @pytest.mark.asyncio
    async def test_deploy_modal_app(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("Deployed!")) as mock:
            result = await deploy_modal_app("/home/user/project/app.py")
            mock.assert_called_once_with(
                ["modal", "deploy", "app.py"], "/home/user/project"
            )
            assert result["success"] is True


# ---------------------------------------------------------------------------
# Volume file operations (original tools)
# ---------------------------------------------------------------------------

class TestVolumeFileOps:
    @pytest.mark.asyncio
    async def test_list_volumes(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"name": "v1"}])) as mock:
            result = await list_modal_volumes()
            mock.assert_called_once_with(["modal", "volume", "list", "--json"])
            assert result == {"success": True, "volumes": [{"name": "v1"}]}

    @pytest.mark.asyncio
    async def test_list_volume_contents(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"filename": "a.txt"}])) as mock:
            result = await list_modal_volume_contents("myvol", "/data")
            mock.assert_called_once_with(
                ["modal", "volume", "ls", "--json", "myvol", "/data"]
            )
            assert result["success"] is True
            assert result["contents"] == [{"filename": "a.txt"}]

    @pytest.mark.asyncio
    async def test_copy_files_requires_two_paths(self) -> None:
        result = await copy_modal_volume_files("vol", ["only_one"])
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_copy_files(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await copy_modal_volume_files("vol", ["a.txt", "b.txt"])
            mock.assert_called_once_with(
                ["modal", "volume", "cp", "vol", "a.txt", "b.txt"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_remove_file(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await remove_modal_volume_file("vol", "/old.txt")
            mock.assert_called_once_with(["modal", "volume", "rm", "vol", "/old.txt"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_remove_file_recursive(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await remove_modal_volume_file("vol", "/dir", recursive=True)
            mock.assert_called_once_with(
                ["modal", "volume", "rm", "-r", "vol", "/dir"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_put_file(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await put_modal_volume_file("vol", "/tmp/f.txt", "/dest/")
            mock.assert_called_once_with(
                ["modal", "volume", "put", "vol", "/tmp/f.txt", "/dest/"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_put_file_force(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await put_modal_volume_file("vol", "/tmp/f.txt", "/dest/", force=True)
            mock.assert_called_once_with(
                ["modal", "volume", "put", "-f", "vol", "/tmp/f.txt", "/dest/"]
            )

    @pytest.mark.asyncio
    async def test_get_file(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await get_modal_volume_file("vol", "/data.csv", "/tmp")
            mock.assert_called_once_with(
                ["modal", "volume", "get", "vol", "/data.csv", "/tmp"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_file_force(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await get_modal_volume_file("vol", "/data.csv", "/tmp", force=True)
            mock.assert_called_once_with(
                ["modal", "volume", "get", "--force", "vol", "/data.csv", "/tmp"]
            )


# ---------------------------------------------------------------------------
# Volume management (create / delete / rename)
# ---------------------------------------------------------------------------

class TestVolumeManagement:
    @pytest.mark.asyncio
    async def test_create(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await create_modal_volume("newvol")
            mock.assert_called_once_with(["modal", "volume", "create", "newvol"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_create_with_env(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await create_modal_volume("newvol", environment="staging")
            mock.assert_called_once_with(
                ["modal", "volume", "create", "newvol", "--env", "staging"]
            )

    @pytest.mark.asyncio
    async def test_delete(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await delete_modal_volume("oldvol")
            mock.assert_called_once_with(
                ["modal", "volume", "delete", "--yes", "oldvol"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_rename(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await rename_modal_volume("old", "new")
            mock.assert_called_once_with(
                ["modal", "volume", "rename", "--yes", "old", "new"]
            )
            assert result["success"] is True


# ---------------------------------------------------------------------------
# App management
# ---------------------------------------------------------------------------

class TestAppManagement:
    @pytest.mark.asyncio
    async def test_list_apps(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"app": "a"}])) as mock:
            result = await list_modal_apps()
            mock.assert_called_once_with(["modal", "app", "list", "--json"])
            assert result == {"success": True, "apps": [{"app": "a"}]}

    @pytest.mark.asyncio
    async def test_list_apps_with_env(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([])) as mock:
            await list_modal_apps(environment="prod")
            mock.assert_called_once_with(
                ["modal", "app", "list", "--json", "--env", "prod"]
            )

    @pytest.mark.asyncio
    async def test_get_app_logs(self) -> None:
        with patch(MOCK_PATH, return_value=_timed_out("log line 1\nlog line 2")) as mock:
            result = await get_modal_app_logs("my-app")
            mock.assert_called_once_with(["modal", "app", "logs", "my-app"], timeout=30)
            assert result["success"] is True
            assert result["timed_out"] is True

    @pytest.mark.asyncio
    async def test_get_app_logs_custom_timeout(self) -> None:
        with patch(MOCK_PATH, return_value=_timed_out()) as mock:
            await get_modal_app_logs("my-app", timeout=10)
            mock.assert_called_once_with(["modal", "app", "logs", "my-app"], timeout=10)

    @pytest.mark.asyncio
    async def test_stop_app(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await stop_modal_app("my-app")
            mock.assert_called_once_with(["modal", "app", "stop", "my-app"])
            assert result["success"] is True
            assert "Successfully stopped" in result["message"]

    @pytest.mark.asyncio
    async def test_stop_app_failure(self) -> None:
        with patch(MOCK_PATH, return_value=_fail("not found")):
            result = await stop_modal_app("bad-app")
            assert result["success"] is False
            assert "error" in result

    @pytest.mark.asyncio
    async def test_get_app_history(self) -> None:
        history = [{"version": "v1"}, {"version": "v2"}]
        with patch(MOCK_PATH, return_value=_ok_json(history)) as mock:
            result = await get_modal_app_history("my-app")
            mock.assert_called_once_with(
                ["modal", "app", "history", "--json", "my-app"]
            )
            assert result == {"success": True, "history": history}

    @pytest.mark.asyncio
    async def test_rollback_no_version(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await rollback_modal_app("my-app")
            mock.assert_called_once_with(["modal", "app", "rollback", "my-app"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_rollback_with_version(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await rollback_modal_app("my-app", version="v3")
            mock.assert_called_once_with(
                ["modal", "app", "rollback", "my-app", "v3"]
            )
            assert "v3" in result["message"]


# ---------------------------------------------------------------------------
# Container management
# ---------------------------------------------------------------------------

class TestContainerManagement:
    @pytest.mark.asyncio
    async def test_list_containers(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"id": "c1"}])) as mock:
            result = await list_modal_containers()
            mock.assert_called_once_with(
                ["modal", "container", "list", "--json"]
            )
            assert result == {"success": True, "containers": [{"id": "c1"}]}

    @pytest.mark.asyncio
    async def test_get_container_logs(self) -> None:
        with patch(MOCK_PATH, return_value=_timed_out("logs")) as mock:
            result = await get_modal_container_logs("c-123")
            mock.assert_called_once_with(["modal", "container", "logs", "c-123"], timeout=30)
            assert result["success"] is True
            assert result["timed_out"] is True

    @pytest.mark.asyncio
    async def test_get_container_logs_custom_timeout(self) -> None:
        with patch(MOCK_PATH, return_value=_timed_out()) as mock:
            await get_modal_container_logs("c-123", timeout=5)
            mock.assert_called_once_with(["modal", "container", "logs", "c-123"], timeout=5)

    @pytest.mark.asyncio
    async def test_exec_container(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("hello")) as mock:
            result = await exec_modal_container("c-123", ["echo", "hello"])
            mock.assert_called_once_with(
                ["modal", "container", "exec", "c-123", "--", "echo", "hello"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_stop_container(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await stop_modal_container("c-123")
            mock.assert_called_once_with(["modal", "container", "stop", "c-123"])
            assert result["success"] is True
            assert "Successfully stopped" in result["message"]


# ---------------------------------------------------------------------------
# Secret management
# ---------------------------------------------------------------------------

class TestSecretManagement:
    @pytest.mark.asyncio
    async def test_list_secrets(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"name": "s1"}])) as mock:
            result = await list_modal_secrets()
            mock.assert_called_once_with(["modal", "secret", "list", "--json"])
            assert result == {"success": True, "secrets": [{"name": "s1"}]}

    @pytest.mark.asyncio
    async def test_create_secret(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await create_modal_secret("my-secret", {"KEY": "val"})
            mock.assert_called_once()
            args = mock.call_args[0][0]
            kwargs = mock.call_args[1]

            assert args == ["modal", "secret", "create", "my-secret", "KEY=val"]
            assert kwargs["display_command"] == [
                "modal",
                "secret",
                "create",
                "my-secret",
                "KEY=<REDACTED>",
            ]
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_create_secret_force_with_env(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await create_modal_secret(
                "s", {"A": "1", "B": "2"}, environment="dev", force=True
            )
            args = mock.call_args[0][0]
            kwargs = mock.call_args[1]
            display_command = kwargs["display_command"]

            assert "--force" in args
            assert "--env" in args
            assert "A=1" in args
            assert "B=2" in args

            assert "A=<REDACTED>" in display_command
            assert "B=<REDACTED>" in display_command
            assert "A=1" not in display_command
            assert "B=2" not in display_command


# ---------------------------------------------------------------------------
# Environment management
# ---------------------------------------------------------------------------

class TestEnvironmentManagement:
    @pytest.mark.asyncio
    async def test_list_environments(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"name": "main"}])) as mock:
            result = await list_modal_environments()
            mock.assert_called_once_with(
                ["modal", "environment", "list", "--json"]
            )
            assert result == {"success": True, "environments": [{"name": "main"}]}

    @pytest.mark.asyncio
    async def test_create_environment(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await create_modal_environment("staging")
            mock.assert_called_once_with(
                ["modal", "environment", "create", "staging"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_delete_environment(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await delete_modal_environment("staging")
            mock.assert_called_once_with(
                ["modal", "environment", "delete", "--confirm", "staging"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_update_environment_name(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await update_modal_environment("old", new_name="new")
            mock.assert_called_once_with(
                ["modal", "environment", "update", "old", "--set-name", "new"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_update_environment_suffix(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await update_modal_environment("env", web_suffix="beta")
            mock.assert_called_once_with(
                ["modal", "environment", "update", "env", "--set-web-suffix", "beta"]
            )

    @pytest.mark.asyncio
    async def test_update_environment_both(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await update_modal_environment("env", new_name="e2", web_suffix="")
            mock.assert_called_once_with(
                ["modal", "environment", "update", "env",
                 "--set-name", "e2", "--set-web-suffix", ""]
            )


# ---------------------------------------------------------------------------
# Dict management
# ---------------------------------------------------------------------------

class TestDictManagement:
    @pytest.mark.asyncio
    async def test_list_dicts(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"name": "d1"}])) as mock:
            result = await list_modal_dicts()
            mock.assert_called_once_with(["modal", "dict", "list", "--json"])
            assert result == {"success": True, "dicts": [{"name": "d1"}]}

    @pytest.mark.asyncio
    async def test_create_dict(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await create_modal_dict("mydict")
            mock.assert_called_once_with(["modal", "dict", "create", "mydict"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_delete_dict(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await delete_modal_dict("mydict")
            mock.assert_called_once_with(
                ["modal", "dict", "delete", "--yes", "mydict"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_clear_dict(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await clear_modal_dict("mydict")
            mock.assert_called_once_with(
                ["modal", "dict", "clear", "--yes", "mydict"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_dict_value(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("42")) as mock:
            result = await get_modal_dict_value("mydict", "counter")
            mock.assert_called_once_with(["modal", "dict", "get", "mydict", "counter"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_list_dict_items_default(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json({"a": 1})) as mock:
            result = await list_modal_dict_items("mydict")
            mock.assert_called_once_with(
                ["modal", "dict", "items", "--json", "mydict", "20"]
            )
            assert result == {"success": True, "items": {"a": 1}}

    @pytest.mark.asyncio
    async def test_list_dict_items_custom_n(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json({})) as mock:
            await list_modal_dict_items("mydict", n=5)
            mock.assert_called_once_with(
                ["modal", "dict", "items", "--json", "mydict", "5"]
            )

    @pytest.mark.asyncio
    async def test_list_dict_items_all(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json({})) as mock:
            await list_modal_dict_items("mydict", show_all=True)
            mock.assert_called_once_with(
                ["modal", "dict", "items", "--json", "mydict", "--all"]
            )


# ---------------------------------------------------------------------------
# Queue management
# ---------------------------------------------------------------------------

class TestQueueManagement:
    @pytest.mark.asyncio
    async def test_list_queues(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"name": "q1"}])) as mock:
            result = await list_modal_queues()
            mock.assert_called_once_with(["modal", "queue", "list", "--json"])
            assert result == {"success": True, "queues": [{"name": "q1"}]}

    @pytest.mark.asyncio
    async def test_create_queue(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await create_modal_queue("myq")
            mock.assert_called_once_with(["modal", "queue", "create", "myq"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_delete_queue(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await delete_modal_queue("myq")
            mock.assert_called_once_with(
                ["modal", "queue", "delete", "--yes", "myq"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_clear_queue(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await clear_modal_queue("myq")
            mock.assert_called_once_with(
                ["modal", "queue", "clear", "--yes", "myq"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_clear_queue_with_partition(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await clear_modal_queue("myq", partition="p1")
            mock.assert_called_once_with(
                ["modal", "queue", "clear", "--yes", "myq", "--partition", "p1"]
            )

    @pytest.mark.asyncio
    async def test_peek_queue(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("item1")) as mock:
            result = await peek_modal_queue("myq", n=3)
            mock.assert_called_once_with(["modal", "queue", "peek", "myq", "3"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_queue_length(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("5")) as mock:
            result = await get_modal_queue_length("myq")
            mock.assert_called_once_with(["modal", "queue", "len", "myq"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_queue_length_total(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("15")) as mock:
            await get_modal_queue_length("myq", total=True)
            mock.assert_called_once_with(
                ["modal", "queue", "len", "myq", "--total"]
            )

    @pytest.mark.asyncio
    async def test_get_queue_length_partition(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("3")) as mock:
            await get_modal_queue_length("myq", partition="p1")
            mock.assert_called_once_with(
                ["modal", "queue", "len", "myq", "--partition", "p1"]
            )


# ---------------------------------------------------------------------------
# NFS management
# ---------------------------------------------------------------------------

class TestNfsManagement:
    @pytest.mark.asyncio
    async def test_list_nfs(self) -> None:
        with patch(MOCK_PATH, return_value=_ok_json([{"name": "n1"}])) as mock:
            result = await list_modal_nfs()
            mock.assert_called_once_with(["modal", "nfs", "list", "--json"])
            assert result == {"success": True, "network_file_systems": [{"name": "n1"}]}

    @pytest.mark.asyncio
    async def test_create_nfs(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await create_modal_nfs("mynfs")
            mock.assert_called_once_with(["modal", "nfs", "create", "mynfs"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_delete_nfs(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await delete_modal_nfs("mynfs")
            mock.assert_called_once_with(
                ["modal", "nfs", "delete", "--yes", "mynfs"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_list_nfs_contents(self) -> None:
        with patch(MOCK_PATH, return_value=_ok("file1\nfile2")) as mock:
            result = await list_modal_nfs_contents("mynfs", "/data")
            mock.assert_called_once_with(["modal", "nfs", "ls", "mynfs", "/data"])
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_put_nfs_file(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await put_modal_nfs_file("mynfs", "/tmp/f.txt", "/dest/")
            mock.assert_called_once_with(
                ["modal", "nfs", "put", "mynfs", "/tmp/f.txt", "/dest/"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_get_nfs_file(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await get_modal_nfs_file("mynfs", "/data.csv", "/tmp")
            mock.assert_called_once_with(
                ["modal", "nfs", "get", "mynfs", "/data.csv", "/tmp"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_remove_nfs_file(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await remove_modal_nfs_file("mynfs", "/old.txt")
            mock.assert_called_once_with(
                ["modal", "nfs", "rm", "mynfs", "/old.txt"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_remove_nfs_file_recursive(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            result = await remove_modal_nfs_file("mynfs", "/dir", recursive=True)
            mock.assert_called_once_with(
                ["modal", "nfs", "rm", "-r", "mynfs", "/dir"]
            )
            assert result["success"] is True

    @pytest.mark.asyncio
    async def test_nfs_with_env(self) -> None:
        with patch(MOCK_PATH, return_value=_ok()) as mock:
            await create_modal_nfs("mynfs", environment="prod")
            mock.assert_called_once_with(
                ["modal", "nfs", "create", "mynfs", "--env", "prod"]
            )
