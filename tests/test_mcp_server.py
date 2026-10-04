"""The MCP server's contract with agents: which tools exist, that they are read-only, and their input limits."""
from mcp import Client

from services.mcp_server.server import build_server

EXPECTED = {"get_instance_state", "find_instances", "instance_timeline", "fleet_activity", "recent_alerts",
            "pipeline_health"}


async def test_tools_are_few_task_shaped_and_read_only():
    async with Client(build_server("stdio")) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}
    assert set(tools) == EXPECTED
    for t in tools.values():
        assert t.annotations.read_only_hint is True and t.annotations.destructive_hint is False
        assert t.description and t.output_schema, t.name


async def test_inputs_are_constrained():
    async with Client(build_server("stdio")) as client:
        tools = {t.name: t for t in (await client.list_tools()).tools}
    iid = tools["get_instance_state"].input_schema["properties"]["instance_id"]
    assert iid["pattern"].startswith("^i-")
    limit = tools["find_instances"].input_schema["properties"]["limit"]
    assert limit["maximum"] == 50


async def test_bad_instance_id_is_rejected_before_any_query():
    async with Client(build_server("stdio")) as client:
        result = await client.call_tool("get_instance_state", {"instance_id": "not-an-instance"})
    assert result.is_error
