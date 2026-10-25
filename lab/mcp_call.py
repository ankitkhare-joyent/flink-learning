"""Talk to the streaming MCP server the way an MCP client (Claude Code, the triage agent) does. No Claude call.

    source lab/env.sh && mise run -q lab:mcp                 # list tools, call each once, then two bad calls
    source lab/env.sh && mise run -q lab:mcp -- i-0abc...    # look at one instance

The bearer token is read from OpenBao with $BAO_TOKEN (set by lab/env.sh) and never printed.
"""
import asyncio
import json
import os
import sys

import httpx
import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

URL = os.environ.get("MCP_URL", "http://localhost:18002/mcp")


def mcp_token() -> str:
    r = httpx.get(f"{os.environ.get('BAO_ADDR', 'http://localhost:18200')}/v1/secret/data/ec2-streaming/mcp",
                  headers={"X-Vault-Token": os.environ["BAO_TOKEN"]})
    r.raise_for_status()
    return r.json()["data"]["data"]["bearer_token"]


def short(result) -> str:
    return json.dumps(result.structured_content)[:300] if result.structured_content else result.content[0].text[:300]


async def main() -> None:
    http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {mcp_token()}"}, timeout=60)
    async with http, streamable_http_client(URL, http_client=http) as (r, w), ClientSession(r, w) as s:
        init = await s.initialize()
        print("server:", init.server_info.name, init.server_info.version, "| protocol:", init.protocol_version)
        for t in (await s.list_tools()).tools:
            print(f"tool {t.name}: read-only={t.annotations.read_only_hint}, inputs={sorted(t.input_schema.get('properties', {}))}")
        health = await s.call_tool("pipeline_health", {})
        print("\npipeline_health:", health.structured_content["status"], health.structured_content["problems"])
        iid = sys.argv[1] if len(sys.argv) > 1 else None
        if iid is None:
            busy = await s.call_tool("find_instances", {"min_transitions_1h": 6, "limit": 1})
            found = busy.structured_content["instances"] or (await s.call_tool("find_instances", {"limit": 1})).structured_content["instances"]
            iid = found[0]["instance_id"]
        state = (await s.call_tool("get_instance_state", {"instance_id": iid})).structured_content
        print(f"\nget_instance_state {iid}: {state['state']} (was {state['previous_state']}) since {state['state_since']}, "
              f"owner {state['owner_email']}, {state['transitions_1h']} transitions/h, row is {state['freshness_seconds']} s old")
        tl = (await s.call_tool("instance_timeline", {"instance_id": iid, "hours": 1, "limit": 5})).structured_content
        print(f"instance_timeline: {len(tl['events'])} events, newest {tl['events'][0]['event_time'] if tl['events'] else '-'}")
        act = (await s.call_tool("fleet_activity", {"minutes": 5})).structured_content
        print("fleet_activity (5 min):", act["groups"], "latest window end", act["latest_window_end"])
        al = (await s.call_tool("recent_alerts", {"limit": 3})).structured_content
        print("recent_alerts:", [(a["alert_type"], a["instance_id"], bool(a.get("triage"))) for a in al["alerts"]])
        bad = await s.call_tool("get_instance_state", {"instance_id": "i-not-real"})
        print("\nbad input: isError =", bad.is_error, "|", bad.content[0].text[:120].replace("\n", " "))
    async with httpx2.AsyncClient() as c:
        resp = await c.post(URL, json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                            headers={"Accept": "application/json, text/event-stream"})
        print("no token:", resp.status_code, resp.headers.get("www-authenticate", "")[:80])


if __name__ == "__main__":
    asyncio.run(main())
