"""Print a local-only credential so you can explore the stack. Never do this with real secrets.

    docker compose run --rm --no-deps bao-bootstrap python -m scripts.local_creds root-token
    docker compose run --rm --no-deps bao-bootstrap python -m scripts.local_creds mcp-token
"""
import json
import os
import sys

import hvac

INIT_FILE = os.environ.get("BAO_INIT_FILE", "/bao-init/init.json")


def main() -> None:
    what = sys.argv[1] if len(sys.argv) > 1 else ""
    with open(INIT_FILE) as f:
        root = json.load(f)["root_token"]
    if what == "root-token":
        print(root)
    elif what == "mcp-token":
        c = hvac.Client(url=os.environ.get("BAO_ADDR", "http://openbao:8200"), token=root)
        print(c.secrets.kv.v2.read_secret_version(path="ec2-streaming/mcp", mount_point="secret",
                                                  raise_on_deleted_version=True)["data"]["data"]["bearer_token"])
    else:
        raise SystemExit("usage: python -m scripts.local_creds root-token|mcp-token")


if __name__ == "__main__":
    main()
