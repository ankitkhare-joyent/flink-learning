"""DuckDB on your laptop, attached to the Iceberg lakehouse through Lakekeeper (the same path the MCP server uses).

    mise run -q lab:duck                                  # a few starter queries
    mise run -q lab:duck -- "select count(*) from lake.ec2_stream.state_changes"
"""
import sys

import duckdb

db = duckdb.connect()
db.execute("SET TimeZone = 'UTC'")
db.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
# Local SeaweedFS keys (dev-only; stack/seaweedfs/iam.json). In AWS: IAM credentials, nothing in code.
db.execute("""CREATE SECRET (TYPE s3, KEY_ID 's3admin', SECRET 's3admin-local-secret', ENDPOINT 'localhost:19000',
              URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
db.execute("""ATTACH 'lakehouse' AS lake (TYPE iceberg, ENDPOINT 'http://localhost:18181/catalog',
              AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')""")

queries = sys.argv[1:] or [
    "select count(*) as raw_rows from lake.ec2_stream.state_changes_raw",
    "select count(*) as clean_rows from lake.ec2_stream.state_changes",
    "select state, count(*) as n from lake.ec2_stream.state_changes group by 1 order by 2 desc",
]
for q in queries:
    print(">", q)
    print(db.sql(q))
