#!/usr/bin/env python3
"""Explore bronze tables and print counts/statistics."""
import json
import subprocess
import time
import sys

# Read config to get token/workspace
with open(r"C:\Users\thiago\.databrickscfg", "r") as f:
    cfg = f.read()

# Find the [imersao] profile section
in_imersao = False
host = None
for line in cfg.splitlines():
    line = line.strip()
    if line.startswith("[imersao]"):
        in_imersao = True
    elif line.startswith("[") and line.endswith("]"):
        in_imersao = False
    elif in_imersao and "host" in line.lower():
        host = line.split("=")[1].strip()

print(f"Workspace host: {host}")

# Use the Databricks SQL Statements API
# First, submit a query
import base64

# Try using databricks api with a file-based JSON approach
query_json = {
    "warehouse_id": "a115299e7388a922",
    "statement": "SELECT count(*) as total FROM proyectoecommerce.bronze.vendas",
    "response_format": "JSON_ARRAY"
}

json_str = json.dumps(query_json)
print(f"JSON to send: {json_str}")

# Write JSON to temp file
import tempfile
import os
tmpfile = os.path.join(tempfile.gettempdir(), "bronze_query.json")
with open(tmpfile, "w") as f:
    f.write(json_str)

# Use databricks api with @file reference
result = subprocess.run(
    ["databricks", "api", "post", "/api/2.1/sql/statements", 
     "--profile", "imersao",
     "--json", f"@{tmpfile}"],
    capture_output=True, text=True, timeout=30
)
print(f"STDOUT: {result.stdout}")
print(f"STDERR: {result.stderr}")
print(f"Return code: {result.returncode}")

if result.returncode == 0 and result.stdout:
    # Poll for completion
    import re
    match = re.search(r'"statement_id"\s*:\s*"([^"]+)"', result.stdout)
    if match:
        stmt_id = match.group(1)
        print(f"Statement ID: {stmt_id}")
        
        # Poll for results
        for i in range(10):
            time.sleep(2)
            poll = subprocess.run(
                ["databricks", "api", "get", f"/api/2.1/sql/statements/{stmt_id}",
                 "--profile", "imersao",
                 "--output", "json"],
                capture_output=True, text=True, timeout=30
            )
            print(f"Poll {i}: {poll.stdout[:200]}...")
            if poll.returncode == 0:
                import json as jj
                try:
                    poll_data = jj.loads(poll.stdout)
                    state = poll_data.get("state", {}).get("life_cycle_state", "")
                    print(f"State: {state}")
                    if state == "FINISHED":
                        result_data = poll_data.get("result", {})
                        print(f"Result: {jj.dumps(result_data, indent=2)[:1000]}")
                        break
                except:
                    pass
    else:
        print("Could not find statement_id in response")
else:
    print("Failed to submit query")

# Cleanup
try:
    os.unlink(tmpfile)
except:
    pass