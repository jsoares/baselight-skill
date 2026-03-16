#!/usr/bin/env python3
"""Baselight MCP client. Queries return CSV; everything else returns JSON.

Commands:
    ping
    search_catalog   <query> [--category CAT] [--limit N]
    search_tables    <query> [--category CAT] [--limit N]
    dataset_metadata <dataset_id>
    dataset_tables   <dataset_id> [--query Q] [--page N]
    table_metadata   <table_id>
    query            <sql>
    get_results      <job_id> [--limit N] [--offset N] [--poll]

Credentials (checked in order):
    1. BASELIGHT_API_KEY environment variable
    2. ~/.baselight/credentials file  (BASELIGHT_API_KEY=...)

Generate a key at: https://baselight.app → Account Settings → Integrations
"""

import argparse
import csv
import io
import json
import os
import sys
import time
import requests

MCP_URL = "https://api.baselight.app/mcp"
PROTOCOL_VERSION = "2025-03-26"
CREDENTIALS_FILE = os.path.expanduser("~/.baselight/credentials")


# ── Credentials ───────────────────────────────────────────────────────

def load_api_key() -> str | None:
    """Load API key from env var, then credentials file."""
    key = os.environ.get("BASELIGHT_API_KEY")
    if key:
        return key
    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE) as f:
            for line in f:
                line = line.strip()
                if line.startswith("BASELIGHT_API_KEY="):
                    return line.split("=", 1)[1].strip()
    return None


# ── MCP Client ────────────────────────────────────────────────────────

class BaselightClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session_id = None
        self.request_id = 0
        self.http = requests.Session()
        self.http.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "x-api-key": self.api_key,
        })

    def _next_id(self) -> int:
        self.request_id += 1
        return self.request_id

    def _post(self, method: str, params: dict | None = None,
              is_notification: bool = False) -> dict | None:
        body = {"jsonrpc": "2.0", "method": method}
        if not is_notification:
            body["id"] = self._next_id()
        if params is not None:
            body["params"] = params

        headers = {}
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id

        resp = self.http.post(MCP_URL, json=body, headers=headers, timeout=120)
        resp.raise_for_status()

        if "Mcp-Session-Id" in resp.headers:
            self.session_id = resp.headers["Mcp-Session-Id"]

        if is_notification:
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "text/event-stream" in content_type:
            return self._parse_sse(resp.text)
        if resp.text.strip():
            return resp.json()
        return None

    def _parse_sse(self, text: str) -> dict | None:
        """Return the first complete result/error event from an SSE stream."""
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("data:"):
                data_str = line[5:].strip()
                if data_str:
                    try:
                        parsed = json.loads(data_str)
                        if "result" in parsed or "error" in parsed:
                            return parsed
                    except json.JSONDecodeError as e:
                        print(f"Warning: failed to parse SSE frame: {e} — data: {data_str!r}", file=sys.stderr)
        return None

    def initialize(self):
        resp = self._post("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "baselight-skill", "version": "1.0.0"},
        })
        if resp and "error" in resp:
            raise RuntimeError(f"Initialize failed: {resp['error']}")
        self._post("notifications/initialized", is_notification=True)
        return resp

    def call_tool(self, tool_name: str, arguments: dict) -> dict:
        resp = self._post("tools/call", {
            "name": tool_name,
            "arguments": arguments,
        })
        if resp is None:
            raise RuntimeError("No response from server")
        if "error" in resp:
            raise RuntimeError(f"Tool error: {resp['error']}")
        return resp.get("result", resp)


# ── Output formatting ─────────────────────────────────────────────────

def extract_text(result: dict) -> str:
    """Pull the text content out of an MCP tool result."""
    if "content" not in result:
        return json.dumps(result, indent=2)
    parts = []
    for block in result["content"]:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block["text"])
    return "\n".join(parts) if parts else json.dumps(result, indent=2)


def format_output(raw_text: str) -> str:
    """If the response is a query result (columns + rows), return CSV.
    Otherwise return the original JSON."""
    try:
        data = json.loads(raw_text)
    except (json.JSONDecodeError, TypeError):
        return raw_text

    # Detect query results: top-level has .result.columns and .result.rows
    r = data.get("result") if isinstance(data, dict) else None
    if not (r and isinstance(r, dict) and "columns" in r and "rows" in r):
        # Warn if the response looks like a query result but has an unexpected shape,
        # so callers know the data is present but in an unrecognised format.
        if isinstance(data, dict) and any(k in data for k in ("state", "resultId", "jobId")):
            present = ", ".join(data.keys())
            print(
                f"Warning: response looks like a query result but has an unexpected shape "
                f"(keys: {present}). Raw JSON returned — check for API changes.",
                file=sys.stderr,
            )
        return json.dumps(data, indent=2)

    # Build metadata comment
    state = data.get("state", "DONE")
    showing = r.get("showing", "")
    total = r.get("totalResults", "")
    job_id = data.get("resultId", "")
    meta = f"# state: {state}, showing: {showing}, total: {total}"
    if job_id:
        meta += f", jobId: {job_id}"

    # Build CSV
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(r["columns"])
    for row in r["rows"]:
        writer.writerow(row)

    return meta + "\n" + buf.getvalue()


def is_pending(output: str) -> bool:
    """Return True if the formatted output indicates a PENDING query."""
    first_line = output.split("\n")[0] if output else ""
    return "state: PENDING" in first_line


def extract_job_id(output: str) -> str | None:
    """Extract jobId from the metadata comment line."""
    first_line = output.split("\n")[0] if output else ""
    for part in first_line.split(","):
        part = part.strip()
        if part.startswith("jobId:"):
            return part.split(":", 1)[1].strip()
    return None


# ── CLI ───────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="baselight",
        description="Baselight MCP client. Queries return CSV; everything else returns JSON.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ping", help="Check connectivity")

    p = sub.add_parser("search_catalog", help="Search the dataset catalog")
    p.add_argument("query")
    p.add_argument("--category")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("search_tables", help="Search for tables")
    p.add_argument("query")
    p.add_argument("--category")
    p.add_argument("--limit", type=int)

    p = sub.add_parser("dataset_metadata", help="Get dataset metadata")
    p.add_argument("dataset_id")

    p = sub.add_parser("dataset_tables", help="List tables in a dataset")
    p.add_argument("dataset_id")
    p.add_argument("--query")
    p.add_argument("--page", type=int)

    p = sub.add_parser("table_metadata", help="Get table schema and column info")
    p.add_argument("table_id")

    p = sub.add_parser("query", help="Execute a SQL query")
    p.add_argument("sql", nargs="?", help="SQL string, or '-' to read from stdin")

    p = sub.add_parser("get_results", help="Fetch results for a query job")
    p.add_argument("job_id")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument(
        "--poll", action="store_true",
        help="Retry every 3s until the query is no longer PENDING",
    )

    return parser


def main():
    api_key = load_api_key()
    if not api_key:
        print("Error: BASELIGHT_API_KEY is not set.", file=sys.stderr)
        print(
            f"Generate one at: https://baselight.app → Account Settings → Integrations\n"
            f"Then either:\n"
            f"  export BASELIGHT_API_KEY=<key>   (current session only)\n"
            f"  echo 'BASELIGHT_API_KEY=<key>' >> {CREDENTIALS_FILE}   (persistent)",
            file=sys.stderr,
        )
        sys.exit(1)

    parser = build_parser()
    args = parser.parse_args()

    client = BaselightClient(api_key)
    try:
        client.initialize()
    except Exception as e:
        print(f"Error connecting to Baselight: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        result = dispatch(client, args)
        raw_text = extract_text(result)
        output = format_output(raw_text)

        # Auto-poll if --poll requested and query is still PENDING
        if getattr(args, "poll", False) and is_pending(output):
            job_id = extract_job_id(output) or args.job_id
            print("# Polling for results...", file=sys.stderr)
            max_polls = 20
            polls = 0
            while is_pending(output):
                if polls >= max_polls:
                    print(
                        f"Error: query still PENDING after {max_polls} retries (~{max_polls * 3}s). "
                        "Try adding filters or aggregation to reduce query scope.",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                time.sleep(3)
                polls += 1
                result = client.call_tool("baselight_sdk_get_results", {
                    "jobId": job_id,
                    "limit": args.limit,
                    "offset": args.offset,
                })
                raw_text = extract_text(result)
                output = format_output(raw_text)

        print(output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def dispatch(client: BaselightClient, args: argparse.Namespace) -> dict:
    cmd = args.command

    if cmd == "ping":
        return client.call_tool("baselight_ping", {})

    elif cmd == "search_catalog":
        tool_args = {"query": args.query}
        if args.category:
            tool_args["category"] = [args.category]
        if args.limit:
            tool_args["limit"] = args.limit
        return client.call_tool("baselight_search_catalog", tool_args)

    elif cmd == "search_tables":
        tool_args = {"query": args.query}
        if args.category:
            tool_args["category"] = [args.category]
        if args.limit:
            tool_args["limit"] = args.limit
        return client.call_tool("baselight_search_tables", tool_args)

    elif cmd == "dataset_metadata":
        return client.call_tool("baselight_get_dataset_metadata", {"id": args.dataset_id})

    elif cmd == "dataset_tables":
        tool_args = {"id": args.dataset_id}
        if args.query:
            tool_args["query"] = args.query
        if args.page:
            tool_args["page"] = args.page
        return client.call_tool("baselight_get_dataset_tables", tool_args)

    elif cmd == "table_metadata":
        return client.call_tool("baselight_get_table_metadata", {"id": args.table_id})

    elif cmd == "query":
        sql = sys.stdin.read().strip() if (args.sql is None or args.sql == "-") else args.sql
        if not sql:
            raise RuntimeError("No SQL provided. Pass a query string or pipe SQL via stdin.")
        return client.call_tool("baselight_sdk_query_execute", {"sql": sql})

    elif cmd == "get_results":
        return client.call_tool("baselight_sdk_get_results", {
            "jobId": args.job_id,
            "limit": args.limit,
            "offset": args.offset,
        })

    else:
        raise RuntimeError(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
