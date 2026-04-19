from __future__ import annotations

import csv
import json
import re
import socket
import time
import uuid
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyodbc
import streamlit as st


DATA_DIR = Path("data")
JOBS_DIR = DATA_DIR / "jobs"
JOBS_INDEX_FILE = DATA_DIR / "jobs_index.json"
DB_CONNECTIONS_FILE = DATA_DIR / "db_connections.json"
QUERY_PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_\.]*)\s*\}\}")
VARIABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_storage() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    if not JOBS_INDEX_FILE.exists():
        JOBS_INDEX_FILE.write_text("[]", encoding="utf-8")
    if not DB_CONNECTIONS_FILE.exists():
        DB_CONNECTIONS_FILE.write_text("[]", encoding="utf-8")


def parse_queries(raw_text: str) -> list[str]:
    blocks = [block.strip() for block in raw_text.split("\n---\n")]
    return [block for block in blocks if block]


def parse_query_variables(raw_text: str) -> tuple[dict[str, Any] | None, str | None]:
    if not raw_text.strip():
        return {}, None

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return None, f"Query Variables JSON is invalid: {exc}"

    if not isinstance(parsed, dict):
        return None, "Query Variables must be a JSON object, for example: {\"tenant_id\": 42}."

    return parsed, None


def rows_to_dicts(headers: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in rows:
        record: dict[str, Any] = {}
        for idx, header in enumerate(headers):
            record[header] = row[idx] if idx < len(row) else None
        records.append(record)
    return records


def to_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, bytes):
        return value.hex()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(item) for item in value]

    return str(value)


def get_value_by_path(data: dict[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = data
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return False, None
    return True, current


def parse_query_block(raw_query: str, query_index: int) -> dict[str, Any]:
    lines = raw_query.splitlines()
    name = f"q{query_index}"
    for_each: str | None = None
    item_alias = "item"
    for_mode = "combine"
    body_start = 0

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            body_start = idx + 1
            continue

        if not stripped.startswith("-- @"):
            body_start = idx
            break

        directive = stripped[4:].strip()
        if directive.startswith("name "):
            value = directive[5:].strip()
            if value:
                name = value
        elif directive.startswith("for_each "):
            value = directive[9:].strip()
            if value:
                for_each = value
        elif directive.startswith("item "):
            value = directive[5:].strip()
            if value:
                item_alias = value
        elif directive.startswith("for_mode "):
            value = directive[9:].strip().lower()
            if value in {"combine", "split"}:
                for_mode = value
        body_start = idx + 1

    sql_text = "\n".join(lines[body_start:]).strip()
    return {
        "name": name,
        "for_each": for_each,
        "item_alias": item_alias,
        "for_mode": for_mode,
        "sql": sql_text,
        "raw": raw_query,
    }


def parse_variable_value(raw_value: str) -> Any:
    value = raw_value.strip()
    if value == "":
        return ""

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return raw_value


def extract_query_variables(queries: list[str]) -> list[str]:
    names: set[str] = set()
    for query in queries:
        parsed = parse_query_block(query, 1)
        for match in QUERY_PLACEHOLDER_PATTERN.finditer(parsed["sql"]):
            variable_path = match.group(1)
            if "." not in variable_path:
                names.add(variable_path)
    return sorted(names)


def compile_query_template(query: str, variables: dict[str, Any]) -> tuple[str, list[Any], list[str]]:
    params: list[Any] = []
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        variable_path = match.group(1)
        found, value = get_value_by_path(variables, variable_path)
        if not found:
            if variable_path not in missing:
                missing.append(variable_path)
            return match.group(0)

        params.append(value)
        return "?"

    compiled_query = QUERY_PLACEHOLDER_PATTERN.sub(replace, query)
    return compiled_query, params, missing


def normalize_server_name(server: str) -> str:
    value = server.strip()
    if not value:
        return value

    lowered = value.lower()
    if lowered.startswith("tcp:"):
        value = value[4:]
    if lowered.startswith("https://"):
        value = value[8:]
    if lowered.startswith("http://"):
        value = value[7:]

    value = value.strip().rstrip("/")
    if "," in value:
        value = value.split(",", maxsplit=1)[0].strip()
    if ":" in value:
        value = value.split(":", maxsplit=1)[0].strip()

    if ".database.windows.net" not in value.lower():
        value = f"{value}.database.windows.net"

    return value


def build_connection_string(
    server: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> str:
    server_value = normalize_server_name(server)
    if not server_value:
        raise ValueError("Server name is required.")

    return (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server_value},{int(port)};"
        f"Database={database.strip()};"
        f"Uid={username.strip()};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
        "Authentication=ActiveDirectoryPassword;"
    )


def run_connectivity_check(server: str, port: int) -> dict[str, Any]:
    normalized_server = normalize_server_name(server)
    result: dict[str, Any] = {
        "server": normalized_server,
        "port": int(port),
        "dns_ok": False,
        "resolved_ip": None,
        "dns_error": None,
        "tcp_ok": False,
        "tcp_error": None,
    }

    if not normalized_server:
        result["dns_error"] = "Server is empty."
        return result

    try:
        resolved_ip = socket.gethostbyname(normalized_server)
        result["dns_ok"] = True
        result["resolved_ip"] = resolved_ip
    except OSError as exc:
        result["dns_error"] = str(exc)
        return result

    try:
        with socket.create_connection((normalized_server, int(port)), timeout=5):
            result["tcp_ok"] = True
    except OSError as exc:
        result["tcp_error"] = str(exc)

    return result


def check_db_connection(
    server: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    conn_str = build_connection_string(server, port, database, username, password)

    result: dict[str, Any] = {
        "ok": False,
        "error": None,
        "elapsed_seconds": None,
    }

    try:
        with pyodbc.connect(conn_str, timeout=60) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT 1 AS ConnectionOk")
            cursor.fetchone()

        elapsed = round(time.perf_counter() - started, 2)
        result["ok"] = True
        result["elapsed_seconds"] = elapsed
    except Exception as exc:  # pylint: disable=broad-exception-caught
        result["error"] = str(exc)

    return result


def load_jobs_index() -> list[dict[str, Any]]:
    ensure_storage()
    try:
        return json.loads(JOBS_INDEX_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def load_db_connections() -> list[dict[str, Any]]:
    ensure_storage()
    try:
        connections = json.loads(DB_CONNECTIONS_FILE.read_text(encoding="utf-8"))
        if isinstance(connections, list):
            return connections
        return []
    except json.JSONDecodeError:
        return []


def save_db_connections(connections: list[dict[str, Any]]) -> None:
    DB_CONNECTIONS_FILE.write_text(json.dumps(connections, indent=2), encoding="utf-8")


def save_jobs_index(index_data: list[dict[str, Any]]) -> None:
    JOBS_INDEX_FILE.write_text(json.dumps(index_data, indent=2), encoding="utf-8")


def write_csv(file_path: Path, headers: list[str], rows: list[tuple[Any, ...]]) -> None:
    with file_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def run_job(
    job_name: str,
    server: str,
    port: int,
    database: str,
    username: str,
    password: str,
    queries: list[str],
    query_variables: dict[str, Any],
) -> dict[str, Any]:
    ensure_storage()

    job_id = str(uuid.uuid4())
    created_at = utc_now_iso()
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    conn_str = build_connection_string(server, port, database, username, password)

    results: list[dict[str, Any]] = []
    query_specs = [parse_query_block(query, i) for i, query in enumerate(queries, start=1)]

    seen_names: set[str] = set()
    for spec in query_specs:
        name = spec["name"]
        if not VARIABLE_NAME_PATTERN.fullmatch(name):
            raise ValueError(
                f"Invalid query name '{name}'. Use letters, numbers, and underscores only."
            )
        if name in seen_names:
            raise ValueError(f"Duplicate query name: {name}")
        seen_names.add(name)

    context: dict[str, Any] = dict(query_variables)
    context["results"] = {}

    with pyodbc.connect(conn_str, timeout=60) as connection:
        cursor = connection.cursor()

        for i, spec in enumerate(query_specs, start=1):
            result: dict[str, Any] = {
                "query_index": i,
                "query_name": spec["name"],
                "query": spec["raw"],
                "compiled_query": None,
                "parameters": [],
                "for_each": spec["for_each"],
                "item_alias": spec["item_alias"],
                "for_mode": spec["for_mode"],
                "status": "success",
                "row_count": 0,
                "column_headers": [],
                "output_file": None,
                "loop_outputs": [],
                "error": None,
            }

            if not spec["sql"]:
                result["status"] = "error"
                result["error"] = "Query block is empty after directives."
                results.append(result)
                continue

            try:
                headers: list[str] = []
                all_rows: list[tuple[Any, ...]] = []

                if spec["for_each"]:
                    found, iterable = get_value_by_path(context, spec["for_each"])
                    if not found:
                        raise ValueError(
                            f"for_each source not found: {spec['for_each']}"
                        )
                    if not isinstance(iterable, list):
                        raise ValueError(
                            f"for_each source must be an array/list: {spec['for_each']}"
                        )

                    for item in iterable:
                        loop_index = len(result["loop_outputs"]) + 1
                        loop_context = dict(context)
                        loop_context[spec["item_alias"]] = item
                        compiled_query, params, missing_vars = compile_query_template(
                            spec["sql"], loop_context
                        )
                        if missing_vars:
                            raise ValueError(
                                "Missing variable values for: " + ", ".join(missing_vars)
                            )

                        result["compiled_query"] = compiled_query
                        result["parameters"].append(params)

                        if params:
                            cursor.execute(compiled_query, params)
                        else:
                            cursor.execute(compiled_query)

                        if cursor.description:
                            current_headers = [column[0] for column in cursor.description]
                            if not headers:
                                headers = current_headers
                            rows = cursor.fetchall()
                            all_rows.extend(rows)

                            if spec["for_mode"] == "split":
                                split_file = job_dir / f"query_{i}_run_{loop_index}.csv"
                                write_csv(split_file, current_headers, rows)
                                result["loop_outputs"].append(
                                    {
                                        "run_index": loop_index,
                                        "item": item,
                                        "row_count": len(rows),
                                        "output_file": str(split_file),
                                        "parameters": params,
                                    }
                                )
                else:
                    compiled_query, params, missing_vars = compile_query_template(spec["sql"], context)
                    if missing_vars:
                        raise ValueError(
                            "Missing variable values for: " + ", ".join(missing_vars)
                        )

                    result["compiled_query"] = compiled_query
                    result["parameters"] = params

                    if params:
                        cursor.execute(compiled_query, params)
                    else:
                        cursor.execute(compiled_query)

                    if cursor.description:
                        headers = [column[0] for column in cursor.description]
                        rows = cursor.fetchall()
                        all_rows.extend(rows)

                if headers:
                    result["row_count"] = len(all_rows)
                    result["column_headers"] = headers

                    if spec["for_each"] and spec["for_mode"] == "split":
                        result["output_file"] = None
                    else:
                        output_file = job_dir / f"query_{i}.csv"
                        write_csv(output_file, headers, all_rows)
                        result["output_file"] = str(output_file)

                    records = rows_to_dicts(headers, all_rows)
                    context["results"][spec["name"]] = records
                    context[spec["name"]] = {
                        "rows": records,
                        "first": records[0] if records else {},
                        "count": len(records),
                    }
                else:
                    result["status"] = "no_result_set"
                    result["row_count"] = cursor.rowcount if cursor.rowcount != -1 else 0
            except Exception as exc:  # pylint: disable=broad-exception-caught
                result["status"] = "error"
                result["error"] = str(exc)

            results.append(result)

    job_metadata = {
        "job_id": job_id,
        "job_name": job_name.strip(),
        "created_at": created_at,
        "azure_sql": {
            "server": server.strip(),
            "port": int(port),
            "database": database.strip(),
            "username": username.strip(),
            "authentication": "ActiveDirectoryPassword",
        },
        "query_count": len(queries),
        "query_variables": query_variables,
        "results": results,
    }

    safe_job_metadata = to_json_safe(job_metadata)
    (job_dir / "job.json").write_text(json.dumps(safe_job_metadata, indent=2), encoding="utf-8")

    jobs_index = load_jobs_index()
    jobs_index.append(
        {
            "job_id": job_id,
            "job_name": job_name.strip(),
            "created_at": created_at,
            "query_count": len(queries),
            "server": server.strip(),
            "port": int(port),
            "database": database.strip(),
        }
    )
    jobs_index.sort(key=lambda item: item["created_at"], reverse=True)
    save_jobs_index(jobs_index)

    return safe_job_metadata


def load_job_metadata(job_id: str) -> dict[str, Any] | None:
    job_file = JOBS_DIR / job_id / "job.json"
    if not job_file.exists():
        return None

    try:
        return json.loads(job_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def read_csv_preview(file_path: Path, max_rows: int = 200) -> tuple[list[str], list[list[str]]]:
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        headers = next(reader, [])
        rows = []
        for idx, row in enumerate(reader):
            if idx >= max_rows:
                break
            rows.append(row)
    return headers, rows


def rows_to_records(headers: list[str], rows: list[list[str]]) -> list[dict[str, Any]]:
    if not headers:
        return [{"value": row} for row in rows]

    normalized_headers: list[str] = []
    seen: dict[str, int] = {}
    for idx, header in enumerate(headers, start=1):
        candidate = header.strip() or f"column_{idx}"
        if candidate in seen:
            seen[candidate] += 1
            candidate = f"{candidate}_{seen[candidate]}"
        else:
            seen[candidate] = 1
        normalized_headers.append(candidate)

    records: list[dict[str, Any]] = []
    for row in rows:
        record: dict[str, Any] = {}
        for idx, key in enumerate(normalized_headers):
            record[key] = row[idx] if idx < len(row) else ""
        records.append(record)

    return records


def render_create_job_tab() -> None:
    st.subheader("Create Job")
    st.write(
        "Create a job by entering Azure SQL connection details and one or more SQL queries. "
        "Separate each query block with a line containing only `---`."
    )

    with st.popover("Help"):
        st.markdown("### Example Usage")
        st.markdown("Use placeholders in SQL with the format `{{variable_name}}`.")
        st.markdown(
            "Add optional directives at the top of each query block: `-- @name`, `-- @for_each`, `-- @item`, `-- @for_mode`."
        )
        st.code(
            """-- @name users
SELECT UserId, Region
FROM dbo.Users
WHERE TenantId = {{tenant_id}};
---
-- @name orders_by_user
-- @for_each users.rows
-- @item user
-- @for_mode split
SELECT OrderId, UserId, Amount
FROM dbo.Orders
WHERE UserId = {{user.UserId}};""",
            language="sql",
        )
        st.markdown("Provide values in **Query Variables (JSON)**:")
        st.code(
            """{
  "tenant_id": 42,
  "start_date": "2026-01-01"
}""",
            language="json",
        )
        st.caption(
            "Tip: `for_mode` defaults to `combine`. Set `-- @for_mode split` to save one output file per loop run."
        )

    if "query_variables_builder" not in st.session_state:
        st.session_state["query_variables_builder"] = {}
    if "query_variables_raw_json" not in st.session_state:
        st.session_state["query_variables_raw_json"] = "{}"
    if "variable_input_mode" not in st.session_state:
        st.session_state["variable_input_mode"] = "Raw JSON"
    if "create_job_name" not in st.session_state:
        st.session_state["create_job_name"] = ""
    if "create_job_queries" not in st.session_state:
        st.session_state["create_job_queries"] = ""

    st.markdown("#### Load From Previous Run")
    previous_jobs = load_jobs_index()
    if previous_jobs:
        previous_job_options = {
            f"{job['job_name']} | {job['created_at']} | {job['job_id']}": job["job_id"]
            for job in previous_jobs
        }
        selected_previous_job_label = st.selectbox(
            "Select Previous Run",
            options=list(previous_job_options.keys()),
            key="selected_previous_job_label",
        )
        if st.button("Auto Populate From Selected Run"):
            selected_previous_job_id = previous_job_options[selected_previous_job_label]
            previous_metadata = load_job_metadata(selected_previous_job_id)
            if not previous_metadata:
                st.error("Could not load metadata for selected run.")
            else:
                previous_results = sorted(
                    previous_metadata.get("results", []),
                    key=lambda item: item.get("query_index", 0),
                )
                previous_queries = [
                    item.get("query", "") for item in previous_results if item.get("query")
                ]
                st.session_state["create_job_queries"] = "\n---\n".join(previous_queries)
                st.session_state["create_job_name"] = (
                    f"{previous_metadata.get('job_name', 'Job')} Copy"
                )

                previous_variables = previous_metadata.get("query_variables", {})
                if isinstance(previous_variables, dict):
                    st.session_state["query_variables_builder"] = previous_variables
                    st.session_state["query_variables_raw_json"] = json.dumps(
                        previous_variables, indent=2
                    )
                else:
                    st.session_state["query_variables_builder"] = {}
                    st.session_state["query_variables_raw_json"] = "{}"
                st.session_state["variable_input_mode"] = "Raw JSON"

                previous_connection = previous_metadata.get("azure_sql", {})
                all_connections = load_db_connections()
                matching_connection = next(
                    (
                        conn
                        for conn in all_connections
                        if normalize_server_name(conn.get("server", ""))
                        == normalize_server_name(previous_connection.get("server", ""))
                        and int(conn.get("port", 1433))
                        == int(previous_connection.get("port", 1433))
                        and conn.get("database", "") == previous_connection.get("database", "")
                        and conn.get("username", "") == previous_connection.get("username", "")
                    ),
                    None,
                )
                if matching_connection:
                    st.session_state["create_selected_connection_id"] = matching_connection[
                        "connection_id"
                    ]

                st.success("Populated SQL queries and variables from previous run.")
                st.rerun()
    else:
        st.caption("No previous runs available yet.")

    st.markdown("#### Query Variables")
    variable_input_mode = st.radio(
        "Variable Input Mode",
        options=["Raw JSON", "UI Builder"],
        horizontal=True,
        key="variable_input_mode",
        help="Choose Raw JSON or add variables one-by-one using the UI Builder.",
    )

    if variable_input_mode == "Raw JSON":
        st.text_area(
            "Query Variables (JSON)",
            height=120,
            key="query_variables_raw_json",
            help=(
                "Use placeholders in SQL like {{tenant_id}} and provide values here as JSON. "
                "Example: {\"tenant_id\": 42, \"start_date\": \"2026-01-01\"}"
            ),
        )
    else:
        if st.session_state["query_variables_builder"]:
            st.write("Current Variables")
            st.json(st.session_state["query_variables_builder"])
        else:
            st.caption("No variables added yet.")

        var_col_1, var_col_2 = st.columns(2)
        with var_col_1:
            builder_variable_name = st.text_input(
                "Variable Name",
                placeholder="tenant_id",
                key="builder_variable_name",
            )
        with var_col_2:
            builder_variable_value = st.text_input(
                "Variable Value",
                placeholder="42 or 2026-01-01 or true",
                key="builder_variable_value",
            )

        btn_col_1, btn_col_2 = st.columns(2)
        with btn_col_1:
            add_variable_clicked = st.button("Add Variable")
        with btn_col_2:
            clear_variables_clicked = st.button("Clear Variables")

        if clear_variables_clicked:
            st.session_state["query_variables_builder"] = {}
            st.session_state["query_variables_raw_json"] = "{}"
            st.success("Cleared all variables from UI Builder.")

        if add_variable_clicked:
            name = builder_variable_name.strip()
            if not name:
                st.error("Variable Name is required.")
            elif not VARIABLE_NAME_PATTERN.fullmatch(name):
                st.error(
                    "Variable Name must use letters, numbers, and underscores, and cannot start with a number."
                )
            else:
                value = parse_variable_value(builder_variable_value)
                st.session_state["query_variables_builder"][name] = value
                st.session_state["query_variables_raw_json"] = json.dumps(
                    st.session_state["query_variables_builder"], indent=2
                )
                st.success(f"Added variable '{name}'.")

    connections = load_db_connections()
    if not connections:
        st.warning("No DB connection saved yet. Use the DB Connections tab to add one.")
        return

    connection_options = {
        conn["connection_id"]: (
            f"{conn['connection_name']} | {conn['server']}:{conn.get('port', 1433)} | "
            f"{conn['database']} | {conn['username']}"
        )
        for conn in connections
    }
    if "create_selected_connection_id" not in st.session_state:
        st.session_state["create_selected_connection_id"] = next(iter(connection_options))

    if st.session_state["create_selected_connection_id"] not in connection_options:
        st.session_state["create_selected_connection_id"] = next(iter(connection_options))

    selected_connection_id = st.selectbox(
        "Select DB Connection",
        options=list(connection_options.keys()),
        format_func=lambda conn_id: connection_options[conn_id],
        key="create_selected_connection_id",
    )
    selected_connection = next(
        (conn for conn in connections if conn["connection_id"] == selected_connection_id),
        None,
    )
    if selected_connection is None:
        st.error("Selected connection could not be loaded.")
        return

    with st.form("create_job_form"):
        job_name = st.text_input(
            "Job Name", placeholder="Daily Sales Snapshot", key="create_job_name"
        )
        raw_queries = st.text_area(
            "SQL Queries",
            height=220,
            key="create_job_queries",
            placeholder=(
                "SELECT TOP 10 * FROM dbo.Customers;\n"
                "---\n"
                "SELECT COUNT(*) AS TotalOrders FROM dbo.Orders;"
            ),
        )

        check_clicked = st.form_submit_button("Check DB Connection")
        run_clicked = st.form_submit_button("Run Job")

    if not check_clicked and not run_clicked:
        return

    server = selected_connection["server"]
    port = int(selected_connection.get("port", 1433))
    database = selected_connection["database"]
    username = selected_connection["username"]
    password = selected_connection["password"]

    if check_clicked:
        with st.spinner("Checking Azure SQL connection..."):
            check_result = check_db_connection(server, int(port), database, username, password)

        if check_result["ok"]:
            st.success(
                f"Connection successful. Response time: {check_result['elapsed_seconds']}s"
            )
        else:
            connection_error = str(check_result["error"])
            st.error(f"Connection failed: {connection_error}")

            check = run_connectivity_check(server, int(port))
            with st.expander("Connection Diagnostics", expanded=True):
                st.write(f"Normalized server: {check['server']}")
                st.write(f"Port: {check['port']}")
                st.write(f"DNS resolved: {check['dns_ok']}")
                if check["resolved_ip"]:
                    st.write(f"Resolved IP: {check['resolved_ip']}")
                if check["dns_error"]:
                    st.write(f"DNS error: {check['dns_error']}")

                st.write(f"TCP {check['port']} reachable: {check['tcp_ok']}")
                if check["tcp_error"]:
                    st.write(f"TCP error: {check['tcp_error']}")

            if "HYT00" in connection_error:
                st.markdown(
                    """
Possible causes to check:
- Server name is incorrect. Use only server name or full *.database.windows.net host.
- Azure SQL Server firewall/network rules do not allow this client IP.
- Private endpoint or VNet rules block public access to port 1433.
- Azure AD account requires MFA/Conditional Access, which can block ActiveDirectoryPassword login.
- Corporate proxy/VPN is blocking outbound SQL traffic.
                    """
                )
        return

    queries = parse_queries(raw_queries)
    if not job_name.strip():
        st.error("Job name is required.")
        return
    if not queries:
        st.error("Enter at least one SQL query.")
        return

    if variable_input_mode == "Raw JSON":
        query_variables, variables_error = parse_query_variables(
            st.session_state["query_variables_raw_json"]
        )
        if variables_error:
            st.error(variables_error)
            return
        assert query_variables is not None
    else:
        query_variables = dict(st.session_state["query_variables_builder"])

    required_variable_names = extract_query_variables(queries)
    missing_variable_names = [name for name in required_variable_names if name not in query_variables]
    if missing_variable_names:
        st.error("Missing query variable values for: " + ", ".join(missing_variable_names))
        return

    with st.spinner("Running job and saving query outputs..."):
        try:
            metadata = run_job(
                job_name,
                server,
                int(port),
                database,
                username,
                password,
                queries,
                query_variables,
            )
            success_count = len([r for r in metadata["results"] if r["status"] == "success"])
            error_count = len([r for r in metadata["results"] if r["status"] == "error"])
            st.success(
                f"Job created: {metadata['job_name']} ({metadata['job_id']}). "
                f"Successful queries: {success_count}, errors: {error_count}."
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            st.error(f"Job execution failed: {exc}")

            connection_error = str(exc)
            if "HYT00" in connection_error:
                st.warning("Azure SQL login timed out. Review connection diagnostics below.")

                check = run_connectivity_check(server, int(port))
                with st.expander("Connection Diagnostics", expanded=True):
                    st.write(f"Normalized server: {check['server']}")
                    st.write(f"Port: {check['port']}")
                    st.write(f"DNS resolved: {check['dns_ok']}")
                    if check["resolved_ip"]:
                        st.write(f"Resolved IP: {check['resolved_ip']}")
                    if check["dns_error"]:
                        st.write(f"DNS error: {check['dns_error']}")

                    st.write(f"TCP {check['port']} reachable: {check['tcp_ok']}")
                    if check["tcp_error"]:
                        st.write(f"TCP error: {check['tcp_error']}")

                st.markdown(
                    """
Possible causes to check:
- Server name is incorrect. Use only server name or full *.database.windows.net host.
- Azure SQL Server firewall/network rules do not allow this client IP.
- Private endpoint or VNet rules block public access to port 1433.
- Azure AD account requires MFA/Conditional Access, which can block ActiveDirectoryPassword login.
- Corporate proxy/VPN is blocking outbound SQL traffic.
                    """
                )


def render_view_results_tab() -> None:
    st.subheader("View Job Results")
    jobs = load_jobs_index()

    if not jobs:
        st.info("No jobs created yet.")
        return

    labels = {
        f"{job['job_name']} | {job['created_at']} | {job['job_id']}": job["job_id"]
        for job in jobs
    }
    selected_label = st.selectbox("Select Job", options=list(labels.keys()))
    job_id = labels[selected_label]

    metadata = load_job_metadata(job_id)
    if not metadata:
        st.error("Could not load the selected job metadata.")
        return

    st.write(f"**Job:** {metadata['job_name']}")
    st.write(f"**Created (UTC):** {metadata['created_at']}")
    st.write(f"**Server:** {metadata['azure_sql']['server']}")
    st.write(f"**Port:** {metadata['azure_sql'].get('port', 1433)}")
    st.write(f"**Database:** {metadata['azure_sql']['database']}")
    st.write(f"**Auth:** {metadata['azure_sql']['authentication']}")
    if metadata.get("query_variables"):
        st.write("**Query Variables:**")
        st.json(metadata["query_variables"])

    for query_result in metadata.get("results", []):
        title = (
            f"Query {query_result['query_index']} | "
            f"Status: {query_result['status']} | "
            f"Rows: {query_result['row_count']}"
        )
        with st.expander(title, expanded=False):
            st.write("Template Query")
            st.code(query_result["query"], language="sql")
            if query_result.get("parameters"):
                st.write("Parameters")
                st.json(query_result["parameters"])
            if query_result.get("for_each"):
                st.write(
                    f"Loop Source: {query_result.get('for_each')} | Mode: {query_result.get('for_mode', 'combine')}"
                )

            if query_result.get("error"):
                st.error(query_result["error"])

            output_file = query_result.get("output_file")
            if output_file:
                csv_path = Path(output_file)
                if csv_path.exists():
                    headers, rows = read_csv_preview(csv_path)
                    st.dataframe(rows_to_records(headers, rows), use_container_width=True)

                    st.download_button(
                        label=f"Download CSV for Query {query_result['query_index']}",
                        data=csv_path.read_bytes(),
                        file_name=csv_path.name,
                        mime="text/csv",
                    )
                else:
                    st.warning("Saved result file was not found on disk.")

            loop_outputs = query_result.get("loop_outputs") or []
            if loop_outputs:
                st.write("Loop Outputs")
                for loop_output in loop_outputs:
                    loop_file = Path(loop_output["output_file"])
                    st.markdown(
                        f"**Run {loop_output['run_index']}** | Rows: {loop_output['row_count']}"
                    )
                    st.write("Loop Item")
                    st.json(loop_output.get("item"))

                    if loop_output.get("parameters"):
                        st.write("Run Parameters")
                        st.json(loop_output["parameters"])

                    if loop_file.exists():
                        headers, rows = read_csv_preview(loop_file)
                        st.dataframe(rows_to_records(headers, rows), use_container_width=True)
                        st.download_button(
                            label=f"Download CSV for Run {loop_output['run_index']}",
                            data=loop_file.read_bytes(),
                            file_name=loop_file.name,
                            mime="text/csv",
                            key=(
                                f"download_{query_result['query_index']}_{loop_output['run_index']}"
                            ),
                        )
                    else:
                        st.warning("Saved loop output file was not found on disk.")

                    st.divider()


def render_db_connections_tab() -> None:
    st.subheader("DB Connections")
    st.write("Save Azure SQL connections and reuse them in the Create Job page.")

    with st.form("add_db_connection_form"):
        connection_name = st.text_input("Connection Name", placeholder="Prod Reporting")
        server = st.text_input("Azure SQL Server", placeholder="myserver.database.windows.net")
        port = st.number_input("Port", min_value=1, max_value=65535, value=1433, step=1)
        database = st.text_input("Database", placeholder="SalesDb")
        username = st.text_input("Azure AD Username (UPN)", placeholder="user@contoso.com")
        password = st.text_input("Azure AD Password", type="password")

        save_clicked = st.form_submit_button("Save Connection")
        test_clicked = st.form_submit_button("Test and Save")

    if save_clicked or test_clicked:
        if (
            not connection_name.strip()
            or not server.strip()
            or not database.strip()
            or not username.strip()
            or not password
        ):
            st.error("All connection fields are required.")
            return

        if test_clicked:
            with st.spinner("Testing connection..."):
                test_result = check_db_connection(server, int(port), database, username, password)
            if not test_result["ok"]:
                st.error(f"Connection test failed: {test_result['error']}")
                return
            st.success(
                f"Connection test successful. Response time: {test_result['elapsed_seconds']}s"
            )

        connections = load_db_connections()
        duplicate = next(
            (
                conn
                for conn in connections
                if conn["connection_name"].strip().lower() == connection_name.strip().lower()
            ),
            None,
        )

        if duplicate:
            duplicate.update(
                {
                    "server": normalize_server_name(server),
                    "port": int(port),
                    "database": database.strip(),
                    "username": username.strip(),
                    "password": password,
                    "updated_at": utc_now_iso(),
                }
            )
            st.success(f"Updated connection '{connection_name.strip()}'.")
        else:
            connections.append(
                {
                    "connection_id": str(uuid.uuid4()),
                    "connection_name": connection_name.strip(),
                    "server": normalize_server_name(server),
                    "port": int(port),
                    "database": database.strip(),
                    "username": username.strip(),
                    "password": password,
                    "created_at": utc_now_iso(),
                }
            )
            st.success(f"Saved connection '{connection_name.strip()}'.")

        connections.sort(key=lambda item: item["connection_name"].lower())
        save_db_connections(connections)

    existing_connections = load_db_connections()
    if existing_connections:
        st.markdown("### Saved Connections")
        for conn in existing_connections:
            with st.expander(
                f"{conn['connection_name']} | {conn['server']}:{conn.get('port', 1433)} | {conn['database']}",
                expanded=False,
            ):
                st.write(f"Server: {conn['server']}")
                st.write(f"Port: {conn.get('port', 1433)}")
                st.write(f"Database: {conn['database']}")
                st.write(f"Username: {conn['username']}")
                st.caption("Password is saved and hidden.")
    else:
        st.info("No saved connections yet.")


def main() -> None:
    st.set_page_config(page_title="Azure SQL Job Runner", layout="wide")
    ensure_storage()

    st.title("Azure SQL Job Runner")
    st.caption(
        "Run and save outputs for multiple SQL queries per job using Azure SQL + Azure Active Directory Password authentication."
    )

    create_tab, view_tab, connections_tab = st.tabs(
        ["Create Job", "View Results", "DB Connections"]
    )

    with create_tab:
        render_create_job_tab()

    with view_tab:
        render_view_results_tab()

    with connections_tab:
        render_db_connections_tab()


if __name__ == "__main__":
    main()
