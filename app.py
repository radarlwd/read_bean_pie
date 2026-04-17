from __future__ import annotations

import csv
import json
import socket
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyodbc
import streamlit as st


DATA_DIR = Path("data")
JOBS_DIR = DATA_DIR / "jobs"
JOBS_INDEX_FILE = DATA_DIR / "jobs_index.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_storage() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    if not JOBS_INDEX_FILE.exists():
        JOBS_INDEX_FILE.write_text("[]", encoding="utf-8")


def parse_queries(raw_text: str) -> list[str]:
    blocks = [block.strip() for block in raw_text.split("\n---\n")]
    return [block for block in blocks if block]


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
) -> dict[str, Any]:
    ensure_storage()

    job_id = str(uuid.uuid4())
    created_at = utc_now_iso()
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    conn_str = build_connection_string(server, port, database, username, password)

    results: list[dict[str, Any]] = []
    with pyodbc.connect(conn_str, timeout=60) as connection:
        cursor = connection.cursor()

        for i, query in enumerate(queries, start=1):
            result: dict[str, Any] = {
                "query_index": i,
                "query": query,
                "status": "success",
                "row_count": 0,
                "column_headers": [],
                "output_file": None,
                "error": None,
            }
            try:
                cursor.execute(query)
                if cursor.description:
                    headers = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    output_file = job_dir / f"query_{i}.csv"
                    write_csv(output_file, headers, rows)

                    result["row_count"] = len(rows)
                    result["column_headers"] = headers
                    result["output_file"] = str(output_file)
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
        "results": results,
    }

    (job_dir / "job.json").write_text(json.dumps(job_metadata, indent=2), encoding="utf-8")

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

    return job_metadata


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

    with st.form("create_job_form"):
        job_name = st.text_input("Job Name", placeholder="Daily Sales Snapshot")
        server = st.text_input("Azure SQL Server", placeholder="myserver.database.windows.net")
        port = st.number_input("Port", min_value=1, max_value=65535, value=1433, step=1)
        database = st.text_input("Database", placeholder="SalesDb")
        username = st.text_input("Azure AD Username (UPN)", placeholder="user@contoso.com")
        password = st.text_input("Azure AD Password", type="password")
        raw_queries = st.text_area(
            "SQL Queries",
            height=220,
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

    if not server.strip() or not database.strip() or not username.strip() or not password:
        st.error("All Azure SQL connection fields are required.")
        return

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

    with st.spinner("Running job and saving query outputs..."):
        try:
            metadata = run_job(job_name, server, int(port), database, username, password, queries)
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

    for query_result in metadata.get("results", []):
        title = (
            f"Query {query_result['query_index']} | "
            f"Status: {query_result['status']} | "
            f"Rows: {query_result['row_count']}"
        )
        with st.expander(title, expanded=False):
            st.code(query_result["query"], language="sql")

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


def main() -> None:
    st.set_page_config(page_title="Azure SQL Job Runner", layout="wide")
    ensure_storage()

    st.title("Azure SQL Job Runner")
    st.caption(
        "Run and save outputs for multiple SQL queries per job using Azure SQL + Azure Active Directory Password authentication."
    )

    create_tab, view_tab = st.tabs(["Create Job", "View Results"])

    with create_tab:
        render_create_job_tab()

    with view_tab:
        render_view_results_tab()


if __name__ == "__main__":
    main()
