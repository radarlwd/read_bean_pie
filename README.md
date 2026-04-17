# Azure SQL Job Runner

This tool provides a UI where users can:

- Create a job.
- Enter one or more SQL queries for that job.
- Execute the queries against Azure SQL.
- Save each query output with column headers.
- View saved results for any previous job.

## Authentication

The app connects to Azure SQL using:

- Authentication method: `ActiveDirectoryPassword`

Users enter Azure SQL connection values in the UI:

- Server
- Database
- Azure AD username (UPN)
- Azure AD password

The password is used only for the connection and is not stored on disk.

## Prerequisites

1. Python 3.10+
2. ODBC Driver 18 for SQL Server installed on macOS

Install ODBC Driver 18 on macOS (Homebrew):

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18
```

## Run

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Start the app:

```bash
python -m streamlit run app.py
```

4. Open the URL shown by Streamlit (usually `http://localhost:8501`).

## Troubleshooting

- If you get `ModuleNotFoundError: No module named 'pyodbc'`, you are likely using the wrong Python interpreter.
- Verify the active interpreter is the project virtual env:

```bash
which python
python -c "import pyodbc; print(pyodbc.version)"
```

- In VS Code, select interpreter: `.venv/bin/python`.

## Query Input Format

In the Create Job page, separate multiple queries with a line containing only:

```text
---
```

Example:

```sql
SELECT TOP 10 * FROM dbo.Customers;
---
SELECT COUNT(*) AS TotalOrders FROM dbo.Orders;
```

## Data Storage

Saved data is written under:

- `data/jobs_index.json` for job list metadata
- `data/jobs/<job_id>/job.json` for per-job details
- `data/jobs/<job_id>/query_<n>.csv` for query outputs
