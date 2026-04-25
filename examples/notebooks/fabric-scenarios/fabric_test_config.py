"""Fabric integration sweep — connection configuration.

Endpoints are not secrets (public infrastructure identifiers).
Secrets are resolved at runtime via CredentialResolver with env:// protocol.
Auth is CLI-based (az login) — each writer acquires Entra ID tokens automatically.
"""

# ── Workspace ────────────────────────────────────────────────────────────────
WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
TENANT_ID = "2536810f-20e1-4911-a453-4409fd96db8a"

# ── Lakehouse ────────────────────────────────────────────────────────────────
LAKEHOUSE_ID = "3a17ecc6-795e-4496-a3b9-581dab931054"
ONELAKE_BASE = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"
)

# ── SQL Database ─────────────────────────────────────────────────────────────
SQL_DB_SERVER = (
    "b6atmjpbeaiutjctiqe73fw3ri-po6a3gor6xeexkjjtx6vbgs5ki"
    ".database.fabric.microsoft.com,1433"
)
SQL_DB_NAME = "FabricDemo_SQL_DB-e0fe6f55-b020-4bd1-9cce-aea43d83bd8c"
SQL_DB_CONN = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server={SQL_DB_SERVER};"
    f"Database={SQL_DB_NAME};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

# ── Warehouse ────────────────────────────────────────────────────────────────
WH_SERVER = (
    "b6atmjpbeaiutjctiqe73fw3ri-po6a3gor6xeexkjjtx6vbgs5ki"
    ".datawarehouse.fabric.microsoft.com"
)
WH_DATABASE = "FabricDemo_WH"
WH_CONN = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server={WH_SERVER};"
    f"Database={WH_DATABASE};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

# ── Eventhouse (KQL) ─────────────────────────────────────────────────────────
EH_CLUSTER_URI = (
    "https://trd-ffhbqfk8q6dbxznaz8.z9.kusto.fabric.microsoft.com"
)
EH_INGEST_URI = (
    "https://ingest-trd-ffhbqfk8q6dbxznaz8.z9.kusto.fabric.microsoft.com"
)
EH_DATABASE = "FabricDemo_EH"

# ── Eventstream ──────────────────────────────────────────────────────────────
EVENTSTREAM_CONN = (
    "Endpoint=sb://esehbniwwdax15lwp8wayj.servicebus.windows.net/;"
    "SharedAccessKeyName=key_dfa1e65d-abd7-4cf2-9b2b-314f8d7cb18c;"
    "SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>;"
    "EntityPath=es_ca3d3698-1277-4e7f-90e5-eff933faec49"
)
EVENTSTREAM_EVENT_HUB = "es_ca3d3698-1277-4e7f-90e5-eff933faec49"
EVENTSTREAM_NAME = "FabricDemo_ES"

# ── Auth ─────────────────────────────────────────────────────────────────────
AUTH_METHOD = "cli"

# ── Test isolation (avoid polluting existing data) ───────────────────────────
SQL_SCHEMA = "spindle_test"
KQL_PREFIX = "spindle_test_"
