import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pyodbc
from databricks import sql as dbsql


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def out_dir() -> Path:
    return Path(require_env("LAKE_COMPARE_OUT_DIR"))


def load_settings() -> Dict[str, Any]:
    here = Path(__file__).resolve().parent
    return json.loads((here / "settings.json").read_text(encoding="utf-8"))


def synapse_connect() -> pyodbc.Connection:
    server = require_env("SYNAPSE_SERVER")
    db = require_env("SYNAPSE_DB")
    uid = os.getenv("SYNAPSE_UID", "")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={db};"
        "Encrypt=yes;TrustServerCertificate=no;"
        "Connection Timeout=30;"
        "Authentication=ActiveDirectoryInteractive;"
        + (f"UID={uid};" if uid else "")
    )
    return pyodbc.connect(conn_str)


def databricks_connect() -> dbsql.Connection:
    host = require_env("DATABRICKS_SERVER_HOSTNAME")
    http_path = require_env("DATABRICKS_HTTP_PATH")
    auth_type = os.getenv("DATABRICKS_AUTH_TYPE", "azure-cli")
    return dbsql.connect(server_hostname=host, http_path=http_path, auth_type=auth_type)


def safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", s)


def normalize_name(name: str, strip_prefixes: List[str]) -> str:
    s = (name or "").strip().lower()
    for p in strip_prefixes:
        if s.startswith(p):
            s = s[len(p) :]
            break
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def jaccard_tokens(a: str, b: str) -> float:
    sa = set(a.split("_")) - {""}
    sb = set(b.split("_")) - {""}
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)
