import os
import re
from dataclasses import dataclass

import pandas as pd
import pyodbc
from databricks import sql as dbsql


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def synapse_connect() -> pyodbc.Connection:
    server = require_env('SYNAPSE_SERVER')
    db = require_env('SYNAPSE_DB')
    uid = os.getenv('SYNAPSE_UID', '')

    # Matches SSMS: Azure Active Directory ג€“ Universal with MFA
    conn_str = (
        'Driver={ODBC Driver 18 for SQL Server};'
        f'Server=tcp:{server},1433;'
        f'Database={db};'
        'Encrypt=yes;TrustServerCertificate=no;'
        'Connection Timeout=30;'
        'Authentication=ActiveDirectoryInteractive;'
        + (f'UID={uid};' if uid else '')
    )
    return pyodbc.connect(conn_str)


def databricks_connect() -> dbsql.Connection:
    host = require_env('DATABRICKS_SERVER_HOSTNAME')
    http_path = require_env('DATABRICKS_HTTP_PATH')
    auth_type = os.getenv('DATABRICKS_AUTH_TYPE', 'azure-cli')
    return dbsql.connect(server_hostname=host, http_path=http_path, auth_type=auth_type)


def out_dir() -> str:
    return require_env('DDR_COMPARE_OUT_DIR')


def safe_filename(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9_]+', '_', s)


def synapse_counts_by_dateid(con: pyodbc.Connection, table_2part: str) -> pd.DataFrame:
    q = f"SELECT DateID, COUNT(*) AS cnt FROM {table_2part} GROUP BY DateID"
    df = pd.read_sql(q, con)
    df['DateID'] = df['DateID'].astype('int64')
    df['cnt'] = df['cnt'].astype('int64')
    return df


def databricks_counts_by_dateid(con: dbsql.Connection, table_3part: str) -> pd.DataFrame:
    q = f"SELECT DateID, COUNT(*) AS cnt FROM {table_3part} GROUP BY DateID"
    with con.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    df = pd.DataFrame.from_records(rows, columns=cols)
    df['DateID'] = df['DateID'].astype('int64')
    df['cnt'] = df['cnt'].astype('int64')
    return df


def merge_counts(syn_df: pd.DataFrame, dbx_df: pd.DataFrame) -> pd.DataFrame:
    merged = syn_df.merge(dbx_df, on='DateID', how='outer', suffixes=('_synapse', '_databricks'))
    merged['cnt_synapse'] = merged['cnt_synapse'].fillna(0).astype('int64')
    merged['cnt_databricks'] = merged['cnt_databricks'].fillna(0).astype('int64')
    merged['diff'] = merged['cnt_databricks'] - merged['cnt_synapse']
    return merged.sort_values('DateID')


def load_monitoring_tables(con: dbsql.Connection) -> pd.DataFrame:
    with con.cursor() as cur:
        cur.execute('SELECT * FROM main.monitoring.tables')
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    raw = pd.DataFrame.from_records(rows, columns=cols)

    lower = {c.lower(): c for c in raw.columns}
    def pick(*names):
        for n in names:
            if n in lower:
                return lower[n]
        return None

    c_catalog = pick('catalog', 'table_catalog')
    c_schema = pick('schema', 'table_schema', 'database', 'table_database')
    c_name = pick('name', 'table_name')

    df = pd.DataFrame()
    df['catalog'] = raw[c_catalog] if c_catalog else 'main'
    df['schema'] = raw[c_schema] if c_schema else None
    df['name'] = raw[c_name] if c_name else None
    df['fqn'] = df['catalog'].astype(str) + '.' + df['schema'].astype(str) + '.' + df['name'].astype(str)
    return df


def best_match_databricks_fqn(monitor_df: pd.DataFrame, synapse_table_2part: str) -> str:
    # Heuristic: look for normalized Synapse table name substring in Databricks object name.
    syn_name = synapse_table_2part.split('.', 1)[1].lower()
    target = re.sub(r'[^a-z0-9]+', '_', syn_name)

    # Prefer main.bi_db catalog+schema if present
    cand = monitor_df[(monitor_df['catalog'] == 'main') & (monitor_df['schema'] == 'bi_db')].copy()
    if cand.empty:
        cand = monitor_df[monitor_df['catalog'] == 'main'].copy()

    hits = cand[cand['name'].fillna('').str.lower().str.contains(syn_name, na=False)]
    if len(hits) >= 1:
        # pick longest name
        hits = hits.assign(nlen=hits['name'].fillna('').str.len()).sort_values('nlen', ascending=False)
        return str(hits.iloc[0]['fqn'])

    # fallback: return best token overlap
    def tokens(s: str) -> set[str]:
        return set(re.sub(r'[^a-z0-9]+', '_', s.lower()).split('_')) - {''}

    tset = tokens(syn_name)
    best = None
    best_score = -1.0
    for _, r in cand.iterrows():
        n = str(r.get('name') or '')
        if not n:
            continue
        score = len(tset & tokens(n)) / max(1, len(tset | tokens(n)))
        if score > best_score:
            best_score = score
            best = r

    if best is None:
        raise RuntimeError(f"No Databricks match found for {synapse_table_2part}")
    return str(best['fqn'])
