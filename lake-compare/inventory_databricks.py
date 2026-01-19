import pandas as pd

from _common import databricks_connect, load_settings, out_dir


def main() -> int:
    settings = load_settings()
    allow_catalogs = set(settings["databricks"].get("allowCatalogs") or [])
    allow_schemas = set(settings["databricks"].get("allowSchemas") or [])

    out = out_dir()
    out.mkdir(parents=True, exist_ok=True)
    out_csv = out / "databricks_objects.csv"

    with databricks_connect() as con:
        with con.cursor() as cur:
            cur.execute("SELECT * FROM main.monitoring.tables")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

    raw = pd.DataFrame.from_records(rows, columns=cols)

    lower = {c.lower(): c for c in raw.columns}
    def pick(*names: str):
        for n in names:
            if n in lower:
                return lower[n]
        return None

    c_catalog = pick("catalog", "table_catalog")
    c_schema = pick("schema", "table_schema", "database", "table_database")
    c_name = pick("name", "table_name")
    c_type = pick("type", "table_type")

    df = pd.DataFrame()
    df["catalog"] = raw[c_catalog] if c_catalog else "main"
    df["schema"] = raw[c_schema] if c_schema else None
    df["name"] = raw[c_name] if c_name else None
    df["type"] = raw[c_type] if c_type else None

    if allow_catalogs:
        df = df[df["catalog"].isin(allow_catalogs)]
    if allow_schemas:
        df = df[df["schema"].isin(allow_schemas)]

    df.to_csv(out_csv, index=False)
    print(f"Wrote {len(df)} rows -> {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
