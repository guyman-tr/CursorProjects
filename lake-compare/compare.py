import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd

from _common import databricks_connect, out_dir, safe_filename, synapse_connect


def load_mapping() -> list[dict]:
    p = out_dir() / "mapping.json"
    if not p.exists():
        raise SystemExit("mapping.json not found. Run inventory + build_mapping first.")
    return json.loads(p.read_text(encoding="utf-8")).get("mappings", [])


def find_databricks_fqn(synapse_2part: str, mappings: list[dict]) -> str:
    schema, name = synapse_2part.split(".", 1)
    for m in mappings:
        s = m["synapse"]
        if str(s.get("schema")) == schema and str(s.get("name")) == name:
            d = m["databricks"]
            return f"{d.get('catalog')}.{d.get('schema')}.{d.get('name')}"
    raise RuntimeError(f"No mapping for {synapse_2part}. Check mapping_review.csv for best candidate.")


def build_metrics_sql(keys: List[str], metrics: List[str]) -> str:
    select_parts = [*keys]

    for m in metrics:
        if m == "count":
            select_parts.append("COUNT(*) AS cnt")
        elif m.startswith("distinct:"):
            col = m.split(":", 1)[1]
            select_parts.append(f"COUNT(DISTINCT {col}) AS distinct_{col}")
        elif m.startswith("sum:"):
            col = m.split(":", 1)[1]
            select_parts.append(f"SUM({col}) AS sum_{col}")
        else:
            raise ValueError(f"Unknown metric: {m}")

    select_sql = ", ".join(select_parts)
    group_sql = ", ".join(keys)
    return f"SELECT {select_sql} FROM {{table}} GROUP BY {group_sql}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--synapse", required=True, help="2-part name: SCHEMA.TABLE")
    ap.add_argument("--key", action="append", required=True, help="Repeatable group key")
    ap.add_argument("--metric", action="append", default=["count"], help="count | distinct:col | sum:col")
    args = ap.parse_args()

    out = out_dir()
    out.mkdir(parents=True, exist_ok=True)

    mappings = load_mapping()
    dbx = find_databricks_fqn(args.synapse, mappings)

    sql_tpl = build_metrics_sql(args.key, args.metric)
    syn_sql = sql_tpl.format(table=args.synapse)
    dbx_sql = sql_tpl.format(table=dbx)

    print("Synapse:", args.synapse)
    print("Databricks:", dbx)

    with synapse_connect() as syn_con:
        syn_df = pd.read_sql(syn_sql, syn_con)

    with databricks_connect() as dbx_con:
        with dbx_con.cursor() as cur:
            cur.execute(dbx_sql)
            rows = cur.fetchall()
            dcols = [d[0] for d in cur.description]
        dbx_df = pd.DataFrame.from_records(rows, columns=dcols)

    for k in args.key:
        syn_df[k] = syn_df[k].astype(str)
        dbx_df[k] = dbx_df[k].astype(str)

    merged = syn_df.merge(dbx_df, on=args.key, how="outer", suffixes=("_synapse", "_databricks"))

    # numeric diffs
    for c in list(merged.columns):
        if c.endswith("_synapse"):
            base = c[: -len("_synapse")]
            c2 = base + "_databricks"
            if c2 in merged.columns:
                merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)
                merged[c2] = pd.to_numeric(merged[c2], errors="coerce").fillna(0)
                merged[base + "_diff"] = merged[c2] - merged[c]

    out_csv = out / f"compare_{safe_filename(args.synapse)}_{safe_filename('_'.join(args.key))}.csv"
    merged.to_csv(out_csv, index=False)
    print("Wrote:", out_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
