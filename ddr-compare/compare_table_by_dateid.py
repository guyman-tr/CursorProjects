import argparse
from pathlib import Path

from _common import (
    databricks_connect,
    databricks_counts_by_dateid,
    load_monitoring_tables,
    best_match_databricks_fqn,
    merge_counts,
    out_dir,
    safe_filename,
    synapse_connect,
    synapse_counts_by_dateid,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--synapse-table', required=True, help='2-part: SCHEMA.TABLE')
    ap.add_argument('--databricks-table', required=False, help='3-part: catalog.schema.name (optional)')
    args = ap.parse_args()

    out = Path(out_dir())
    out.mkdir(parents=True, exist_ok=True)

    with synapse_connect() as syn_con:
        with databricks_connect() as dbx_con:
            monitor = load_monitoring_tables(dbx_con)
            dbx_table = args.databricks_table or best_match_databricks_fqn(monitor, args.synapse_table)

            syn_df = synapse_counts_by_dateid(syn_con, args.synapse_table)
            dbx_df = databricks_counts_by_dateid(dbx_con, dbx_table)

            merged = merge_counts(syn_df, dbx_df)
            out_csv = out / f"compare_{safe_filename(args.synapse_table)}_counts.csv"
            merged.to_csv(out_csv, index=False)

            print('Synapse:', args.synapse_table)
            print('Databricks:', dbx_table)
            print('Wrote:', out_csv)
            print('Rows:', len(merged), 'Nonzero diffs:', int((merged['diff'] != 0).sum()))


if __name__ == '__main__':
    main()
