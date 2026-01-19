import argparse
from pathlib import Path

import pandas as pd

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
    ap.add_argument('--synapse-table', action='append', required=True, help='Repeatable 2-part: SCHEMA.TABLE')
    args = ap.parse_args()

    out = Path(out_dir())
    out.mkdir(parents=True, exist_ok=True)

    summaries = []

    with synapse_connect() as syn_con:
        with databricks_connect() as dbx_con:
            monitor = load_monitoring_tables(dbx_con)

            for syn_table in args.synapse_table:
                dbx_table = best_match_databricks_fqn(monitor, syn_table)

                syn_df = synapse_counts_by_dateid(syn_con, syn_table)
                dbx_df = databricks_counts_by_dateid(dbx_con, dbx_table)
                merged = merge_counts(syn_df, dbx_df)

                out_csv = out / f"compare_{safe_filename(syn_table)}_counts.csv"
                merged.to_csv(out_csv, index=False)

                syn_dates = set(syn_df['DateID'].tolist())
                dbx_dates = set(dbx_df['DateID'].tolist())

                summaries.append({
                    'synapse_table': syn_table,
                    'databricks_table': dbx_table,
                    'csv': str(out_csv),
                    'rows': int(len(merged)),
                    'syn_min': int(syn_df.DateID.min()) if len(syn_df) else None,
                    'syn_max': int(syn_df.DateID.max()) if len(syn_df) else None,
                    'dbx_min': int(dbx_df.DateID.min()) if len(dbx_df) else None,
                    'dbx_max': int(dbx_df.DateID.max()) if len(dbx_df) else None,
                    'syn_total': int(syn_df.cnt.sum()) if len(syn_df) else 0,
                    'dbx_total': int(dbx_df.cnt.sum()) if len(dbx_df) else 0,
                    'missing_in_dbx': int(len(syn_dates - dbx_dates)),
                    'missing_in_syn': int(len(dbx_dates - syn_dates)),
                    'mismatch_dates': int((merged['diff'] != 0).sum()),
                })

                print(f"Done {syn_table} -> {dbx_table} ({out_csv.name})")

    summary_path = out / 'DDR_compare_summary.csv'
    pd.DataFrame.from_records(summaries).to_csv(summary_path, index=False)
    print('Wrote summary:', summary_path)


if __name__ == '__main__':
    main()
