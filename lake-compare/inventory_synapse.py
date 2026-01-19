import pandas as pd

from _common import load_settings, out_dir, synapse_connect


def main() -> int:
    settings = load_settings()
    schemas = settings["synapse"]["schemas"]
    in_list = ",".join([f"'{s}'" for s in schemas])

    query = f"""
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, 'TABLE' AS OBJECT_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ({in_list})
UNION ALL
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, 'VIEW' AS OBJECT_TYPE
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA IN ({in_list})
"""

    out = out_dir()
    out.mkdir(parents=True, exist_ok=True)
    out_csv = out / "synapse_objects.csv"

    with synapse_connect() as con:
        df = pd.read_sql(query, con)

    df.rename(
        columns={
            "TABLE_CATALOG": "catalog",
            "TABLE_SCHEMA": "schema",
            "TABLE_NAME": "name",
            "OBJECT_TYPE": "type",
        },
        inplace=True,
    )
    df.to_csv(out_csv, index=False)
    print(f"Wrote {len(df)} rows -> {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
