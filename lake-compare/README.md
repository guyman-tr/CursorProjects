# Lake Compare (Synapse ג†” Databricks)

Reusable tooling to:

1. **Inventory** objects (tables/views) from Synapse and Databricks.
2. Build a refreshable **mapping** between ג€matchingג€ objects across systems.
3. Run **comparisons** using arbitrary keys and metrics (not just `DateID`).

## Auth model

- **Synapse**: ODBC Driver 18 + `Authentication=ActiveDirectoryInteractive`
- **Databricks**: SQL Warehouse via `databricks-sql-connector` using `DATABRICKS_AUTH_TYPE=azure-cli`

## Install

```powershell
C:\Python311\python.exe -m pip install -r lake-compare\requirements.txt
```

## Configure env vars

```powershell
# Databricks warehouse
$env:DATABRICKS_SERVER_HOSTNAME = "adb-5142916747090026.6.azuredatabricks.net"
$env:DATABRICKS_HTTP_PATH       = "/sql/1.0/warehouses/96cd390a7e172342"
$env:DATABRICKS_AUTH_TYPE       = "azure-cli"

# Synapse (prod)
$env:SYNAPSE_SERVER = "prod-synapse-dataplatform-we.sql.azuresynapse.net"
$env:SYNAPSE_DB     = "sql_dp_prod_we"
$env:SYNAPSE_UID    = "guyman@etoro.com"

# Output directory
$env:LAKE_COMPARE_OUT_DIR = "C:\\Users\\guyman\\Documents\\LakeCompareOut"
```

## Inventory + mapping

```powershell
C:\Python311\python.exe lake-compare\inventory_synapse.py
C:\Python311\python.exe lake-compare\inventory_databricks.py
C:\Python311\python.exe lake-compare\build_mapping.py
```

Outputs in `%LAKE_COMPARE_OUT_DIR%`:

- `synapse_objects.csv`
- `databricks_objects.csv`
- `mapping.json`
- `mapping_review.csv`

## Compare

```powershell
C:\Python311\python.exe lake-compare\compare.py --synapse BI_DB_dbo.BI_DB_DDR_Fact_AUM --key DateID --metric count
```

Multiple keys & metrics:

```powershell
C:\Python311\python.exe lake-compare\compare.py --synapse DWH_dbo.Dim_Instrument --key InstrumentID --metric count --metric distinct:InstrumentID
```
