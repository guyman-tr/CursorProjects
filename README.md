# DDR Compare (Synapse ג†” Databricks)

Small, reusable scripts to compare *per-DateID row counts* between:

- **Azure Synapse dedicated pool** (ODBC Driver 18, AAD interactive / MFA)
- **Azure Databricks SQL Warehouse** (databricks-sql-connector, Azure CLI auth)

## Prereqs

- Python 3.11+ (this VM uses `C:\\Python311\\python.exe`)
- ODBC Driver 18 for SQL Server installed
- `az` / Azure CLI login is not required for Synapse (interactive), but Databricks uses Azure CLI auth.

## Setup

Install deps:

```powershell
C:\Python311\python.exe -m pip install -r ddr-compare\requirements.txt
```

Set environment variables (PowerShell):

```powershell
# Databricks warehouse
$env:DATABRICKS_SERVER_HOSTNAME = "adb-5142916747090026.6.azuredatabricks.net"
$env:DATABRICKS_HTTP_PATH       = "/sql/1.0/warehouses/96cd390a7e172342"
$env:DATABRICKS_AUTH_TYPE       = "azure-cli"

# Synapse (prod)
$env:SYNAPSE_SERVER = "prod-synapse-dataplatform-we.sql.azuresynapse.net"
$env:SYNAPSE_DB     = "sql_dp_prod_we"
$env:SYNAPSE_UID    = "guyman@etoro.com"

# Output folder
$env:DDR_COMPARE_OUT_DIR = "C:\\Users\\guyman\\Documents\\DDR_Compare"
```

## Run

Compare one table (Synapse ג†’ Databricks auto-match by name):

```powershell
C:\Python311\python.exe ddr-compare\compare_table_by_dateid.py --synapse-table BI_DB_dbo.BI_DB_DDR_Fact_AUM
```

Compare a list of tables:

```powershell
C:\Python311\python.exe ddr-compare\compare_many_tables_by_dateid.py --synapse-table BI_DB_dbo.BI_DB_DDR_Fact_AUM --synapse-table BI_DB_dbo.BI_DB_DDR_Fact_MIMO_AllPlatforms
```

Outputs:

- One CSV per table: `compare_<synapse_table>_counts.csv`
- Summary CSV: `DDR_compare_summary.csv`

## Notes

- These scripts scan full tables (can be slow on huge facts). If needed we can add optional date filters.
