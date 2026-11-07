-- Expose the gold layer through Azure Synapse serverless SQL (or any engine that reads
-- Parquet/Delta from ADLS Gen2). Replace <storage-account> and <container>.

CREATE SCHEMA gold;
GO

-- Current suppliers (Parquet output)
CREATE OR ALTER VIEW gold.dim_supplier_current AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/<container>/lake/gold/dim_supplier_current/',
    FORMAT = 'PARQUET'
) AS r;
GO

-- Monthly spend by category at order time
CREATE OR ALTER VIEW gold.fact_spend_monthly AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/<container>/lake/gold/fact_spend_monthly/',
    FORMAT = 'PARQUET'
) AS r;
GO

-- Full supplier history, when the pipeline runs with --format delta
CREATE OR ALTER VIEW gold.supplier_history AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/<container>/lake/silver/supplier_history/',
    FORMAT = 'DELTA'
) AS r;
GO

-- Example: what did supplier S004 look like on 15 January 2026?
-- SELECT * FROM gold.supplier_history
-- WHERE supplier_id = 'S004'
--   AND valid_from <= '2026-01-15' AND valid_to > '2026-01-15';
