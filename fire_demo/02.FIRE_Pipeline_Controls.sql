-- Databricks notebook source
-- MAGIC %md
-- MAGIC ---
-- MAGIC + <a href="$./00.Fire Setup">STAGE 0</a>: Setup
-- MAGIC + <a href="$./01.FIRE_Pipeline_Template">STAGE 1</a>: ETL Pipeline Template
-- MAGIC + <a href="$./02.FIRE_Pipeline_Controls">STAGE 2</a>: Pipeline Controls
-- MAGIC + <a href="$./02.UDFs_Expectations">STAGE 2</a>: Custom data quality checks with UDFs
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

CREATE LIVE TABLE collateral_ctl
COMMENT "Tabla de Control."
TBLPROPERTIES ("quality" = "ctl")
AS
SELECT a.InputFileName, a.IngestionDate, a.IngestionTime, a.rows_OK,b.rows_KO
FROM (
  SELECT count(a.id) as rows_OK, a.InputFileName, a.IngestionDate, a.IngestionTime
  FROM LIVE.collateral_silver a
  GROUP BY a.InputFileName,a.IngestionDate, a.IngestionTime 
) AS a
LEFT JOIN (
  SELECT count (b.id) as rows_KO, b.InputFileName, b.IngestionDate, b.IngestionTime
  FROM LIVE.collateral_quarantine b
  GROUP BY b.InputFileName,b.IngestionDate, b.IngestionTime
) AS B
ON a.InputFileName = b.InputFileName
