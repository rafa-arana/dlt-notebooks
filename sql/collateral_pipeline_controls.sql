-- Databricks notebook source
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
