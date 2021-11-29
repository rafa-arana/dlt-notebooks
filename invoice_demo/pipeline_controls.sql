-- Databricks notebook source
CREATE LIVE TABLE invoice_ctl
COMMENT "Tabla de Control."
TBLPROPERTIES ("quality" = "ctl")
AS
SELECT InputFileName as fileName, count(*) as records, Entity, IngestionDate, IngestionTime
FROM LIVE.silver_invoice
GROUP BY InputFileName,Entity,IngestionDate,IngestionTime
