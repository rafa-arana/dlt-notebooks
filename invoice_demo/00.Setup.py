# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./01.Autoloader">STAGE 1</a>: Ingestion with Live Tables and Autoloader
# MAGIC + <a href="$./01.Autoloader_Metaprogramming">STAGE 1 bis</a>: Ingestion with Live Tables and Autoloader Using metaprogramming
# MAGIC + <a href="$./02.UDFs_Expectations">STAGE 2</a>: Custom data quality checks with UDFs
# MAGIC + <a href="$./03.Event_Log_Analysis">STAGE 3</a>: Event Log Analysis
# MAGIC ---

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lart "/dbfs/Users/rafael.arana@databricks.com/DLT/finreg/tables"

# COMMAND ----------

shopping_cart_path = "/Users/rafael.arana@databricks.com/DLT/finreg/data/OnlineRetail.csv"

# get shopping cart dataframe
retail_df = spark.read.option("header","true").csv(shopping_cart_path)
# repartition to simulate loading
shopping_cart_landing_path = "/Users/rafael.arana@databricks.com/DLT/finreg/landing/es/account"
retail_df.repartition(20).write.format("csv").option("header", "true").save(shopping_cart_landing_path)

# COMMAND ----------

pt_df = retail_df.limit(40000)
shopping_cart_landing_path = "/Users/rafael.arana@databricks.com/DLT/finreg/landing/br/account"
pt_df.repartition(10).write.format("csv").option("header", "true").save(shopping_cart_landing_path)

# COMMAND ----------

# DBTITLE 1,Expectations in a CSV file
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/finreg/expectations.csv","""
name,constraint,tag
existing_invoice,"InvoiceNo is not null",invoice_bronze
existing_customer,"CustomerID is not null",invoice_bronze
valid_invoice,"Description != 'Discount'",invoice_bronze
existing_invoice,"InvoiceNo is not null",invoice_silver
existing_customer,"CustomerID is not null",invoice_silver
valid_invoice,"Description != 'Discount'",invoice_silver
""",overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT db_name DEFAULT "aranadlt"

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists $db_name.sources
# MAGIC (
# MAGIC   TableName string,
# MAGIC   Expectations string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table $db_name.sources

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into $db_name.sources
# MAGIC values(
# MAGIC   'bronze_br_invoice',
# MAGIC   '{
# MAGIC      "Valid InvoiceNo": "InvoiceNo IS NOT NULL", 
# MAGIC      "Valid CustomerID": "CustomerID IS NOT NULL"
# MAGIC    }'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into $db_name.sources
# MAGIC values(
# MAGIC   'bronze_es_invoice',
# MAGIC   '{
# MAGIC      "Valid InvoiceNo": "InvoiceNo IS NOT NULL", 
# MAGIC      "Valid CustomerID": "CustomerID IS NOT NULL"
# MAGIC    }'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into $db_name.sources
# MAGIC values(
# MAGIC   'bronze_pt_invoice',
# MAGIC   '{
# MAGIC      "Valid InvoiceNo": "InvoiceNo IS NOT NULL", 
# MAGIC      "Valid CustomerID": "CustomerID IS NOT NULL"
# MAGIC    }'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into $db_name.sources
# MAGIC values(
# MAGIC   'bronze_uk_invoice',
# MAGIC   '{
# MAGIC      "Valid InvoiceNo": "InvoiceNo IS NOT NULL", 
# MAGIC      "Valid CustomerID": "CustomerID IS NOT NULL"
# MAGIC    }'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into $db_name.sources
# MAGIC values(
# MAGIC  'bronze_invoice',
# MAGIC  '{
# MAGIC      "Valid InvoiceNo": "InvoiceNo IS NOT NULL", 
# MAGIC      "Valid CustomerID": "CustomerID IS NOT NULL"
# MAGIC    }'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from $db_name.sources
