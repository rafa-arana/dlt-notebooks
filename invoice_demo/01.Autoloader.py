# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./01.Autoloader">STAGE 1</a>: Ingestion with Live Tables and Autoloader
# MAGIC + <a href="$./01.Autoloader_Metaprogramming">STAGE 2</a>: Ingestion with Live Tables and Autoloader Using metaprogramming
# MAGIC + <a href="$./03.Event_Log_Analysis">STAGE 3</a>: Event Log Analysis
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Load CSV files from Cloud in Streaming Mode

# COMMAND ----------

# MAGIC %md
# MAGIC # Example of the automated ETL workflow with Delta Live Tables

# COMMAND ----------

# DBTITLE 1,More easily build and maintain data pipelines
# MAGIC %md
# MAGIC   - With Delta Live Tables, easily define end-to-end data pipelines by specifying the data source, the transformation logic, and destination state of the data — instead of manually stitching together siloed data processing jobs. 
# MAGIC   - Automatically maintain all data dependencies across the pipeline and reuse ETL pipelines with environment independent data management. 
# MAGIC   - Run in **batch or streaming** and specify incremental or complete computation for each table.
# MAGIC 
# MAGIC <img src ='/files/dimad/media/Live-Tables-Pipeline.png' width='600'>

# COMMAND ----------

# DBTITLE 1,Automatic Testing
# MAGIC %md
# MAGIC  
# MAGIC   - Delta Live Tables helps to ensure accurate and useful BI, data science and machine learning with high-quality data for downstream users. 
# MAGIC   - Prevent bad data from flowing into tables through validation and integrity checks and avoid data quality errors with predefined error policies (fail, drop, alert or quarantine data). 
# MAGIC   - In addition, you can monitor data quality trends over time to get insight into how your data is evolving and where changes may be necessary.
# MAGIC   
# MAGIC   <img src ='/files/dimad/media/Bronze-Silver-Gold-Tables.png' width='600'>

# COMMAND ----------

# DBTITLE 1,Deep visibility for monitoring and easy recovery
# MAGIC %md  
# MAGIC 
# MAGIC   - Gain deep visibility into pipeline operations with tools to visually track operational stats and data lineage. 
# MAGIC   - Reduce downtime with automatic error handling and easy replay. 
# MAGIC   - Speed up maintenance with single-click deployment and upgrades.
# MAGIC 
# MAGIC <img src ='/files/dimad/media/dimad-pipelineui.png' width='800'>
# MAGIC 
# MAGIC 
# MAGIC <a href="https://eastus2.azuredatabricks.net/?o=5206439413157315#joblist/pipelines/fc611aee-6167-4038-a13c-e40b0775d107" target="_blank">Transactions - Delta Live Tables Pipeline</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Diving into the code👇

# COMMAND ----------

try:
  # where new data file will be received
  landing_zone_es = spark.conf.get("mypipeline.landing_zone.es")
except:
  raise Exception("Please provide [landing_zone] as job configuration")
  
try:
  # where new data file will be received
  landing_zone_br = spark.conf.get("mypipeline.landing_zone.br")
except:
  raise Exception("Please provide [landing_zone] as job configuration")
  
try:
  # where new data file will be received
  landing_zone_uk = spark.conf.get("mypipeline.landing_zone.uk")
except:
  raise Exception("Please provide [landing_zone] as job configuration")
  
try:
  # number of new file to read at each iteration
  max_files = int(spark.conf.get("mypipeline.max_files", "1"))
except:
  raise Exception("Please provide [max_files] as job configuration")
  

# COMMAND ----------

# DLT import
import dlt
from pyspark.sql.functions import *

# Pyspark functions
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, TimestampType, DecimalType, LongType

shopping_cart_path = "/FileStore/dimad/data/OnlineRetail.csv"
#shopping_cart_input_path = "/mnt/dimad/retail/csv2"

# setup retail schema
retail_input_schema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", DateType(), True),  
  StructField("UnitPrice", DecimalType(), True),  
  StructField("CustomerID", IntegerType(), True),  
  StructField("Country", StringType(), True),  
])

# COMMAND ----------

# DBTITLE 1,Ingesting  shopping cart files with Autoloader into Bronze layer
@dlt.table(
  name="autoloader_invoice_bronze_es",
  comment="This is an incremental streaming source from autoloadder csv files on ADLS")
def get_cloudfiles_es():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("rescuedDataColumn", "_rescue")
      .option("cloudFiles.maxFilesPerTrigger", max_files)
      .option("header", "true") 
      .schema(retail_input_schema)
      .load(landing_zone_es)
      .withColumn("inputFileName",input_file_name())
      .withColumn("entity",lit("ES"))
      
    )

# COMMAND ----------

@dlt.table(
  name="autoloader_invoice_bronze_br",
  comment="Acocunts from BR") 
def get_cloudfiles_br():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("rescuedDataColumn", "_rescue")
      .option("cloudFiles.maxFilesPerTrigger", max_files)
      .option("header", "true") 
      .schema(retail_input_schema)
      .load(landing_zone_br)
      .withColumn("inputFileName",input_file_name())
      .withColumn("entity",lit("BR"))
      
    )

# COMMAND ----------

@dlt.table(
  name="autoloader_invoice_bronze_uk",
  comment="Accounts from UK")
def get_cloudfiles_uk():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("rescuedDataColumn", "_rescue")
      .option("cloudFiles.maxFilesPerTrigger", max_files)
      .option("header", "true") 
      .schema(retail_input_schema)
      .load(landing_zone_uk)
      .withColumn("inputFileName",input_file_name())
      .withColumn("entity",lit("UK"))
      
    )

# COMMAND ----------

all_tables = []
all_tables.append("autoloader_invoice_bronze_es")
all_tables.append("autoloader_invoice_bronze_br")
all_tables.append("autoloader_invoice_bronze_uk")

# COMMAND ----------

@dlt.table(
  name="autoloader_invoice_bronze",
  comment="This is an incremental streaming source from autoloadder csv files on ADLS",
  table_properties={
                    "delta.autoOptimize.optimizeWrite" : "true", 
#                     "delta.autoOptimize.autoCompact" : "true", 
                    "quality" : "bronze"})
def summary():
  target_tables = [dlt.read_stream(t) for t in all_tables]
  unioned = functools.reduce(lambda x,y: x.union(y), target_tables)
  return unioned


# COMMAND ----------

# MAGIC %md
# MAGIC <a href="https://eastus2.azuredatabricks.net/?o=5206439413157315#joblist/pipelines/d057bc82-b643-486f-8a66-6079b7ddab49" target="_blank">Transactions - Delta Live Tables Pipeline</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.Lakehouse Context">STAGE 0</a>: Home page
# MAGIC + <a href="$./01.Orchestrating delta pipelines">STAGE 1</a>: Orchestrating delta pipelines
# MAGIC + <a href="$./02.Querying Delta Tables">STAGE 2</a>: Querying Delta Tables
# MAGIC + <a href="$./03. Real Time fraud prediction">STAGE 3</a>: Real Time fraud prediction
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
