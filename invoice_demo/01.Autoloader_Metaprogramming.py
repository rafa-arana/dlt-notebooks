# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./01.Autoloader">STAGE 1</a>: Ingestion with Live Tables and Autoloader
# MAGIC + <a href="$./01.bis.Autoloader_Metaprogramming">STAGE 2</a>: Ingestion with Live Tables and Autoloader Using metaprogramming
# MAGIC + <a href="$./02. TODO">STAGE 3</a>: TODO
# MAGIC ---

# COMMAND ----------

import json

# COMMAND ----------

#database = spark.conf.get("dpdlt.pipeline.database")
#inputData = spark.conf.get("dpdlt.pipeline.entityName")
inputData = "bronze_invoice"
database = "aranadlt"

# COMMAND ----------

from pyspark.sql.functions import *
ingestionDate = current_date()
ingestionTime = current_timestamp()
ingestionTime

# COMMAND ----------

conf = spark.table(f"{database}.sources").filter(f"TableName = '{inputData}'").first()
expectconf = json.loads(conf.Expectations)

print(f"data frame options: {expectconf}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Load CSV files from Cloud in Streaming Mode

# COMMAND ----------

# MAGIC %md
# MAGIC # Example of the automated ETL workflow with Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Diving into the code👇

# COMMAND ----------

try:
  # where new data file will be received
  landing_zone_es = spark.conf.get("mypipeline.landing_zone.es")
except:
  raise Exception("Please provide [mypipeline.landing_zone.es] as job configuration")

try:
  # where new data file will be received
  landing_zone_pt = spark.conf.get("mypipeline.landing_zone.pt")
except:
  raise Exception("Please provide [mypipeline.landing_zone.pt] as job configuration")
  
try:
  # where new data file will be received
  landing_zone_br = spark.conf.get("mypipeline.landing_zone.br")
except:
  raise Exception("Please provide [mypipeline.landing_zone.br] as job configuration")
  
try:
  # where new data file will be received
  landing_zone_uk = spark.conf.get("mypipeline.landing_zone.uk")
except:
  raise Exception("Please provide [mypipeline.landing_zone.uk] as job configuration")
  
try:
  # number of new file to read at each iteration
  max_files = int(spark.conf.get("mypipeline.max_files", "1"))
except:
  raise Exception("Please provide [max_files] as job configuration")
  

# COMMAND ----------

from pyspark.sql.functions import expr, col

def get_rules(tag):
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.format("csv").option("header", "true").load("/Users/rafael.arana@databricks.com/DLT/finreg/expectations.csv")
  for row in df.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
  return rules

# COMMAND ----------

# DLT import
import dlt
from pyspark.sql.functions import *

# Pyspark functions
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, TimestampType, DecimalType, LongType

# setup retail schema
retail_input_schema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),  
  StructField("UnitPrice", DecimalType(), True),  
  StructField("CustomerID", IntegerType(), True),  
  StructField("Country", StringType(), True), 
  StructField("InputFileName", StringType(), False), 
  StructField("Entity", StringType(), True), 
  StructField("IngestionDate", DateType(), True),
  StructField("IngestionTime", TimestampType(), True) 
])

# COMMAND ----------

# DBTITLE 1,Ingesting  shopping cart files with Autoloader into Bronze layer
all_tables = []

def generate_tables(table_name, landing_path, country, max_files):
  @dlt.table(
    name=table_name,
    comment="This is an incremental streaming source from Auto loader csv files on ADLS" 
  )
  @dlt.expect_all(get_rules('invoice_bronze'))
  def create_invoice_bronze_table():
    return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("rescuedDataColumn", "_rescue")
      .option("cloudFiles.maxFilesPerTrigger", max_files)
      .option("header", "true") 
      .schema(retail_input_schema)
      .load(landing_path)
      .withColumn("InputFileName",input_file_name())
      .withColumn("Entity",lit(country))
      .withColumn("IngestionDate",ingestionDate)    
      .withColumn("IngestionTime",ingestionTime)    
    )

  all_tables.append(table_name)

# COMMAND ----------

generate_tables("bronze_es_invoice", landing_zone_es, "ES",1)
generate_tables("bronze_pt_invoice", landing_zone_pt, "PT",1)
generate_tables("bronze_br_invoice", landing_zone_br, "BR",1)
generate_tables("bronze_uk_invoice", landing_zone_uk, "UK",1)

# COMMAND ----------

@dlt.table(
  name="silver_invoice",
  comment="This is an incremental streaming source from autoloadder csv files on ADLS",
  table_properties={
                    "delta.autoOptimize.optimizeWrite" : "true", 
#                     "delta.autoOptimize.autoCompact" : "true", 
                    "quality" : "silver"
})
@dlt.expect_all_or_drop(expectconf)
def summary():
  target_tables = [dlt.read_stream(t) for t in all_tables]
  unioned = functools.reduce(lambda x,y: x.union(y), target_tables)
  return unioned


# COMMAND ----------

# MAGIC %md
# MAGIC <a href="https://adb-984752964297111.11.azuredatabricks.net/?o=984752964297111#joblist/pipelines/308c838b-238f-4539-9ae1-f5d0ffc12da7" target="_blank">Transactions - Delta Live Tables Pipeline</a>

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
