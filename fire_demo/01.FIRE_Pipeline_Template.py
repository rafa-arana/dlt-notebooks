# Databricks notebook source
# MAGIC %pip install git+https://github.com/aamend/fire.git

# COMMAND ----------

try:
  # the name of the fire entity we want to process
  fire_entity = spark.conf.get("fire_entity")
except:
  raise Exception("Please provide [fire_entity] as job configuration")
 
try:
  # where new data file will be received
  landing_zone = spark.conf.get("landing_zone")
except:
  raise Exception("Please provide [landing_zone] as job configuration")
  
try:
  # where corrupted data file will be stored
  invalid_format_path = spark.conf.get("invalid_format_path")
except:
  raise Exception("Please provide [invalid_format_path] as job configuration")
 
try:
  # format we ingest raw data
  file_format = spark.conf.get("file_format", "json")
except:
  raise Exception("Please provide [file_format] as job configuration")
 
try:
  # number of new file to read at each iteration
  max_files = int(spark.conf.get("max_files", "1"))
except:
  raise Exception("Please provide [max_files] as job configuration")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from fire.spark import FireModel
import dlt

# COMMAND ----------

fire_model = FireModel().load(fire_entity)
fire_schema = fire_model.schema
fire_constraints = fire_model.constraints

# COMMAND ----------

ingestionDate = current_date()
ingestionTime = current_timestamp()

# COMMAND ----------

all_tables = []
def generate_tables(table_name, landing_path, country, max_files):
    @dlt.table(
        name=table_name,
        comment="This is an incremental streaming source from Auto loader  files on ADLS" 
    )
    def bronze():
      return (
        spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format",file_format)
          .option("badRecordsPath", invalid_format_path)
          .option("rescuedDataColumn", "_rescue")
          .option("cloudFiles.maxFilesPerTrigger", max_files)
          .option("header", "true") 
          .schema(fire_model.schema)
          .load(landing_zone)
          .withColumn("InputFileName",input_file_name())
          .withColumn("Entity",lit(country))
          .withColumn("IngestionDate",ingestionDate)    
          .withColumn("IngestionTime",ingestionTime)    
      )
    all_tables.append(table_name)

# COMMAND ----------

generate_tables(fire_entity + "_bronze", landing_zone, "ES",1)

# COMMAND ----------

@dlt.table(
    name=fire_entity+"_silver",
    comment="This is an incremental streaming source from Auto loader csv files on ADLS" 
)
@dlt.expect_all_or_drop(dict(zip(fire_constraints, fire_constraints)))
def silver():
  return dlt.read_stream(fire_entity+"_bronze")

# COMMAND ----------

@udf("array<string>")
def failed_expectations(expectations):
  # retrieve the name of each failed expectation 
  return [name for name, success in zip(fire_constraints, expectations) if not success]

# COMMAND ----------

@dlt.table(
    name=fire_entity+"_quarantine",
    comment="This is an incremental streaming source from Auto loader csv files on ADLS" 
)
def quarantine():
  return (
      dlt
        .read_stream(fire_entity+"_bronze")
        .withColumn("_fire", array([expr(value) for value in fire_constraints]))
        .withColumn("_fire", failed_expectations("_fire"))
        .filter(size("_fire") > 0)
  )
