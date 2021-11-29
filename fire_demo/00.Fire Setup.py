# Databricks notebook source
# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./00.Fire Setup">STAGE 0</a>: Setup
# MAGIC + <a href="$./01.FIRE_Pipeline_Template">STAGE 1</a>: ETL Pipeline Template
# MAGIC + <a href="$./02.FIRE_Pipeline_Controls">STAGE 2</a>: Pipeline Controls
# MAGIC + <a href="$./02.UDFs_Expectations">STAGE 2</a>: Custom data quality checks with UDFs
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lart "/dbfs/Users/rafael.arana@databricks.com/DLT/finreg/tables"

# COMMAND ----------

# MAGIC %pip install git+https://github.com/aamend/fire.git

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from fire.spark import FireModel

# COMMAND ----------

fire_entity = "collateral"
landing_zone = "/FileStore/legend/raw/collateral"
invalid_format_path = "/FileStore/legend/invalid/collateral"
file_format = "json"
max_files = "1"
limit = 100

# COMMAND ----------

fire_model = FireModel().load(fire_entity)
fire_schema = fire_model.schema

# COMMAND ----------

df = spark.read.schema(fire_model.schema).json(landing_zone).limit(limit)
          

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("append").json(landing_zone)
