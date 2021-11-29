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

# DLT import
import dlt
from pyspark.sql.functions import *

# Pyspark functions
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType, TimestampType, DecimalType, LongType


# COMMAND ----------

import re

# Function to validate  String length
def is_valid_col(str,num): 
    # If the string is empty
    # return false
    if (str == None):
        return False
 
    # Return if the string
    # matched the ReGex
    if(len(str) == num):
        return True
    else:
        return False

# Register function as UDF
spark.udf.register("is_valid_col", lambda s,n: is_valid_col(s,n), "boolean")

# COMMAND ----------

import math

def is_valid_precision_and_scale(x, v_precision, v_scale):
    max_digits = 14
    try:
      x=float(str(x).replace(',',''))
      int_part = int(abs(x))
      magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
      if magnitude >= max_digits:
          return (magnitude, 0)
      frac_part = abs(x) - int_part
      multiplier = 10 ** (max_digits - magnitude)
      frac_digits = multiplier + int(multiplier * frac_part + 0.5)
      while frac_digits % 10 == 0:
          frac_digits /= 10
      scale = int(math.log10(frac_digits))
      if ( (magnitude + scale) == v_precision) and (scale == v_scale):
        return True
      else:
        return False
    except TypeError as e:
      return False
      
    
# Register function as UDF
spark.udf.register("is_valid_precision_and_scale", lambda x,p,s: is_valid_precision_and_scale(x,p,s), "boolean")

# COMMAND ----------

@dlt.table(  
  name="autoloader_invoice2",
  comment="This is an incremental streaming source from autoloadder csv files on ADLS",
  table_properties={
                    "delta.autoOptimize.optimizeWrite" : "true", 
#                     "delta.autoOptimize.autoCompact" : "true", 
                    "quality" : "bronze"})
@dlt.expect_all({
  "valid_entity" : "is_valid_col(entity,2)",
  "valid_decimal_unit_price" : "is_valid_precision_and_scale(UnitPrice,4,0)"
})
def get_silver_invoice():
  return dlt.read_stream("silver_invoice")   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
