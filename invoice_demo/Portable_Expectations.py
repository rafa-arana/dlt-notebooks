# Databricks notebook source
dbutils.fs.put("/Users/rafael.arana@databricks.com/DLT/expectations.csv","""
name,constraint,tag
website_not_null,"Website IS NOT NULL",validity
location_not_null,"Location IS NOT NULL",validity
state_not_null,"State IS NOT NULL",validity
fresh_data,"to_date(updateTime,'M/d/yyyy h:m:s a') > '2010-01-01'",maintained
social_media_access,"NOT(Facebook IS NULL AND Twitter IS NULL AND Youtube IS NULL)",maintained
""",overwrite=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat "/dbfs/Users/rafael.arana@databricks.com/DLT/expectations.csv"

# COMMAND ----------

import dlt
from pyspark.sql.functions import expr, col

def get_rules(tag):
  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.format("csv").option("header", "true").load("/Users/rafael.arana@databricks.com/DLT/expectations.csv")
  for row in df.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
  return rules

# COMMAND ----------

@dlt.table(
  name="raw_farmers_market"
)
@dlt.expect_all_or_drop(get_rules('validity'))
def get_farmers_market_data():
  return (
    spark.read.format('csv').option("header", "true")
      .load('/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/')
  )

# COMMAND ----------

@dlt.table(
  name="organic_farmers_market"
)
@dlt.expect_all_or_drop(get_rules('maintained'))
def get_organic_farmers_market():
  return (
    dlt.read("raw_farmers_market")
      .filter(expr("Organic = 'Y'"))
      .select("MarketName", "Website", "State",
        "Facebook", "Twitter", "Youtube", "Organic",
        "updateTime"
      )
  )
