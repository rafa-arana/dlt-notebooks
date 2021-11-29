// Databricks notebook source
// MAGIC %md
// MAGIC ---
// MAGIC + <a href="$./00.Setup">STAGE 0</a>: Setup
// MAGIC + <a href="$./01.Autoloader">STAGE 1</a>: Ingestion with Live Tables and Autoloader
// MAGIC + <a href="$./01.Autoloader_Metaprogramming">STAGE 2</a>: Ingestion with Live Tables and Autoloader Using metaprogramming
// MAGIC + <a href="$./03.Event_Log_Analysis">STAGE 3</a>: Event Log Analysis
// MAGIC ---

// COMMAND ----------

// DBTITLE 1,Setup paths
val storage_path =   "/Users/rafael.arana@databricks.com/DLT/finreg" 

val event_log_path = storage_path + "/system/events"

// COMMAND ----------

// DBTITLE 1,Register the Event Log table
spark.sql("CREATE DATABASE IF NOT EXISTS aranadlt_ops")

spark.sql(s"""
CREATE TABLE IF NOT EXISTS aranadlt_ops.event_log_raw
USING delta
LOCATION '$event_log_path'
""")

// COMMAND ----------

val json_parsed = spark.read.json(spark.table("aranadlt_ops.event_log_raw").select("details").as[String])
val json_schema = json_parsed.schema

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC val parsed_event_log = spark.table("aranadlt_ops.event_log_raw").withColumn("details_parsed", from_json($"details", json_schema))
// MAGIC 
// MAGIC parsed_event_log.createOrReplaceTempView("event_log")

// COMMAND ----------

// %sql
// DROP TABLE IF EXISTS dimad.event_log

// COMMAND ----------

parsed_event_log.write.format("delta").mode("append").option("optimizeWrite", "true").option("mergeSchema", "true").saveAsTable("aranadlt_ops.event_log")

// COMMAND ----------

// DBTITLE 0,The "details" column contains metadata about each Event sent to the Event Log
// MAGIC %md
// MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
// MAGIC * `user_action` Events occur when taking actions like creating the pipeline
// MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
// MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
// MAGIC   * `flow_type` - whether this is a complete or append flow
// MAGIC   * `explain_text` - the Spark explain plan
// MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
// MAGIC   * `metrics` - currently contains `num_output_rows`
// MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
// MAGIC     * `dropped_records`
// MAGIC     * `expectations`
// MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
// MAGIC   
