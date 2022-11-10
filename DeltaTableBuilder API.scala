// Databricks notebook source
display(dbutils.fs.ls("dbfs:/databricks-datasets/iot/"))

// COMMAND ----------

val df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

import org.apache.spark.sql.functions._

val enrichedDF = df
  .withColumn("id", monotonically_increasing_id())
  .withColumn("date", current_date())
  .withColumn("timestamp", current_timestamp())
  .withColumn("year", year(col("timestamp")))
  .withColumn("month", month(col("timestamp")))
  .withColumn("day", dayofmonth(col("timestamp")))
  .withColumn("hour", hour(col("timestamp")))

// COMMAND ----------

display(enrichedDF)

// COMMAND ----------

enrichedDF.write.saveAsTable("enriched_iot_devices_managed")

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE default.enriched_iot_devices_managed

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE DETAIL default.enriched_iot_devices_managed

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY default.enriched_iot_devices_managed

// COMMAND ----------

import io.delta.tables.DeltaTable

// COMMAND ----------

val tableName = "enriched_iot_devices_delta_table_builder"

// COMMAND ----------

val deltaTableBuilder = DeltaTable
  .createIfNotExists(spark)
  .tableName(tableName)
  .partitionedBy("year", "month", "day")
  .addColumns(enrichedDF.schema)
  .comment("IoT Devices Data Created with DeltaTableBuilder API")
  .property("delta.autoOptimize.optimizeWrite", "true")
  .property("delta.autoOptimize.autoCompact", "true")

// COMMAND ----------

deltaTableBuilder.execute()

// COMMAND ----------

val emptyTable = spark.sql(s"select * from default.$tableName")

// COMMAND ----------

display(emptyTable)

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE DETAIL default.enriched_iot_devices_delta_table_builder

// COMMAND ----------

enrichedDF.write.mode("overwrite").saveAsTable(tableName)

// COMMAND ----------

val enrichedTableDF = spark.sql(s"select * from default.$tableName")

// COMMAND ----------

display(enrichedTableDF)

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY default.enriched_iot_devices_delta_table_builder
