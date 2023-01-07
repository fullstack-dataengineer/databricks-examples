// Databricks notebook source
val tablePath = "s3://databricks-fullstack-dataengineer/tables/iot_devices_rollback"

// COMMAND ----------

val df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

df.write.option("path", tablePath).saveAsTable("iot_devices_rollback")

display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE iot_devices_rollback ADD COLUMNS (badColumn string)

// COMMAND ----------

// MAGIC %sql
// MAGIC UPDATE iot_devices_rollback SET badColumn = "*{)(()(*))}";

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM iot_devices_rollback where cn = 'United States'

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM iot_devices_rollback where cn = 'United States'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM iot_devices_rollback where cn = 'United States'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM iot_devices_rollback

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY iot_devices_rollback

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE iot_devices_rollback

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY iot_devices_rollback

// COMMAND ----------

import io.delta.tables._

val deltaTable = DeltaTable.forPath(spark, tablePath)

val fullHistoryDF = deltaTable.history()    // get the full history of the table

val lastOperationDF = deltaTable.history(1) // get the last operation

// COMMAND ----------

display(fullHistoryDF)

// COMMAND ----------

display(lastOperationDF)

// COMMAND ----------

val latestCommitFileJson = spark.read.json("s3://databricks-fullstack-dataengineer/tables/iot_devices_rollback/_delta_log/00000000000000000003.json")

// COMMAND ----------

display(latestCommitFileJson)

// COMMAND ----------

deltaTable.restoreToVersion(1)

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY iot_devices_rollback

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM iot_devices_rollback

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM iot_devices_rollback where cn = 'United States'

// COMMAND ----------


