// Databricks notebook source
val df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

df.write.saveAsTable("iot_devices_rollback")

display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE iot_devices_rollback ADD COLUMNS (badColumn string)

// COMMAND ----------

// MAGIC %sql
// MAGIC UPDATE iot_devices_rollback SET badColumn = "*{)(()(*))}";

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from iot_devices_rollback

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

// MAGIC %sql
// MAGIC DESCRIBE HISTORY iot_devices_rollback

// COMMAND ----------


