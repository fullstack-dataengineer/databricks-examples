// Databricks notebook source
// Notebook created with the following specifications:
// 1 Driver
// 30.5 GB Memory, 4 Cores
// Runtime
// 11.3.x-scala2.12
// i3.xlarge
// 1 DBU/h

// COMMAND ----------

val df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

df.write.saveAsTable("iot_devices")

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from iot_devices

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW TBLPROPERTIES iot_devices

// COMMAND ----------

// MAGIC %sql 
// MAGIC ALTER TABLE iot_devices RENAME COLUMN cn TO country

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE iot_devices SET TBLPROPERTIES (
// MAGIC   'delta.minReaderVersion' = '2',
// MAGIC   'delta.minWriterVersion' = '5',
// MAGIC   'delta.columnMapping.mode' = 'name'
// MAGIC )

// COMMAND ----------

// MAGIC %sql 
// MAGIC ALTER TABLE iot_devices RENAME COLUMN cn TO country

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from iot_devices

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE iot_devices DROP COLUMNS (c02_level, cca2, cca3)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from iot_devices
