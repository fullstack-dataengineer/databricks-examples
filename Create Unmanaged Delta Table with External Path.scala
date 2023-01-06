// Databricks notebook source
val df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

display(df)

// COMMAND ----------

df.write.option("path", "s3://databricks-fullstack-dataengineer/tables/iot_devices_unmanaged").saveAsTable("iot_devices_unmanaged")

// COMMAND ----------

// MAGIC %sql 
// MAGIC DESCRIBE TABLE EXTENDED iot_devices_unmanaged

// COMMAND ----------


