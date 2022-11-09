// Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets"))

// COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/iot/"))

// COMMAND ----------

val inferredJsonDF = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

display(inferredJsonDF)

// COMMAND ----------

val textStringDF = spark.read.text("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

display(textStringDF)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val dfWithJsonString = textStringDF
  .withColumn("id", monotonically_increasing_id())
  .withColumn("date", current_date())
  .withColumn("timestamp", current_timestamp())
  .withColumn("randomFloatValue", rand())
  .withColumn("year", year(col("timestamp")))
  .withColumn("month", month(col("timestamp")))
  .withColumn("day", dayofmonth(col("timestamp")))
  .withColumn("hour", hour(col("timestamp")))

// COMMAND ----------

display(dfWithJsonString)

// COMMAND ----------

import org.apache.spark.sql.types.StructType

val jsonSchema: StructType = spark.read.json(dfWithJsonString.select("value").as[String]).schema

// COMMAND ----------

val parsedJsonDF = dfWithJsonString
 .withColumn("value", from_json(col("value"), jsonSchema))
 .select("*", "value.*")
 .drop("value")

// COMMAND ----------

display(parsedJsonDF)
