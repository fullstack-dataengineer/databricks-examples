// Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets"))

// COMMAND ----------

spark.sql("CREATE TABLE default.people10m OPTIONS (PATH 'dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta')")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.people10m

// COMMAND ----------

val df = spark.sql("select * from default.people10m")

// COMMAND ----------

display(df)

// COMMAND ----------

df.show

// COMMAND ----------

df.printSchema#

// COMMAND ----------

import org.apache.spark.sql.types.StructType

val schema: StructType = df.schema

// COMMAND ----------

val columns = df.columns

// COMMAND ----------

val dataTypes = df.dtypes

// COMMAND ----------

val dTypesMap = df.dtypes.toMap

// COMMAND ----------

df.printSchema
