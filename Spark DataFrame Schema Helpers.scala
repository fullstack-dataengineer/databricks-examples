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

val columns = df.columns

// COMMAND ----------

import org.apache.spark.sql.types.StructType

// COMMAND ----------

val schema: StructType = df.schema

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.columns

// COMMAND ----------

df.dtypes

// COMMAND ----------

df.dtypes.toMap

// COMMAND ----------

display(df)
