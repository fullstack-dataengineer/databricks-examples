// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StringType, IntegerType}
import org.apache.spark.sql.functions.{from_json,col}

// COMMAND ----------

val structOneSchema = new StructType()
.add("sharedField", IntegerType)
.add("fieldOne", StringType)

val structTwoSchema = new StructType()
.add("sharedField", IntegerType)
.add("fieldTwo", StringType)

// COMMAND ----------

val df1 = sc.parallelize(List(
  ("a", "b", """{"fieldOne": "foo", "sharedField": 1}"""),
  ("a", "b", """{"fieldOne": "foo1", "sharedField": 2}""")
)).toDF("a", "b", "structCol")
.withColumn("structCol", from_json(col("structCol"), structOneSchema))

val df2 = sc.parallelize(List(
  ("b", "c", "d", """{"fieldTwo": "bar", "sharedField": 3}"""),
  ("b", "c", "d", """{"fieldTwo": "bar1", "sharedField": 4}"""),
)).toDF("b", "c", "d", "structCol")
.withColumn("structCol", from_json(col("structCol"), structTwoSchema))


// COMMAND ----------

display(df1)

// COMMAND ----------

display(df2)

// COMMAND ----------

df1.union(df2)

// COMMAND ----------

val unionedByName = df1.unionByName(df2, allowMissingColumns=true)

// COMMAND ----------

display(unionedByName)
