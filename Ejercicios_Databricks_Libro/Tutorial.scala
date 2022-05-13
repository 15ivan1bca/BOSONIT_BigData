// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

val spark = SparkSession.builder.appName("MnMCount").getOrCreate()


// COMMAND ----------

val df1 = spark.read.format("csv").option("header", true).option("inferSchema", true).load("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/mnm_dataset.csv")

// COMMAND ----------

df1.show(5, false)

// COMMAND ----------

val pruebaagg = df1.select("*").where(col("State").isin("CA", "NV", "TX")).groupBy("State").agg(max("Count").alias("MAX"),min("Count").alias("MIN"), avg("Count").alias("AVG"), count("Count").alias("COUNT"))

// COMMAND ----------

pruebaagg.show()
