# Databricks notebook source
# In Python
from pyspark.sql import SparkSession
# Create a SparkSession
spark = (SparkSession
.builder
.appName("SparkSQLExampleApp")
.getOrCreate())
# Path to data set
csv_file = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/departuredelays.csv"


# COMMAND ----------

# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
.option("inferSchema", "true")
.option("header", "true")
.load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

# Los spark.sql están en el notebook del tema 4 de scala, porque es lo mismo en python que en scala
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)
