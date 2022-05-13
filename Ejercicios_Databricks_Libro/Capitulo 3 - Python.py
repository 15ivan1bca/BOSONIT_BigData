# Databricks notebook source
# In Python, define a schema
from pyspark.sql.types import *
# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
StructField('UnitID', StringType(), True),
StructField('IncidentNumber', IntegerType(), True),
StructField('CallType', StringType(), True),
StructField('CallDate', StringType(), True),
StructField('WatchDate', StringType(), True),
StructField('CallFinalDisposition', StringType(), True),
StructField('AvailableDtTm', StringType(), True),
StructField('Address', StringType(), True),
StructField('City', StringType(), True),
StructField('Zipcode', IntegerType(), True),
StructField('Battalion', StringType(), True),
StructField('StationArea', StringType(), True),
StructField('Box', StringType(), True),
StructField('OriginalPriority', StringType(), True),
StructField('Priority', StringType(), True),
StructField('FinalPriority', IntegerType(), True),
StructField('ALSUnit', BooleanType(), True),
StructField('CallTypeGroup', StringType(), True),
StructField('NumAlarms', IntegerType(), True),
StructField('UnitType', StringType(), True),
StructField('UnitSequenceInCallDispatch', IntegerType(), True),
StructField('FirePreventionDistrict', StringType(), True),
StructField('SupervisorDistrict', StringType(), True),
StructField('Neighborhood', StringType(), True),
StructField('Location', StringType(), True),
StructField('RowID', StringType(), True),
StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# COMMAND ----------

fire_df.show(5)

# COMMAND ----------

parquet_path = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/parquetpath3"
fire_df.write.format("parquet").save(parquet_path)

# COMMAND ----------

few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
.where(col("CallType") != "Medical Incident"))


# COMMAND ----------

few_fire_df.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *
(fire_df
.select("CallType")
.where(col("CallType").isNotNull())
.agg(countDistinct("CallType").alias("DistinctCallTypes"))
.show())

# COMMAND ----------

# Cambiando nombre de la columna
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df.select("ResponseDelayedinMins").where(col("ResponseDelayedinMins") > 5).show(5, False))

# COMMAND ----------

fire_ts_df = (new_fire_df
.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
.drop("CallDate")
.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
.drop("WatchDate")
.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
"MM/dd/yyyy hh:mm:ss a"))
.drop("AvailableDtTm"))

# COMMAND ----------

(fire_ts_df
.select("IncidentDate", "OnWatchDate", "AvailableDtTS")
.show(5, False))

# COMMAND ----------

# Seleccionar año de un timestamp
(fire_ts_df
.select(year('IncidentDate'))
.distinct()
.orderBy(year('IncidentDate'))
.show())

# COMMAND ----------

# Numero de tipos de llamada de cada tipo de llamada
(fire_ts_df
.select("CallType")
.where(col("CallType").isNotNull())
.groupBy("CallType")
.count()
.orderBy("count", ascending=False)
.show(n=10, truncate=False))

# COMMAND ----------

# In Python
import pyspark.sql.functions as F
(fire_ts_df
.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
.show())

# COMMAND ----------


