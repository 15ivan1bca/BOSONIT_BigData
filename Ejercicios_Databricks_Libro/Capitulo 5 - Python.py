# Databricks notebook source
# In Python
from pyspark.sql.types import LongType
# Create cubed function
def cubed(s):
    return s * s * s
# Register UDF
spark.udf.register("cubed", cubed, LongType())
# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# COMMAND ----------

# Cubos UDF panda 2

# Import pandas
import pandas as pd
# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
# Declare the cubed function
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a
# Create the pandas UDF for the cubed function
cubed_udf = pandas_udf(cubed, returnType=LongType())

# COMMAND ----------

# Create a Pandas Series
x = pd.Series([1, 2, 3])
# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# COMMAND ----------

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 4)
# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()
