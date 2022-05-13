// Databricks notebook source
// In Scala
// Create cubed function
val cubed = (s: Long) => {
s * s * s
}
// Register UDF
spark.udf.register("cubed", cubed)
// Create temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")


// COMMAND ----------

// In Scala/Python
// Query the cubed UDF
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
