// Databricks notebook source
import org.apache.spark.sql.functions._
// Set file paths

val delaysPath = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/departuredelays.csv"
val airportsPath = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/airport_codes_na.txt"
// Obtain airports data set

val airports = spark.read.option("header", "true").option("inferschema", "true").option("delimiter", "\t").csv(airportsPath)
airports.createOrReplaceTempView("airports_na")

// Obtain departure Delays data set
val delays = spark.read.option("header","true").csv(delaysPath).withColumn("delay", expr("CAST(delay as INT) as delay"))
.withColumn("distance", expr("CAST(distance as INT) as distance"))
delays.createOrReplaceTempView("departureDelays")

// Create temporary small table
val foo = delays.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0"""))
foo.createOrReplaceTempView("foo")


spark.sql("SELECT * FROM airports_na LIMIT 10").show()
spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
spark.sql("SELECT * FROM foo").show()

// COMMAND ----------

// Unions
val bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

// COMMAND ----------

// Lo mismo pero en spark sql
spark.sql("""SELECT * FROM bar WHERE origin = 'SEA'AND destination = 'SFO'AND date LIKE '01010%' AND delay > 0""").show()

// COMMAND ----------

// Joins
foo.join(airports.as('air), $"air.IATA" === $"origin").select("City", "State", "date", "delay", "distance", "destination").show()

// SQL
spark.sql("""SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination FROM foo f JOIN airports_na a ON a.IATA = f.origin""").show()

// COMMAND ----------

// Una funcion de ventana usa valores de las filas de una ventana para devolver un conjunto de valoras (suele ser con forma de otra fila)
spark.sql("""
SELECT origin, destination, TotalDelays, rank
FROM (
SELECT origin, destination, TotalDelays, dense_rank()
OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
FROM departureDelaysWindow
) t
WHERE rank <= 3
""").show()
