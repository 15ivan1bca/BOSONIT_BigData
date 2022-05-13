// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
val spark = SparkSession.builder.appName("SparkSQLExampleApp").getOrCreate()
val csvFile="dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/departuredelays.csv"

// COMMAND ----------

//Infer schema 
val df = spark.read.format("csv").option("inferSchema", true).option("header", "true").load(csvFile)

// Creamos la vista temporal
df.createOrReplaceTempView("us_delay_flights_tbl")

// COMMAND ----------

spark.sql("""SELECT distinct(distance), origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""").show(10)

// COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER BY delay DESC""").show(10)

// COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                  WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                  WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'Early'
              END AS Flight_Delays
              FROM us_delay_flights_tbl
              ORDER BY origin, delay DESC""").show(10)

// COMMAND ----------

// La primera consulta SQL pero esta vez con dataframe

df.select("distance", "origin", "destination").where("distance > 1000").orderBy(desc("distance")).show(10)

// COMMAND ----------

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

// COMMAND ----------

// CREATE MANAGED TABLE

spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

// COMMAND ----------

// CREATE UNMANAGED TABLE

spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
distance INT, origin STRING, destination STRING)
USING csv OPTIONS (PATH
'dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/departuredelays.csv')""")

// COMMAND ----------

spark.sql("""SELECT * from us_delay_flights_tbl""").show(10)

// COMMAND ----------

// VIEWS
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
origin = 'SFO'""")


// COMMAND ----------

spark.sql("""SELECT * FROM us_origin_airport_SFO_global_tmp_view""").show(10)

// COMMAND ----------

spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

// COMMAND ----------

// Crear dataframes de consultas sql

val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
// o
val usFlightsDF2 = spark.table("us_delay_flights_tbl")

// COMMAND ----------

//Cargar parquet, csv, json files en un dataframe
val file = """..."""
val df = spark.read.format("parquet").load(file)

//csv 
val df3 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("mode", "PERMISSIVE").load("...")



// COMMAND ----------

//Guardar df en archivo
df.write.format("json").mode("overwrite").save(location)

// COMMAND ----------

// In Scala
val file = """dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/pruebaFire.parquet/part-00000-tid-9189645471426765642-1ed6a2e5-da95-4a84-9881-16b6d8ba962e-23-1-c000.snappy.parquet"""
val df = spark.read.format("parquet").load(file)
df.show(5)

// COMMAND ----------

/* 

// Leer archivo parquet en sql
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
USING parquet
OPTIONS (
path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
2010-summary.parquet/" )

*/

// COMMAND ----------

// Escribir dataframes en un parquet
df.write.format("parquet").mode("overwrite").option("compression", "snappy").save("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/paruqet_cp4")

// Escribir dataframe en una tabla spark sql
df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

// COMMAND ----------

// Leer de un archivo JSON a un dataframe
val file = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/iot_devices.json"
val df = spark.read.format("json").load(file)

// COMMAND ----------

/*

// Leer archivo json en sql
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
USING json
OPTIONS (
path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
)

*/

// COMMAND ----------

// Escribir dataframe en un archivo json
df.write.format("json").mode("overwrite").save("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/paruquetcp4JSON")

// COMMAND ----------

// Leer de un archivo CSV a un dataframe

val file = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/csv/*"
val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
val df = spark.read.format("csv")
.schema(schema)
.option("header", "true")
.option("mode", "FAILFAST") // Exit if any errors
.option("nullValue", "") // Replace any null data with quotes
.load(file)



// COMMAND ----------

/* SQL

CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
USING csv
OPTIONS (
path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
header "true",
inferSchema "true",
mode "FAILFAST"
)

*/

// COMMAND ----------

// Escribir dataframe en un CSV
df.write.format("csv").mode("overwrite").save("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/csvEscritos")


// COMMAND ----------

// Leer de un archivo AVRO a un DataFrame
ç
val df = spark.read.format("avro")
.load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")
df.show(false)

// COMMAND ----------

/* SQL

CREATE OR REPLACE TEMPORARY VIEW episode_tbl
USING avro
OPTIONS (
path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
)

*/

// COMMAND ----------

// Escribir contenido de DataFrame en avro
df.write.format("avro").mode("overwrite").save("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/avro")

// COMMAND ----------

// Leer ORC con dataframe
val file = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/part_r_00000_2c4f7d96_e703_4de3_af1b_1441d172c80f_snappy.orc"
val df = spark.read.format("orc").load(file)
df.show(10, false)


// COMMAND ----------

/* CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
USING orc
OPTIONS (
path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
) */

// COMMAND ----------

// Escribir el dataframe en un archivo orc
df.write.format("orc").mode("overwrite").option("compression", "snappy").save("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/orc")

// COMMAND ----------

// Imagenes
import org.apache.spark.ml.source.image

val imageDir = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/imagenes/"
val imagesDF = spark.read.format("image").load(imageDir)
imagesDF.printSchema
imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode").show(5,false)




// COMMAND ----------

// Leer archivo binario en un dataFrame

val path = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/imagenes/"
val binaryFilesDF = spark.read.format("binaryFile")
.option("pathGlobFilter", "*.jpg")
.option("recursiveFileLookup", "true")
.load(path)
binaryFilesDF.show(5)

// COMMAND ----------

val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val tC = Seq(t1, t2).toDF("celsius")
tC.createOrReplaceTempView("tC")
tC.show()

// COMMAND ----------

// Transform() produce un array aplicando una funcion a cada elementos del input array 
spark.sql("""SELECT celsius,transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

// COMMAND ----------

// Filter() produce un array de solo los elementos del input array en los que la funcion Booleana es true
spark.sql("""SELECT celsius,filter(celsius, t -> t > 38) as high FROM tC""").show()

// COMMAND ----------

// exists() devuelve true si la funcion booleana es cierta para cualquier elemento del array
spark.sql("""SELECT celsius,exists(celsius, t -> t = 38) as threshold FROM tC""").show()

// COMMAND ----------

// reduce() reduce los elmeentos del array a un unico valor mezclando los elementos en un buffer B usando la funcion <B, T, B> y aplicando una funcion <B, R> en el buffer final
spark.sql("""SELECT celsius,reduce(celsius,0,(t, acc) -> t + acc,acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC""").show()
