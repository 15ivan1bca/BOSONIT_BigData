// Databricks notebook source
// Hacemos los imports necesarios
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// Se guarda la ruta del archivo en una variable y se lee en un dataframe
val ruta = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/NASA_access_log_Aug95__1___1_.gz"
val df1 = spark.read.text(ruta)
df1.show()

// COMMAND ----------

// Regex de la direccion
val regexHost = "(^\\S+.[\\S+\\.]+\\S+)\\s"

val dfHost = df1.select(regexp_extract($"value", regexHost, 1).alias("direccion"))

dfHost.show()

// COMMAND ----------

//Regex de la fecha
val regexFecha = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]"
val dfFecha = df1.select(regexp_extract($"value", regexFecha, 1).alias("fecha"))

dfFecha.show()

// COMMAND ----------

// Regex del metodo HTTP, la uri y el protocolo
val regexMUP = "\042(\\S+)\\s(\\S+)\\s*(\\S*)\042"

val dfMUP = df1.select(regexp_extract($"value", regexMUP, 1).alias("method"), regexp_extract($"value", regexMUP, 2).alias("uri"), regexp_extract($"value", regexMUP, 3).alias("protocolo"))

dfMUP.show()

// COMMAND ----------

// Regex del estado del http
val regexEstado = "\\s(\\d{3})\\s"
val dfEstado = df1.select(regexp_extract($"value", regexEstado, 1).alias("estado"))

dfEstado.show()

// COMMAND ----------

// Regex del tamaño del archivo
val regexTamanio = "\\s(\\d+)$"
val dfTamanio = df1.select(regexp_extract($"value",regexTamanio, 1).alias("tamaño"))

dfTamanio.show()

// COMMAND ----------

// Juntamos todo en una tabla
val dfLogs = df1.select(regexp_extract($"value",regexHost, 1).alias("direccion"),
                             regexp_extract($"value", regexFecha, 1).alias("fecha"),
                             regexp_extract($"value", regexMUP, 1).alias("method"),
                             regexp_extract($"value",  regexMUP, 2).alias("uri"),
                             regexp_extract($"value", regexMUP, 3).alias("protocolo"),
                             regexp_extract($"value", regexEstado,1 ).cast("integer").alias("estado"),
                             regexp_extract($"value", regexTamanio, 1).cast("integer").alias("tamaño"))

dfLogs.show()

// COMMAND ----------

// Les asignamos a los valores nulos 0
val dfLogs2 = dfLogs.na.fill(0) 

// COMMAND ----------

//Mapa para asignar el numero de mes a cada mes
val mapa = typedLit(Map("Jan"->1,"Feb"->2,"Mar"->3,"Apr"->4,"May"->5,"Jun"->6,
"Jul"->7,"Aug"->8,"Sep"->9,"Oct"->10,"Nov"->11,"Dec"->12))

val dfLogs3 = dfLogs2.withColumn("fechayhora", concat(substring(col("fecha"),8,4),lit("-0"), mapa(split(col("fecha"), "/")(1)),lit("-"),
substring(col("fecha"),1,2),lit(" "), substring(col("fecha"),13,8))).withColumn("time",to_timestamp(col("fechayhora"))).drop("fecha").drop("fechayhora")

dfLogs3.show()

// COMMAND ----------

// 1. ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
val dfLogsProtocolo = dfLogs3.select("protocolo").distinct()
dfLogsProtocolo.show()

// COMMAND ----------

// 2. ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.
val dfLogsEstado = (dfLogs3.select("estado").groupBy("estado").agg(count("*").alias("frecuencia")).orderBy(col("frecuencia").desc).withColumn("descripcion", 
                    when(dfLogs3("estado") === "200","OK")
                    .when(dfLogs3("estado") === "302","Found")
                    .when(dfLogs3("estado") === "304","Not Modified")
                    .when(dfLogs3("estado") === "400","Bad Request")
                    .when(dfLogs3("estado") === "403","Forbidden")
                    .when(dfLogs3("estado") === "404","Not Found")
                    .when(dfLogs3("estado") === "500","Internal Server Error")
                    .when(dfLogs3("estado") === "501","Not Implemented")))

dfLogsEstado.show()

// COMMAND ----------

// 3. ¿Y los métodos de petición (verbos) más utilizados?
val dfLogsPeticion = (dfLogs3.select("method")
                        .groupBy("method")
                        .agg(count("*").alias("frecuencia"))
                        .orderBy(col("frecuencia").desc))

dfLogsPeticion.show()

// COMMAND ----------

// 4. ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
val dfLogsBytes = (dfLogs3.select("uri", "tamaño").orderBy(col("tamaño").desc))

dfLogsBytes.show(1)

// COMMAND ----------

// 5. Además, queremos saber qué recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros de nuestro log.
val dfLogsTrafico = (dfLogs3.select("uri")
                     .groupBy("uri")
                     .agg(count("*").alias("conteo"))
                     .orderBy(col("conteo").desc))
dfLogsTrafico.show(1)

// COMMAND ----------

// 6. ¿Qué días la web recibió más tráfico?
val dfLogsDias = (dfLogs3.select("time")
                  .groupBy(substring(date_trunc("day",col("time")),1,10).alias("Fecha"))
                  .agg(count("*").alias("Conteo"))
                  .orderBy(col("Conteo").desc))

dfLogsDias.show()

// COMMAND ----------

// 7. ¿Cuáles son los hosts más frecuentes?
val dfDireccion = (dfLogs3.select("direccion").groupBy(col("direccion")).agg(count("*").alias("Conteo")).orderBy(col("Conteo").desc))
dfDireccion.show()

// COMMAND ----------

// 8. ¿A qué horas se produce el mayor número de tráfico en la web?
val dfTiempo = (dfLogs3.select("time").groupBy(substring((date_trunc("hour", col("time"))),11,6).alias("Tiempo")).agg(count("*").alias("Conteo")).orderBy(col("Conteo").desc))
dfTiempo.show()

// COMMAND ----------

// 9. ¿Cuál es el número de errores 404 que ha habido cada día?
val dfError = (dfLogs3.select("time","estado").filter(dfLogs3("estado") === "404")
               .groupBy(substring(col("time"),1,10).alias("Fecha"))
               .agg(count("*").alias("Conteo"))
               .orderBy(col("Conteo").desc))
display(dfError)
