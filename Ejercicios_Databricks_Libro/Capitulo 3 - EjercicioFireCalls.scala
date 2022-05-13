// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//Crear schema 

val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
StructField("UnitID", StringType, true),
StructField("IncidentNumber", IntegerType, true),
StructField("CallType", StringType, true),
StructField("CallDate", StringType, true),
StructField("WatchDate", StringType, true),
StructField("CallFinalDisposition", StringType, true),
StructField("AvailableDtTm", StringType, true),
StructField("Address", StringType, true),
StructField("City", StringType, true),
StructField("Zipcode", IntegerType, true),
StructField("Battalion", StringType, true),
StructField("StationArea", StringType, true),
StructField("Box", StringType, true),
StructField("OriginalPriority", StringType, true),
StructField("Priority", StringType, true),
StructField("FinalPriority", IntegerType, true),
StructField("ALSUnit", BooleanType, true),
StructField("CallTypeGroup", StringType, true),
StructField("NumAlarms", IntegerType, true),
StructField("UnitType", StringType, true),
StructField("UnitSequenceInCallDispatch", IntegerType, true),
StructField("FirePreventionDistrict", StringType, true),
StructField("SupervisorDistrict", StringType, true),
StructField("Neighborhood", StringType, true),
StructField("Location", StringType, true),
StructField("RowID", StringType, true),
StructField("Delay", FloatType, true)))

// COMMAND ----------

// Guardar el archivo en el directorio

val sfFireFile="dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com"
val fireDF = spark.read.schema(fireSchema)
.option("header", "true")
.csv(sfFireFile)

// COMMAND ----------

fireDF.show(5)

// COMMAND ----------

val parquetPath  = "dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/parquetpath2"

// COMMAND ----------

fireDF.write.format("csv").save(parquetPath)

// COMMAND ----------


val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where($"CallType" =!= "Medical Incident")
fewFireDF.show(10,false)


// COMMAND ----------

fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct('CallType) as 'DistinctCallTypes).show()

// COMMAND ----------

//Cambiando nombre de la columna
val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedMins")
newFireDF.select("ResponseDelayedMins").where($"ResponseDelayedMins" > 5).show(5,false)


// COMMAND ----------

// Cambiar de formato
val fireTsDF = newFireDF.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate").withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")).drop("WatchDate").withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
"MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")

fireTsDF.select("IncidentDate", "OnwatchDate", "AvailableDtTS").show(5,false)

// COMMAND ----------

//Seleccionar año de un timestamp
fireTsDF.select(year($"IncidentDate"))
.distinct()
.orderBy(year($"IncidentDate"))
.show()

// COMMAND ----------

//Numero de tipos de llamada de cada tipo de llamada
fireTsDF
.select("CallType")
.where(col("CallType").isNotNull)
.groupBy("CallType")
.count()
.orderBy(desc("count"))
.show(10, false)

// COMMAND ----------


//import org.apache.spark.sql.{functions => F}
//fireTsDF
//.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
//F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
//.show()

// COMMAND ----------

// Ejemplo creando un dataset

case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

val ds = spark.read
.json("dbfs:/FileStore/shared_uploads/ivan.saez@bosonit.com/iot_devices.json")
.as[DeviceIoTData]

ds.show(5, false)

// COMMAND ----------

val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})
filterTempDS.show(5, false)

// COMMAND ----------

// Otro ejemplo de Dataset

case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
cca3: String)
val dsTemp = ds
.filter(d => {d.temp > 25})
.map(d => (d.temp, d.device_name, d.device_id, d.cca3))
.toDF("temp", "device_name", "device_id", "cca3")
.as[DeviceTempByCountry]
dsTemp.show(5, false)

// COMMAND ----------

// Usando funciones dataframes pero casteando a dataset al final
val dsTemp2 = ds
.select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
.where("temp > 25")
.as[DeviceTempByCountry]
dsTemp2.show(5,false)
