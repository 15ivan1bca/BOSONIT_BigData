// Databricks notebook source
// Definir una clase case Scala que defina los campos
case class Bloggers(id:Int, first:String, last:String, url:String, date:String,
hits: Int, campaigns:Array[String])

val bloggers = "../data/bloggers.json"
val bloggersDS = spark.read.format("json").option("path", bloggers).load().as[Bloggers]

// COMMAND ----------

// JavaBean clase de tipo Bloggers
/*
import org.apache.spark.sql.Encoders;
import java.io.Serializable;
public class Bloggers implements Serializable {
private int id;
private String first;
158 | Chapter 6: Spark SQL and Datasetsprivate String last;
private String url;
private String date;
private int hits;
private Array[String] campaigns;
// JavaBean getters and setters
int getID() { return id; }
void setID(int i) { id = i; }
String getFirst() { return first; }
void setFirst(String f) { first = f; }
String getLast() { return last; }
void setLast(String l) { last = l; }
String getURL() { return url; }
void setURL (String u) { url = u; }
String getDate() { return date; }
Void setDate(String d) { date = d; }
int getHits() { return hits; }
void setHits(int h) { hits = h; }
Array[String] getCampaigns() { return campaigns; }
void setCampaigns(Array[String] c) { campaigns = c; }
}
*/

// Crear encoder
Encoder<Bloggers> BloggerEncoder = Encoders.bean(Bloggers.class);
String bloggers = "../bloggers.json"
Dataset<Bloggers>bloggersDS = spark.read.format("json").option("path", bloggers).load().as(BloggerEncoder);

// COMMAND ----------

// Crear Dataset con Scala object
import scala.util.Random._
// Our case class for the Dataset
case class Usage(uid:Int, uname:String, usage: Int)
val r = new scala.util.Random(42)
// Create 1000 instances of scala Usage class
// This generates data on the fly
val data = for (i <- 0 to 1000)
yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
r.nextInt(1000)))
// Create a Dataset of Usage typed data
val dsUsage = spark.createDataset(data)
dsUsage.show(10)


// COMMAND ----------

// Lo mismo en java (tenemos que usar un Encoder explicito)

/*
// In Java
import org.apache.spark.sql.Encoders;
import org.apache.commons.lang3.RandomStringUtils;
import java.io.Serializable;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
// Create a Java class as a Bean
public class Usage implements Serializable {
int uid; // user id
String uname; // username
int usage; // usage
public Usage(int uid, String uname, int usage) {
this.uid = uid;
this.uname = uname;
this.usage = usage;
}
// JavaBean getters and setters
public int getUid() { return this.uid; }
public void setUid(int uid) { this.uid = uid; }
public String getUname() { return this.uname; }
public void setUname(String uname) { this.uname = uname; }
public int getUsage() { return this.usage; }
public void setUsage(int usage) { this.usage = usage; }
public Usage() {
}
public String toString() {
return "uid: '" + this.uid + "', uame: '" + this.uname + "',
usage: '" + this.usage + "'";
}
}
// Create an explicit Encoder
Encoder<Usage> usageEncoder = Encoders.bean(Usage.class);
Random rand = new Random();
rand.setSeed(42);
List<Usage> data = new ArrayList<Usage>()
// Create 1000 instances of Java Usage class
for (int i = 0; i < 1000; i++) {
data.add(new Usage(i, "user" +
RandomStringUtils.randomAlphanumeric(5),
rand.nextInt(1000));
// Create a Dataset of Usage typed data
Dataset<Usage> dsUsage = spark.createDataset(data, usageEncoder);
*/

// COMMAND ----------

import org.apache.spark.sql.functions._

// Filtro usando lambda function
dsUsage.filter(d => d.usage > 900).orderBy(desc("usage")).show(5, false)

// Lo mismo pero definciendo una funcion
def filterWithUsage(u: Usage) = u.usage > 900
dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

// COMMAND ----------

/*
// En java
// Define a Java filter function
FilterFunction<Usage> f = new FilterFunction<Usage>() {
public boolean call(Usage u) {
return (u.usage > 900);
}
};
// Use filter with our function and order the results in descending order
dsUsage.filter(f).orderBy(col("usage").desc()).show(5);
*/

// COMMAND ----------

// Use an if-then-else lambda expression and compute a value
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
.show(5, false)
// Define a function to compute the usage
def computeCostUsage(usage: Int): Double = {
if (usage > 750) usage * 0.15 else usage * 0.50
}
// Use the function as an argument to map()
dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)

// COMMAND ----------

// In Scala
// Create a new case class with an additional field, cost
case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)
// Compute the usage cost with Usage as a parameter
// Return a new object, UsageCost
def computeUserCostUsage(u: Usage): UsageCost = {
val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
UsageCost(u.uid, u.uname, u.usage, v)
}
// Use map() on our original Dataset
dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

// COMMAND ----------

// Convertir dataframes a datasets
val bloggersDS = spark.read.format("json").option("path", "/data/bloggers/bloggers.json").load().as[Bloggers]
