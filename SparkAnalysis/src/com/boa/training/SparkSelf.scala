package com.boa.training
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext._
import mongo._
import Entity._
import hive._
//import scala.reflect.api.Scopes
////import org.apache.spark.sql.implicits._
//import org.apache.spark.sql.SaveMode
//import com.mongodb.spark._
import analysis._
import hdfs.hdfsfileioutil
import java.util.Calendar
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
object SparkSelf extends App{
  //val jars = Seq("/opt/spark/jars/spark-core_2.11-2.3.3.jar")
  val conf= new SparkConf().setAppName("SparkBatchAnalysis")
  //.set("spark.driver.host","10.10.10.1")
  .setSparkHome("/opt/spark")
  //set("spark.serializer","org.apache.spark.serializer.KryoSerializer")  
  //val sc= new SparkContext(conf)
  //config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.mycollection").
  //config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.mycollection").
  //config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.3.2").
  //getOrCreate
  
val sparks = SparkSession.builder.appName("BatchLayer")
    .config("hive.exec.dynamic.partition","true")
    .config("hive.exec.dynamic.partition.mode","nonstrict")
  //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.mycollection")
  //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.mycollection")
  //.config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
  .getOrCreate
  
import sparks.implicits._

val sc=sparks.sparkContext
val a:RDD[String] = sc.textFile("hdfs://master:9000/newcbs.txt")
val b = a.map(x=> x.split("  "))
val c =b.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11)))
val e = c.map(x=>(x._1.split(" "),x._10,x._12))
val f=e.map(x=>SysInfo(x._1(0),x._1(1).replace(",",""),x._1(2),x._2,x._3))
//val e = c.map(x=>(x._1.split(" "),x._10,x._12))
//val n = f.toDF()
val data :Array[SysInfo]= f.take(1)
val time =DateTimeFormatter.ofPattern("hhmmss").format(LocalDateTime.now)

new hdfsfileioutil(sc).SaveToHDFS(new analysisutil().CalculateTypeAverage(f, ""), "/BatchData","/"+data(0).event_date.toString()+time.toString()) 
//new mongoutil().SaveToMongo(f, sparks);
new hiveutil().SaveToHive(Array("event_date","type1"),f,sparks);
}
