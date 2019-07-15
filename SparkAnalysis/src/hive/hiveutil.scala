package hive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Entity._
import org.apache.spark.sql.SaveMode
class hiveutil {
  
  def SaveToHive(partitionColumns:Array[String],sysInfoRDD:RDD[SysInfo],sparkss : SparkSession)
  {
   println("Saving Data to Hive") 
    import sparkss.implicits._    
    val n = sysInfoRDD.toDF()
    n.write.partitionBy("event_date","type1").mode(SaveMode.Append).option("path","/BatchData/MasterText/").saveAsTable("attemp4");
    println("Saved Data to Hive")
    //n.write.format("orc").saveAsTable("/type1/orc/sysinfomaster");
    //n.write.format("paraquet").saveAsTable("/type1/parquet/sysinfomaster");
  }
}