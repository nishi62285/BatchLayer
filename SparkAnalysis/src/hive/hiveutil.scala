package hive
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Entity._
class hiveutil {
  
  def SaveToHive(sysInfoRDD:RDD[SysInfo],sparkss : SparkSession)
  {
    import sparkss.implicits._
    val n =sysInfoRDD.toDF()
    n.createOrReplaceTempView("sysinfo")
    n.write.partitionBy("type2").format("orc").saveAsTable("sysinfomaster_par");
    n.write.format("orc").saveAsTable("sysinfomaster");
  }
}