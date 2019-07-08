package mongo
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql.SaveMode
import com.mongodb.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Entity._
class mongoutil 
{
  
  def SaveToMongo(b:RDD[SysInfo],sparkss : SparkSession)
  {
    import sparkss.implicits._
    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1:27017/test.mycollection", "readPreference.name" -> "secondaryPreferred"))
    MongoSpark.load(sparkss,readConfig)
    MongoSpark.save(b.toDF().write.option("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.mycollection").mode("overwrite"))
  }

}