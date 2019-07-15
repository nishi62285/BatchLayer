package hdfs
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
class hdfsfileioutil(val sc:SparkContext) {
  
  def IsPathExists(path:String):Boolean=
  {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    return fs.exists(new Path(path))
  }
  
  def SaveToHDFS(rdd:RDD[(String,String,Int)],path:String,fileName:String):Unit={
 println("Saving Data to HDFS :"+path+fileName)
    rdd.saveAsTextFile(path+fileName)
    println("Saved Data to HDFS :"+path+fileName)
  }
 
}