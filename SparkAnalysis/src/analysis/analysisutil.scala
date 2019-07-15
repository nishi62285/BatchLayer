package analysis
import org.apache.spark._
import org.apache.spark.rdd.RDD
import Entity._


class analysisutil 
{
  
def CalculateTypeAverage(rdd:RDD[SysInfo],t : String): RDD[(String,String,Int)]=
{
  
  type Type1Collector = (Int,Int)
  type Type1Score=(String,(Int,Int))
  val createType1Combiner= (s:Int)=>(1,s)
  val type1Combiner = (a:Type1Collector,s:Int)=>
    {
      val (c,d)=a
      (c+1,d+s)
    }
  val type1Merger=(a:Type1Collector,b:Type1Collector)=>{
      val (c,d)=a
      val (e,f)=b
      (c+e,d+f)
    }
  val h = rdd.map(x=>(x.event_date+"--"+x.type1,1))
  val l =h.combineByKey(createType1Combiner,type1Combiner,type1Merger)
  val m=l.map(x=>(x._1.split("--")(0),x._1.split("--")(1),x._2._1/x._2._2))
  return m
} 

}
