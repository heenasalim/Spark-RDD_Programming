package spark_Context_Programs

import org.apache.spark.{SparkConf,SparkContext}
import collection._


object Maths_Functions {
  
  def main(args:Array[String]) 
  {
    
    //Subtract by keys is use to eliminate the common key elements
    //Subtract is used to eliminate common key value pair
    var configuration = new SparkConf().setAppName("The Mathmatical Operation").setMaster("local")
    //Take the examples of the frequency
    var sc = new SparkContext(configuration)
    var Map1:Map[Any,Any]= Map(1 -> 90, 3 -> 100 ,5 -> 900, 9 ->800, 5->100)
    var Map2:Map[Any,Any]= Map(4 -> 50,  9-> 200 ,5 -> 1030, 9-> 800, 3->267)
    //Reduce  of the data 
    var Map1RDD = sc.parallelize(Map1.toSeq, 2)
    var Map2RDD = sc.parallelize(Map2.toSeq, 3);
    
    //Find out distinct elements in RDD
    var distict_elements = Map1RDD.distinct();
    println(distict_elements.collect().mkString("|"))
    

    //Max, min, sum, mean
    
    var t1 =  sc.parallelize(Array(1,2,3,4,8))
    println(t1.max())
    
    
    //Minimum Element
    
    var t2 =  sc.parallelize(Array((1,2),(2,3),(3,4)));
    var mins = t2.min()
    println(mins)
    
    //Sum of the elements
    //There is no any metod name Sum we have to define it
    //var sums =  t1.reduce((sc.parallelize(Array(2,3,4,5,6)))
    //println(sums)
    //Sum is only for the not  single number not colllection
    
     
    
  }

}