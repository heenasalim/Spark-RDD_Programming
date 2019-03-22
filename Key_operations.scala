package spark_Context_Programs

import org.apache.spark.{SparkConf,SparkContext}
import collection.mutable.HashMap

object Key_operations {
  
  
  def main(args:Array[String])
  {
      
  var configuration =  new SparkConf().setAppName("Te key operations").setMaster("local");
  var sc = new SparkContext(configuration)
  var  smap = Map((1 -> "Apple") , 
                  (5 -> "Mango"),
                  (3  -> "PineApple"),
                  (2 -> "Pomogranate")
   )
    
    
 //  Now , we cannot directly opeate with map or kevalue pair in scla
 // so first convert it in sequence  
   println(smap.toSeq.sortBy(_._1));
 
 //  _._1 is the first feild  which is key
   
   //sort_with function  for ascending and decending order
   
   //Ascending order 
   println(smap.toSeq.sortWith(_._1 < _._1))
   //Descending order
//   println(smap.toSeq.sortWith(_._1 > _._1));  /same as sort by
   
   
   //***************ContByKey*********************/
     //it counts how many keys are their
   //we will see count by key with HashMap and Sparkcontext
   
 var h:HashMap[Int,String] = HashMap((1 -> "Ajinkya"),(2 -> "Heena"),( 11 -> "Praveen"),(11 -> "Praveen") ,( 3 -> "Satendra"),(4 -> "Rahul"))
   //to create rdd we have converted map to sequence
 var rdds = sc.makeRDD(h.toSeq);
 var rdd2  =  sc.makeRDD(smap.toSeq)
 println(rdds.countByKey());
 rdds.countByValue().foreach(println)
// Count By,CountWith does not exists for rdds  println(rdds.countBy(_._1);
 
 println(rdds.sortByKey().collect().mkString(""));  //to print the string
 //sortWith,countBy is not applied on the rdds
 // sort keys provies unique elements
 println(rdds.sortBy(_._1).collect().mkString(""))   // ascending order
 //does not exists println(rdds.sortWith( _._1 > _._1).collect.mkString())
 
 // group by key also deals with the unique keys
 println(rdds.groupByKey().collect().mkString(""));
 println(rdds.groupBy(_._1).collect().mkString(""));  //.. explicitly need to provide the keys
// see later  println(rdds.groupWith.rdd2);
 
 // reduce  by and reducebykey operations
 
 var textfile  = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt");
 textfile.foreach(println);
 var words =textfile.collect().mkString("|");
 
 var stuple = Array(1,3,5,8,9) 
 //Make it rdd then only we can do reduce operation
 var rdd=  sc.parallelize(stuple)
 var sum = rdd.reduce(_+_)
 println(sum)

 var hmap= HashMap((1 -> 3),( 2 -> 4),( 3 -> 5));
 
 //map should be converted into sequence in order to perform the operations
 
 // the reduce by key doe not wor with sequence hence doesnot with map hashmap
 var hpair = Array((101,1),(102,2),(103,3))
 var rdd1  = sc.parallelize(hpair);
 var sum1 = rdd1.reduceByKey(_+_);
// var sum2 = rdd1.fold(0,0)(_.2+_.2)
 
 sum1.foreach(println)
 
 
 
 

 
 
  }
}