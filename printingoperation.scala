package spark_Context_Programs

import org.apache.spark.{SparkContext,SparkConf}
//import Tuple._

//Actions and Tranformations are RDD operations
//Actions:- https://www.supergloo.com/fieldnotes/apache-spark-examples-of-actions/

object printingoperation { 
 
  def main(args:Array[String]) {
      val configuration = new SparkConf().setAppName("Printing the values").setMaster("local");
      var sc = new SparkContext(configuration);
      var read_file = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt");
     
      //Print the RDD by the following two functions only
      println( read_file.collect().mkString("|"));
      read_file.foreach(println)
      
      //print the integer element
       var stuple = (1,23,22,56);
       println(stuple)
       
      // print the string values
       
       var sarray = Array("heena" , "salim")
       for (  a  <- sarray)
       println(s"The name is :$a ")
   
     // print the top elements it only prints the top elements if the data types are same for each elements
       var t:Seq[Int] =  Seq(1,2,3)
       var rdd = sc.makeRDD(t);  
       println(rdd.top(4));   //printing the innteger
       
       //printing the array of the top elements
       // No any concet of the map in spark context 
       //var smap:Map[Int,String] = Map((1 -> "Apple"),(2 -> "Mango"));
       //create rdd now
       
      
       //Taking the elements for maps
       //Map RDD /Hash Map RDD
       //var key_value:RDD[(Int,String)]  =  {(1)(2)}
       
        println(rdd.take(4))   //it just take the elements not  print it
        println(rdd.first())
         
         
       
      //touple cannot b created as rdd
      
       // data shouldbe sent to spark engine and then we map the data t the tuple
       
      // println(read_file)//collect(println);
      //  Now , whole the file is in string format to access the column values 
      //  we need to separate the words
      // flatmap is use to separate the word and printing  it is use along with split
      // so split is there to separate the words alongwith flatmap  and printing
      //to print individual element
      
    
      var individual_element = read_file.flatMap(a=>a.split("|"));
     // println(individual_element);// dont use thiss method here
     // individual_element.foreach(println)  // this prints the elements one by one
      println( individual_element.collect().mkString(""));
      
      //print method to print the file elements
      println(read_file.collect().mkString("|"));
      
      
    }
    
}