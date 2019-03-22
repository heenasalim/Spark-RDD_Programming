package spark_Context_Programs

import org.apache.spark.{SparkConf,SparkContext}

object spark_contexts {

  def main(args:Array[String]) { 

     val configuration = new SparkConf().setAppName("The sparkcontext and configuration creation").setMaster("local");
     val sparkcontexts = new SparkContext(configuration);
     // I will not get the spark context unless and utill i dowload the libraries
  
     var textfile1 = sparkcontexts.textFile("C:\\Users\\jabin\\Desktop\\sample_test.txt"); 
     //textfile.collect();  
     // for printing the files using collect we need to make the files as string and delimeter should be given so use it as 
     println(textfile1.collect().mkString("|"))
     textfile1.foreach(println);   
 
    
     sparkcontexts.stop();

  }
}
  

 
  
