package spark_Context_Programs

//Check Below for more information
//https://mapr.com/blog/using-apache-spark-dataframes-processing-tabular-data/

import org.apache.spark.{SparkContext,SparkConf}


case class user_defined_schema(Atm_no:Integer,Name:String,Adreess:String ,Account_Balance:String);

object SparkSqlTextFile_Operations {
  
  def main(args:Array[String])
  {
    
    
   var configuration = new SparkConf().setAppName("The pipe file operations").setMaster("local");
   var sc = new SparkContext(configuration)
   var ctext = sc.textFile("C:\\Users\\jabin\\Desktop\\delimited_files\\sample_file.txt")
   var words = ctext.map( a  => a.split("|")).map( c=> user_defined_schema(c(0).toInt,c(1) ,c(2), c(3)))
   words.foreach(println)
   //words.toDF();
   
   
    
  }
}