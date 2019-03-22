package spark_Context_Programs

import org.apache.spark.{SparkConf,SparkContext}
object PairedRDD {
 
  
  def main(args:Array[String])
  {
    var configuration  = new SparkConf().setAppName("The Paired RDD").setMaster("local")
    
    var sc  = new SparkContext(configuration);
    
    var  text1 = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt", 5)
    var  text2   = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_test.txt", 2)
  
    //paired rdd of the text1 and text2
    var feild_text1=  text1.map(con => con.split("|")).map( line => (line(0).toInt,line(1).toString(),
      line(2).toString(),line(3).toString   ))

    var feild_text2 = text2.map(con => con.split("|")).map(word =>  (word(0).toInt,word(1).toString(),
       word(1).toString(),word(2).toString()))
      
      
  }
  }