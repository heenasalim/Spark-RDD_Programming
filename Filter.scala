package spark_Context_Programs

//import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.{SparkContext,SparkConf}
object Filter {
  
  def main(args:Array[String])
  {
   // RDD should be printed by using foreach
    var configuration =  new SparkConf().setAppName("Filter Apllictions").setMaster("local")
    var sc = new SparkContext(configuration);
    var text_files = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt");
    var filter_size = text_files.filter(a => (a != 70000));
    filter_size.foreach(println);
//____________________________________________________________    
    var t = Array(2,4,3,5,7)
    var create_rdd = sc.makeRDD(t);
    println("*******The divident greater than 2**********")
    t.filter(content => (content/2 > 2 )).foreach(println);
  // check_file_size.foreach(println)
    
    println("***** The remider should  be equal zero*******")
    t.filter( a => (a%2 == 0)).foreach(println);
  } 
}
