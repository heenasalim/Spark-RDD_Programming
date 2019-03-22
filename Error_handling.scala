package spark_Context_Programs
import org.apache.spark.{SparkConf,SparkContext}
import scala.util.{Try ,Success,Failure}

object Error_handling {
  
  var total =0
  def main(args:Array[String]):Unit = {
  var configuration =  new SparkConf().setAppName("Exceptional Handling").setMaster("local")
  var sc = new SparkContext(configuration);
  var list = sc.parallelize(List(1,2,3,44,55))
  

   def calculate():Int =
    {
    if(list.isEmpty())   
     throw new Exception("Elements not fond");
    
    total = list.fold(0)((a,b) => a +b)
    return total
     
    }
  
   Try(calculate())  match
    {
     case Success(_)  => println(s"The script completed Successfully : $total")
     case Failure(_) =>  println ("The script has failed")
    
    }
    
  }
}