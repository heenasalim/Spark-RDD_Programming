package spark_Context_Programs
//Note :-Paired RDD
// https://acadgild.com/blog/spark-rdd-operations-scalax
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
object Union_Intersection_Joins {
 
def main(args:Array[String]){
    
var configuration = new SparkConf().setAppName("Joins/Uion/Intersecion").setMaster("local");
var sc  =  new SparkContext(configuration);
var textfile1 = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt");
var textfile2 = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_test.txt") 


//Create the paied RDD to separate the columns
var columns_of_the_textfile1:RDD[(Int,String,String,String,String)] = textfile1.map(line => line.split("|")).map(line => (line(0).toInt, line(1).toString(),line(2).toString,line(3).toString(),line(4).toString()));



// Union,Intersection

//Union
var union_data =textfile1.union(textfile2).collect.mkString("\n");
//Intersection
var intersection_data = textfile1.intersection(textfile2).collect().mkString("\n");
 println(s"The union od the files is $union_data");
 println(s"The intersection of the files is $intersection_data");

//join we do with sparksql only
 
 
 
    //Subtract by keys is use to eliminate the common key elements
    //Subtract is used to eliminate common key value pair
    
    var Map1:Map[Any,Any]= Map(1 -> 90, 3 -> 100 ,5 -> 900, 9 ->800, 5->100)
    var Map2:Map[Any,Any]= Map(4 -> 50,  9-> 200 ,5 -> 1030, 9-> 800, 3->267)
    //Reduce  of the data 
    var Map1RDD = sc.parallelize(Map1.toSeq, 2)
    var Map2RDD = sc.parallelize(Map2.toSeq, 3);
    var subtract_rdds = Map1RDD.subtract(Map2RDD)
    //RDD is printed by  collect and println
    println(subtract_rdds.collect.mkString("|"));
    
    var subtract_By_key = Map1RDD.subtractByKey(Map2RDD)
    println(subtract_By_key.collect().mkString("|"))
    
    //Find out distinct elements in RDD

}
}