package spark_Context_Programs


import org.apache.spark.{SparkConf,SparkContext}
object MapnFlatMap {
  
  def main(args:Array[String])
  {
    
    //Map is useful for doing some operation with object  like . 
    // it is similar to the map but it does not print the value at any cost
    //Also it returns the sequence as the result
  
    var configuration  =  new SparkConf().setAppName("Map and latMap Use").setMaster("local");
    var sc = new SparkContext(configuration);
    var text_files = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt");
    
    // flatmap is also useful for doing the operations and printing the elements
    // flatmap returns the individual string element
    
    var flat_map_example = text_files.flatMap(x  => x.split("|"));
  
    var map_examples = text_files.map( x => x.split("|"));
    
   
    
    //println(map_examples.foreach(println))  
    println(flat_map_example.foreach(println))
    
    
    var filtered_data =text_files.filter( a => a.contains("70000"));
    // filter is use for applying conditions on the data
   filtered_data.foreach(println)
    
    
  };
  
  
}