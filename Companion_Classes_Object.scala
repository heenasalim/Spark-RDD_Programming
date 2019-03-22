package spark_Context_Programs


class fruit{  
  var multiplication:Int = 0;
  def mango_method()
  {
   println("In companion class")
   for (  a <- fruit.sample_array)
   multiplication += a 
   
   // we can directly access the objects methods/variables in class
   println(s"*******  accessing object fruit's variable :  $fruit.total  **********");
 
   }
}
object fruit{
  
  
  var sample_array =  Array(1,2,4,2,6)
  var total:Int =0
  def papaya_method()
  { 
    println("In companion object")
    for (  a <- sample_array)
    total += a;
  } 
       
   //If we have to access the methods of the companion class in companion objects then
   //instantiate the companion class then access the methods  and variables
   
   //so initializing the fruits class
    var f  = new fruit();
    println(s"*********** accessing the class variables here: $f.multiplication *************");
  
  
}



  
  
  
  
  
  
  