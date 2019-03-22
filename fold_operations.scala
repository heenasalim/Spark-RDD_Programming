package spark_Context_Programs

//Read it :-//https://commitlogs.com/2016/09/10/scala-fold-foldleft-and-foldright/
//https://www.credera.com/blog/credera-site/mastering-scala-folding/

object fold_operations {
  
  def main(args:Array[String]){    
   // Syntax
   // val variable  = collection_name.(initial_value)(operation)
   // add the values using fold
    var l = List(2,4,5,6)  
    val sum = l.fold(0)(_+_)
    val sum1 = l.fold(0){ (a,b) => (a + b) };
    
     //difference between add and fold
     // 1: no need of for loop in the fold
     // 2.variable initialization at the time of defining only
  
    
  }
  
}