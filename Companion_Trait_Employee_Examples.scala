package spark_Context_Programs
import org.apache.spark
import scala.collection.mutable._
import collection._   //hashmap does not exist in collection class it exists in scala collection class
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success ,Failure}
 trait geeneric_customer_information
 {
  var account_holder_name ="" 
  private var balance_update:Boolean = true;
  private var balanc_update_id:Boolean = true
  var balance =0 
  var transaction_entries:HashMap[Int,Int]  = new HashMap[Int,Int]();
   // this entry is overridden in companion class
  transaction_entries += 786 -> 0
  transaction_entries.put(132,6779)
  transaction_entries += 1245 -> 100
  //var transcation_in_detail = Array((account_holder_name,account_no))  
  def deposite_transaction(customer_order:String,account_no:Int,amount:Int):Any
  {
    //trait overridden in below class
  }
  def withdrawl_transaction(){}
 }
  
class account_no extends geeneric_customer_information//(trait)
{

  for( a <- account_no.customer_tokens)
  println("Customer tokens:" + a)  
  
  override def deposite_transaction(customer_order:String,account_no:Int,amount:Int):Any= 
  {         
  
    for (  a <- transaction_entries )   {
        
    if (a._1 ==  account_no)
    { 
     transaction_entries(account_no) += amount ;
     return  transaction_entries(account_no)
    
    }
    
   
    else
      println(a)
      throw new Exception("No such account exists ")
      
    }  
  
    }
  override def withdrawl_transaction()
      {
    
      }
    
}
  
object account_no{// for customer from employee
 var customer_tokens:List[Int] =  List(1,2,3,4)
  def main(args:Array[String]) {
  var s = new  account_no()
  Try (s.deposite_transaction("Customer have request to add the money in my account",1245,5000)) match {
 
  case Success(a) => println(s"Amount in $account_no is $a")
  case Failure(_) => println("The Exception has occured")
 
  }
 
  
 
 } 
  }
