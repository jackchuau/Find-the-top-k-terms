package comp9313.ass3

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.util.Sorting.stableSort


object Problem1 {
  
  
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val k = args(2).toInt
    val conf = new SparkConf().setAppName("find top k terms").setMaster("local")
    val sc = new SparkContext(conf)
    
    val input = sc.textFile(inputFile)

    var valid_terms:List[String] = List()
    for (line <- input.collect() )  {
      val string = line.toString()
      val words = string.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").filterNot{_.isEmpty()}
      var scanned_string:List[String] = List()
      for(word <- words) {
        if (Character.isLetter(word.charAt(0)))  {
          if (!scanned_string.contains(word.toLowerCase())) {
            valid_terms = word.toLowerCase() :: valid_terms
            scanned_string = word.toLowerCase() :: scanned_string
          }        
        }
      }
    }
    
    val term_pairs_RDD = sc.parallelize(valid_terms).map(word => (word, 1)).reduceByKey(_+_)
    
    val term_pairs_list = term_pairs_RDD.collect()
    
    val sorted_term_pairs = term_pairs_list.sortBy( x => (-x._2, x._1))

    var top_k_terms_list:List[(String, Int)] = List()
    for(i <- 0 to k-1) {
      top_k_terms_list ::= sorted_term_pairs(i)
    }
    
    val builder = StringBuilder.newBuilder

    for(i <- 0 to k-1) {
      builder.append(top_k_terms_list(k-1-i)._1 + "\t" + top_k_terms_list(k-1-i)._2 + "\n")
    }

    val output = sc.parallelize(builder.toString().split("\n")).map(x => x)
    output.saveAsTextFile(outputFolder)
  } 
}
