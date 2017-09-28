package comp9313.ass3

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Problem2 {
  
  def main(args:Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    
    val conf = new SparkConf().setAppName("reverse a graph").setMaster("local")
    val sc = new SparkContext(conf)
    
    val input = sc.textFile(inputFile)
    
    val input_to_array = input.collect()

    val transformed_input = input_to_array.map { x => (x.split("\t")(0), x.split("\t")(1)) }
    
    val reversed_input = sc.parallelize(transformed_input).map(x => (x._2, x._1)).reduceByKey((x, y) => (x.toString() + ":" + y.toString()))                     
    
    val sorted_output_array = reversed_input.collect().sortBy(_._1)
    
    val builder = StringBuilder.newBuilder

    for(i <- 0 to sorted_output_array.length-1) {
      val adjacency_list = sorted_output_array(i)._2.split(":").sorted
      val neighbour_builder = StringBuilder.newBuilder
      var prefix = ""
      for(i <- 0 to adjacency_list.length-1) {
        neighbour_builder.append(prefix)
        prefix = ","
        neighbour_builder.append(adjacency_list(i))
      }
      val neighbour = neighbour_builder.toString()
      builder.append(sorted_output_array(i)._1 + "\t" + neighbour)
      builder.append("\n")
    }
    
    val output = sc.parallelize(builder.toString().split("\n")).map { x => x }
    output.saveAsTextFile(outputFolder)

  }
}