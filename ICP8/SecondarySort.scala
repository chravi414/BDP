package com.wordcount.icp8
import org.apache.spark.{ SparkConf, SparkContext }

object SecondarySort {
  def main(args: Array[String]) {
    // Creating the SparkConf instance
    val conf = new SparkConf()
    conf.setAppName("Secondary Sort")
    conf.setMaster("local")

    // Creating SparkContext instance using conf object
    val sc = new SparkContext(conf)

    // Reading input from the text file
    val persons = sc.textFile("input/persons.txt")

    // Splitting the data and mapping with both first name and last name as keys
    val pairs = persons.map(_.split(",")).map { k => (k(0), k(1)) }

    // Printing the Mapper Output
    println("===== Mapper output =====")
    pairs.foreach(println)

    // Setting the Number of Reducers to 2
    val numReducers = 2;

    // Grouping by key and creating the list of values from each key
    val list = pairs.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))


    // Printing the Grouping Output
    println("===== Grouping output =====")
    list.foreach(println)

    // Applying flatmap transformation to convert Grouped output to Key value pair

    val resultRDD = list.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    // Printing Final Result
    resultRDD.foreach(println)
  }
}