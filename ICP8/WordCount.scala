package com.wordcount.icp8

import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {
    /* configure spark application */
    val conf = new SparkConf()
    conf.setAppName("WordCount Program")
    conf.setMaster("local")

    /* creating spark context instance */
    val sc = new SparkContext(conf)

    /* Reading Input file and splitting data into words and mapping each word with 1 */
    val map = sc.textFile("input/data.txt").
      flatMap(line => line.split(" ")).
      map(word => (word, 1))

    /* Summing up the words */
    val counts = map.reduceByKey(_ + _)

    /* printing the word and count */
    counts.collect().foreach(println)

    /* Checking for a particular word  using map and getting that value count */

    val specificWordMap = sc.textFile("input/data.txt").
      flatMap(line => line.split(" ")).
      filter(word => word.toLowerCase.charAt(0) == 's').
      map(specificWord => (specificWord, 1))

    val specificWordCount =  specificWordMap.reduceByKey(_+_)

    specificWordCount.collect().foreach(println)
    sc.stop()
  }
}
