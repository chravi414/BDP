package com.spark.icp9
import org.apache.spark._
object MergeSort {
  def main(args: Array[String]) {

    /* Setting the Win utils path as HADOOP_HOME */
    System.setProperty("hadoop.home.dir", "E:\\UMKC\\Summer Sem\\BDP")

    /* Creating instance of Spark Config and setting App Name, Master */
    val conf = new SparkConf()
    conf.setAppName("merge sort")
    conf.setMaster("local")

    /* Create a Scala Spark Context.*/
    val sc = new SparkContext(conf)


    /* Reading Input and creating RDD */
    val inputList = List(1,2,3,4,1,2,3,4)
    val inputListRDD = sc.parallelize(inputList)

    /* Merge Sorting using Map and Reduce */
    val mappedList = inputListRDD.map(a=> (a,1))
    val sortedList = mappedList.sortByKey()

    /* Collecting the Keys of sorted List output */
    println(" Output using SortByKey function \n")
    sortedList.keys.collect().foreach(println)

    /* Merge Sort using a user defined function */
    def mergeSort(input: List[Int]): List[Int] = {
      val n = input.length / 2
      if (n == 0) input
      else {
        def merge(leftList: List[Int], rightList: List[Int]): List[Int] =
          (leftList, rightList) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(a :: leftList1, b :: rightList1) =>
              if (a < b) a::merge(leftList1, rightList)
              else b :: merge(leftList, rightList1)
          }
        val (left, right) = input splitAt(n)
        merge(mergeSort(left), mergeSort(right))
      }
    }
    println(" Output using user Defined function \n")
    println(mergeSort(inputList))
  }
}
