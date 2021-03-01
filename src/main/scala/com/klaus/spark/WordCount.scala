package com.klaus.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setMaster("local[2]").setAppName("WC")
    val sc = new SparkContext(conf)
    //本地模式
//    sc.textFile("C:\\Users\\Administrator\\Desktop\\idea_workspace\\spark1\\input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println(_))
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    sc.stop()

  }

} 
