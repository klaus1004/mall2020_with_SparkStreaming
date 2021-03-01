package com.klaus.spark

import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD_file {
  def main(args: Array[String]): Unit = {
    //创建sparkConf并设置app名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkCOntext,该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://hadoop102:8020/input")

    rdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
