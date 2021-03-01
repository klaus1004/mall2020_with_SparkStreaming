package com.klaus.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Examp2 {
  def main(args: Array[String]): Unit = {
    Option
    //创建sparkConf并设置app名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkCOntext,该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val valueRdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\idea_workspace\\spark1\\src\\main\\resources\\user_visit_action.txt")

    val arrayRdd = valueRdd.map(_.split("_"))

    val ObjectRdd = arrayRdd.map(
      array => {
        UserVisitAction(
          array(0),
          array(1).toLong,
          array(2),
          array(3).toLong,
          array(4),
          array(5),
          array(6).toLong,
          array(7).toLong,
          array(8),
          array(9),
          array(10),
          array(11),
          array(12).toLong
        )
      }
    )
    //创建累加器
    val accumulator1 = new MyAccumulator()
    //注册累加器
    sc.register(accumulator1)
    //使用累加器
    ObjectRdd.foreach(
      data=>{
        accumulator1.add(data)
      }
    )
    val map = accumulator1.value
    val list = map.map(_._2).toList
    val result = list.sortBy(data => {
      (-data.clickCount,
        -data.orderCount,
        -data.payCount)
    }).take(10)

    println(result)
    //关闭连接
    sc.stop()
  }
}
//case class UserVisitAction(date: String,//用户点击行为的日期
//                           user_id: Long,//用户的ID
//                           session_id: String,//Session的ID
//                           page_id: Long,//某个页面的ID
//                           action_time: String,//动作的时间点
//                           search_keyword: String,//用户搜索的关键词
//                           click_category_id: Long,//某一个商品品类的ID
//                           click_product_id: Long,//某一个商品的ID
//                           order_category_ids: String,//一次订单中所有品类的ID集合
//                           order_product_ids: String,//一次订单中所有商品的ID集合
//                           pay_category_ids: String,//一次支付中所有品类的ID集合
//                           pay_product_ids: String,//一次支付中所有商品的ID集合
//                           city_id: Long)//城市 id
//// 输出结果表
//case class CategoryCountInfo(var categoryId: Long,//品类id
//                             var clickCount: Long,//点击次数
//                             var orderCount: Long,//订单次数
//                             var payCount: Long)//支付次数
