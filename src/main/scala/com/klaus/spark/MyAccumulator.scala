package com.klaus.spark


import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


/**
 * 自定义累加器
 */

class MyAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[Long, CategoryCountInfo]] {

  var map: mutable.Map[Long, CategoryCountInfo] = mutable.Map[Long, CategoryCountInfo]()

  override def isZero: Boolean = map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[Long, CategoryCountInfo]] = {

    new MyAccumulator()
  }

  //清空累加器
  override def reset(): Unit = map.clear()

  // 增加数据
  override def add(v: UserVisitAction): Unit = {

    val temp = CategoryCountInfo(v.click_category_id, 0, 0, 0)

    if (v.click_category_id != -1) {

      map(v.click_category_id) = CategoryCountInfo(v.click_category_id, map.getOrElse(v.click_category_id, temp).clickCount + 1, map.getOrElse(v.click_category_id, temp).orderCount, map.getOrElse(v.click_category_id, temp).payCount)

    } else if (v.order_category_ids != "null") {

      val ids = v.order_category_ids.split(",")

      for (i <- 0 until ids.length) {

        map(ids(i).toLong) = CategoryCountInfo(ids(i).toLong, map.getOrElse(ids(i).toLong, temp).clickCount, map.getOrElse(ids(i).toLong, temp).orderCount + 1, map.getOrElse(ids(i).toLong, temp).payCount)
      }
    } else if (v.pay_category_ids != "null") {

      val ids = v.pay_category_ids.split(",")

      for (i <- 0 until ids.length) {

        map(ids(i).toLong) = CategoryCountInfo(ids(i).toLong, map.getOrElse(ids(i).toLong, temp).clickCount, map.getOrElse(ids(i).toLong, temp).orderCount, map.getOrElse(ids(i).toLong, temp).payCount + 1)
      }
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[Long, CategoryCountInfo]]): Unit = {

    val map1 = map

    val map2 = other.value

    //空值对象
    val temp = CategoryCountInfo(9, 0, 0, 0)

    map = map1.foldLeft(map2) {

      (map, kv) => {

        map(kv._1) = CategoryCountInfo(kv._1, map.getOrElse(kv._1, temp).clickCount + kv._2.clickCount, map.getOrElse(kv._1, temp).orderCount + kv._2.orderCount, map.getOrElse(kv._1, temp).payCount + kv._2.payCount)
      }
        map
    }
  }


  // 累加器的值，其实就是累加器的返回结果
  override def value: mutable.Map[Long, CategoryCountInfo] = map
}
