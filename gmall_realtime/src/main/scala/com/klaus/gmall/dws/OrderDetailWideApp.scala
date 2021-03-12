package com.klaus.gmall.dws

import com.alibaba.fastjson.JSON
import com.klaus.gmall.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.klaus.gmall.utils.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

//开滑动窗口进行双流join，再利用redis去重
object OrderDetailWideApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("orderDetailWide_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_ORDER_WIDE_CONSUMER"
    val DW_ORDER_DETAIL_topic = "DW_ORDER_DETAIL"
    val DW_ORDER_INFO_topic = "DW_ORDER_INFO"

    //从redis读取偏移量
    val DW_ORDER_DETAIL_orderOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, DW_ORDER_DETAIL_topic)
    val DW_ORDER_INFO_orderOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, DW_ORDER_INFO_topic)

    //根据偏移起始点获得数据
    //判断如果之前没有在redis保存，则从kafka最新加载数据
    var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (DW_ORDER_INFO_orderOffsets != null && DW_ORDER_INFO_orderOffsets.size > 0) {
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(DW_ORDER_INFO_topic, ssc, DW_ORDER_INFO_orderOffsets, groupId)
    } else {
      println("offset is null")
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(DW_ORDER_INFO_topic, ssc, groupId)
    }

    var orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (DW_ORDER_DETAIL_orderOffsets != null && DW_ORDER_DETAIL_orderOffsets.size > 0) {
      orderDetailInputDstream = MyKafkaUtil.getKafkaStream(DW_ORDER_DETAIL_topic, ssc, DW_ORDER_DETAIL_orderOffsets, groupId)
    } else {
      println("offset is null")
      orderDetailInputDstream = MyKafkaUtil.getKafkaStream(DW_ORDER_DETAIL_topic, ssc, groupId)
    }

    //获得偏移结束点
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
      orderInfo
    }
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail = JSON.parseObject(jsonString,classOf[OrderDetail])
      orderDetail
    }
    orderDetailDstream.cache()
    orderInfoDstream.cache()
    val orderDetailWinDstream = orderDetailDstream.window(Seconds(10), Seconds(5))
    val orderInfoWinDstream = orderInfoDstream.window(Seconds(10), Seconds(5))

//    orderDetailDstream.foreachRDD(rdd=>OffsetManager.saveOffset(groupId,DW_ORDER_DETAIL_topic,orderDetailOffsetRanges))
//    orderInfoDstream.foreachRDD(rdd=>OffsetManager.saveOffset(groupId,DW_ORDER_INFO_topic,orderInfoOffsetRanges))
//    orderInfoDstream.print(1000)
//    orderDetailDstream.print(1000)
val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWinDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailWinDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val orderJoinDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDstream.join(orderDetailWithKeyDstream)

    val orderDetailWideDstream: DStream[OrderDetailWide] = orderJoinDstream.map { case (id, (orderInfo, orderDetail)) => new OrderDetailWide(orderInfo, orderDetail) }


    //去重
    val orderDetailWideFilteredDstream: DStream[OrderDetailWide] = orderDetailWideDstream.transform { rdd =>
      rdd.cache()
      println("前：" + rdd.count())
      val logInfoRdd: RDD[OrderDetailWide] = rdd.mapPartitions { orderDetailWideItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val orderDetailWideFilteredList = new ListBuffer[OrderDetailWide]
        val orderDetailWideList: List[OrderDetailWide] = orderDetailWideItr.toList

//        val longs = orderDetailWideList.map(orderDetailWide => orderDetailWide.order_id)
//        println(longs.mkString(","))
        for (orderDetailWide <- orderDetailWideList) {

          val orderDetailWideKey = "order_detail_wide:"
          val ifFirst= jedis.sadd(orderDetailWideKey, orderDetailWide.order_detail_id.toString)
          if (ifFirst == 1L) {
            orderDetailWideFilteredList += orderDetailWide
          }
        }
        jedis.close()
        orderDetailWideFilteredList.toIterator
      }
      logInfoRdd.cache()
      println("后：" + logInfoRdd.count())

      OffsetManager.saveOffset(groupId,DW_ORDER_DETAIL_topic,orderDetailOffsetRanges)
      OffsetManager.saveOffset(groupId,DW_ORDER_INFO_topic,orderInfoOffsetRanges)

      logInfoRdd
    }
    orderDetailWideFilteredDstream.map(orderwide=>(orderwide.order_id,orderwide.final_total_amount,orderwide.original_total_amount,  orderwide.sku_price,orderwide.sku_num,orderwide.final_detail_amount)).print(1000)

    ssc.start()
    ssc.awaitTermination()




}
}
