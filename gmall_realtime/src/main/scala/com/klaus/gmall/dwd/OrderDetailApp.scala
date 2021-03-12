package com.klaus.gmall.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.klaus.gmall.bean.{Category3Info, OrderDetail, OrderInfo, SkuInfo, SpuInfo, TmInfo}
import com.klaus.gmall.utils.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}



object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("orderdetail_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_ORDER_DETAIL_CONSUMER"
    val topic = "ODS_T_ORDER_DETAIL"

    //从redis读取偏移量
    val orderOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    //根据偏移起始点获得数据
    //判断如果之前没有在redis保存，则从kafka最新加载数据
    var orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderOffsets != null && orderOffsets.size > 0) {
      orderDetailInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, orderOffsets, groupId)
    } else {
      println("offset is null")
      orderDetailInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获得偏移结束点
    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailInputDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
//封装为样例类流
    val orderDetailDstream: DStream[OrderDetail] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }
    //关联维表
    val wideStream = orderDetailDstream.transform(rdd => {
      val categoryJSONObjects = PhoenixUtil.queryList("select * from gmall2020_Category3_info")
      val spuJSONObjects = PhoenixUtil.queryList("select * from gmall2020_spu_info")
      val tmJSONObjects = PhoenixUtil.queryList("select * from gmall2020_tm_info")
      val skuJSONObjects = PhoenixUtil.queryList("select * from gmall2020_sku_info")

      val categorymap = categoryJSONObjects.map(json => (json.getString("ID"), JSON.toJavaObject(json, classOf[Category3Info]))).toMap
      val spumap = spuJSONObjects.map(json => (json.getString("ID"), JSON.toJavaObject(json, classOf[SpuInfo]))).toMap
      val tmmap = tmJSONObjects.map(json => (json.getString("TM_ID"), JSON.toJavaObject(json, classOf[TmInfo]))).toMap
      val skumap = skuJSONObjects.map(json => (json.getString("ID"), JSON.toJavaObject(json, classOf[SkuInfo]))).toMap

      val categroyMapBC = ssc.sparkContext.broadcast(categorymap)
      val spuMapBC = ssc.sparkContext.broadcast(spumap)
      val tmMapBC = ssc.sparkContext.broadcast(tmmap)
      val skuMapBC = ssc.sparkContext.broadcast(skumap)

      val wideRDD = rdd.map(orderDetail => {
        val categroyMap = categroyMapBC.value
        val spuMap = spuMapBC.value
        val tmMap = tmMapBC.value
        val skuMap = skuMapBC.value

        var spu_id = ""
        var tm_id = ""
        var category3_id = ""
        var spu_name = ""
        var tm_name = ""
        var category3_name = ""

        if (categroyMap.size > 0 && spuMap.size > 0 && tmMap.size > 0 && skuMap.size > 0) {

          val skuinfo = skuMap.getOrElse(orderDetail.sku_id.toString, null)
          if (skuinfo != null) {
            spu_id = skuinfo.spu_id
            category3_id = skuinfo.category3_id
            tm_id = skuinfo.tm_id
          }
          val category3Info = categroyMap.getOrElse(category3_id, null)
          if (category3Info != null) {
            category3_name = category3Info.name
          }
          val tmInfo = tmMap.getOrElse(tm_id, null)
          if (tmInfo != null) {
            tm_name = tmInfo.tm_name
          }
          val spuInfo = spuMap.getOrElse(spu_id, null)
          if (spuInfo != null) {
            spu_name = spuInfo.spu_name
          }
          orderDetail.spu_id = spu_id.toLong
          orderDetail.tm_id = tm_id.toLong
          orderDetail.category3_id = category3_id.toLong
          orderDetail.spu_name = spu_name
          orderDetail.tm_name = tm_name
          orderDetail.category3_name = category3_name

          orderDetail
        } else {
          orderDetail
        }
      })
      wideRDD
    })

    wideStream.cache()

    wideStream.print(1000)

    wideStream.foreachRDD(Rdd=>{
      Rdd.foreach(orderdetail=>{
        MyKafkaSink.send("DW_ORDER_DETAIL",orderdetail.order_id.toString,JSON.toJSONString(orderdetail,new SerializeConfig(true)))
      })
      OffsetManager.saveOffset(groupId,topic,orderDetailOffsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}