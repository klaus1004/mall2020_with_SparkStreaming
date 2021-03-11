package com.klaus.gmall.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.klaus.gmall.bean.{OrderInfo, ProvinceInfo, UserState}
import com.klaus.gmall.utils.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

//首单分析
object OrderInfoApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("orderinfo_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_ORDER_INFO_CONSUMER"
    val topic = "ODS_T_ORDER_INFO"

    //从redis读取偏移量
    val orderOffsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    //根据偏移起始点获得数据
    //判断如果之前没有在redis保存，则从kafka最新加载数据
    var orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderOffsets != null && orderOffsets.size > 0) {
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, orderOffsets, groupId)
    } else {
      println("offset is null")
      orderInfoInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获得偏移结束点
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoInputDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      orderInfo
    }
    //利用hbase判断该用户是否首单并标记
    val flagedStream = orderInfoDstream.mapPartitions(orderInfoItr => {
      val list = orderInfoItr.toList
      if (list.length > 0) {
        val idStrings = list.map("'" + _.user_id + "'").mkString(",")
        val jSONObjectsList = PhoenixUtil.queryList("select USER_ID,IF_CONSUMED from USER_STATE2020 where USER_ID in (" + idStrings + ")")
        val idMap:Map[Long,String] = jSONObjectsList.map(JSONObject =>
          (JSONObject.getLong("USER_ID").toLong, JSONObject.getString("IF_CONSUMED"))).toMap
        for (orderinfo <- list) {
          val value = idMap.getOrElse(orderinfo.user_id, "0")
          if (value == "1") {
            orderinfo.if_first_order = "0"
          } else if(value == "0"){
            orderinfo.if_first_order = "1"
          }
        }
      }
      list.toIterator
    }
    )
    //修正，同批次内相同用户id会存在多个首单
    val groupByUserIdStream = flagedStream.map(orderinfo => (orderinfo.user_id, orderinfo)).groupByKey()
    val orderInfoFinalDstream = groupByUserIdStream.flatMap { tuple => {
      val list = tuple._2.toList
      if (list.length > 1) {
        val sortedList = list.sortWith((orderinfo, orderinfo2) => orderinfo.create_time < orderinfo2.create_time)
        if (sortedList(0).if_first_order == "1") {
          for (i <- 1 to sortedList.length - 1) {
            sortedList(i).if_first_order = "0"
          }
        }
        sortedList
      }else{list}
    }
    }
//    orderInfoFinalDstream.cache()
    //    flagedStream.foreachRDD(rdd=>{
    //      val filterRdd = rdd.filter(_.if_first_order == "0").map(orderinfo=>(orderinfo.user_id.toString,orderinfo.if_first_order))
    //      filterRdd.saveToPhoenix("USER_STATE2020",Seq[String]("USER_ID","IF_CONSUMED"),new Configuration(),Some[String]("hadoop102,hadoop103,hadoop104:2181"))
    //    })
//    val userStateDstream: DStream[UserState] = orderInfoFinalDstream.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.user_id.toString, orderInfo.if_first_order))
//    userStateDstream.foreachRDD { rdd =>
//      rdd.saveToPhoenix("USER_STATE2020", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))
//    }

//    orderInfoFinalDstream.print(1000)
    ////////////////////////////////////////////////////////////
    //////////////////////合并维表代码//////////////////////
    ////////////////////////////////////////////////////////////
    val orderInfoFinalWithProvinceDstream: DStream[OrderInfo] = orderInfoFinalDstream.transform { rdd =>
      //transform中算子之外的代码在driver端执行,按批次周期性执行
      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select ID,NAME,REGION_ID ,AREA_CODE from GMALL2020_PROVINCE_INFO")

      val provinceMap: Map[Long, ProvinceInfo] = provinceJsonObjList.map { jsonObj => (jsonObj.getLong("ID").toLong, jsonObj.toJavaObject(classOf[ProvinceInfo])) }.toMap
      //使用广播变量将map数据从driver分发到各个分区（广播变量是executer级别，每个executer分发一次）
      val provinceMapBC: Broadcast[Map[Long, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)

      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMap: Map[Long, ProvinceInfo] = provinceMapBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id, null)
        if (provinceInfo != null) {
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
        }
        orderInfo
      }
      orderInfoWithProvinceRDD
    }

    ////////////////////////////////////////////////////////////
    //////////////////////最终存储代码//////////////////////
    ////////////////////////////////////////////////////////////
    orderInfoFinalWithProvinceDstream.foreachRDD{rdd=>
      rdd.cache()
      //存储用户状态
      val userStateRdd: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.id.toString, orderInfo.if_first_order))
      userStateRdd.saveToPhoenix("USER_STATE2020", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("hadoop102,hadoop103,hadoop104:2181"))

      rdd.foreachPartition { orderInfoItr =>

        val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkDoc(orderInfoList, "gmall2020_order_info_" + dateStr)

        for ((id,orderInfo) <- orderInfoList ) {
          MyKafkaSink.send("DW_ORDER_INFO",id,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
        }

      }
      OffsetManager.saveOffset(groupId, topic, orderInfoOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()

  }}
