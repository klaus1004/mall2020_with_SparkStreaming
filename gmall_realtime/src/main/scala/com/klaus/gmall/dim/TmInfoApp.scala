package com.klaus.gmall.dim

import com.alibaba.fastjson.JSON
import com.klaus.gmall.bean.{SpuInfo, TmInfo}
import com.klaus.gmall.utils.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._

object TmInfoApp {
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("tm_info_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "gmall_tm_group"
    val topic = "ODS_T_BASE_TRADEMARK"
    val offsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsets, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获得偏移结束点
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val provinceInfoDstream: DStream[TmInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val tmInfo: TmInfo = JSON.parseObject(jsonString, classOf[TmInfo])
      tmInfo
    }

    provinceInfoDstream.cache()

    provinceInfoDstream.print(1000)

    provinceInfoDstream.foreachRDD { rdd =>
      rdd.saveToPhoenix("gmall2020_tm_info", Seq("TM_ID","TM_NAME"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(groupId, topic, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
