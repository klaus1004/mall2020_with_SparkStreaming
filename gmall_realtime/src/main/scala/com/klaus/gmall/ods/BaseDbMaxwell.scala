package com.klaus.gmall.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.klaus.gmall.utils.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json

object BaseDbMaxwell {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("canal_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_DB_GMALL2020_M"
    val groupId = "gmall_base_db_maxwell_group"
    //读取redis中的偏移量
    val offsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    //加载数据流
    var dbInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets == null) {
      println("no offsets")
      dbInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    } else {
      println("offset:" + offsets.mkString(","))
      dbInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsets, groupId)
    }
    //读取偏移量移动位置
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val recordDstream: DStream[ConsumerRecord[String, String]] = dbInputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val jsonDstream: DStream[JSONObject] = recordDstream.map { record =>
      val jsonString: String = record.value()
      val jSONObject: JSONObject = JSON.parseObject(jsonString)
      jSONObject
    }

    //写入数据
    jsonDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonItr =>
        if (offsetRanges != null && offsetRanges.size > 0) {
          val offsetRange: OffsetRange = offsetRanges(TaskContext.get().partitionId())
          println("from:" + offsetRange.fromOffset + " --- to:" + offsetRange.untilOffset)
        }
        for(json <- jsonItr) {
          if (!json.getString("type").equals("bootstrap-start") && !json.getString("type").equals("bootstrap-complete")) {
            val tableName: String = json.getString("table")
            val jsonObj: JSONObject = json.getJSONObject("data")
            val topic: String = "ODS_T_" + tableName.toUpperCase
            val key: String = tableName + "_" + jsonObj.getString("id")
            MyKafkaSink.send(topic, key, jsonObj.toJSONString)
          }

        }
      }
      // 偏移量移动位置写入redis
      OffsetManager.saveOffset(groupId, topic, offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()

  }
}