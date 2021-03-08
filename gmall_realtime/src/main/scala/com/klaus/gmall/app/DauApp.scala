package com.klaus.gmall.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.klaus.gmall.bean.DauInfo
import com.klaus.gmall.utils.{MyEsUtil, MyKafkaSink, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_DAU_CONSUMER"
    val topic = "GMALL_START"
    val startupInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)


    val startLogInfoDStream: DStream[JSONObject] = startupInputDstream.map { record =>
      val startupJson: String = record.value()
      val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
      val ts= startupJSONObj.getLong("ts")
      startupJSONObj
    }
//利用redis去重
    val dauLoginfoDstream: DStream[JSONObject] = startLogInfoDStream.transform { rdd =>
      println("前：" +  rdd.count())
      val logInfoRdd: RDD[JSONObject] = rdd.mapPartitions { startLogInfoItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val dauLogInfoList = new ListBuffer[JSONObject]
        val startLogList: List[JSONObject] = startLogInfoItr.toList

        for (startupJSONObj <- startLogList) {
          val ts= startupJSONObj.getLong("ts")
          val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
          val dauKey = "dau:" + dt
          val ifFirst = jedis.sadd(dauKey, startupJSONObj.getJSONObject("common").getString("mid"))
          if (ifFirst == 1L) {
            dauLogInfoList += startupJSONObj
          }
        }
        jedis.close()
        dauLogInfoList.toIterator
      }
      // println("后：" + logInfoRdd.count())
      logInfoRdd
    }
//调整数据结构
    val dauDstream: DStream[DauInfo] = dauLoginfoDstream.map { startupJsonObj =>
      val dtHr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startupJsonObj.getLong("ts")))
      val dtHrArr: Array[String] = dtHr.split(" ")
      val dt = dtHrArr(0)
      val timeArr = dtHrArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)
      val commonJSONObj: JSONObject = startupJsonObj.getJSONObject("common")
      DauInfo(commonJSONObj.getString("mid"), commonJSONObj.getString("uid"), commonJSONObj.getString("mid"), commonJSONObj.getString("ch")
        , commonJSONObj.getString("vc"), dt, hr, mi, startupJsonObj.getLong("ts"))
    }
//写入ES
    dauDstream.foreachRDD{rdd=>
      rdd.foreachPartition{dauInfoItr=>
        val dauInfoWithIdList: List[(String, DauInfo)] = dauInfoItr.toList.map(dauInfo=>(dauInfo.dt+  "_"+dauInfo.mid,dauInfo))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        MyEsUtil.bulkDoc(dauInfoWithIdList,"gmall_dau_info_"+dateStr)
      }

    }


    ssc.start()
    ssc.awaitTermination()

  }
}
