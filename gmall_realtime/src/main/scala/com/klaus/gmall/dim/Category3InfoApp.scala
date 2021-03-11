import com.alibaba.fastjson.JSON
import com.klaus.gmall.bean.{Category3Info, ProvinceInfo}
import com.klaus.gmall.utils.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object Category3InfoApp {
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("category_info_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "gmall_category_group"
    val topic = "ODS_T_BASE_CATEGORY3"
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

    val Category3InfoDstrea:DStream[Category3Info] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val category3Info: Category3Info = JSON.parseObject(jsonString, classOf[Category3Info])
      category3Info
    }

    Category3InfoDstrea.cache()

    Category3InfoDstrea.print(1000)

    Category3InfoDstrea.foreachRDD { rdd =>
      rdd.saveToPhoenix("gmall2020_Category3_info", Seq("ID", "NAME"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))

      OffsetManager.saveOffset(groupId, topic, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
