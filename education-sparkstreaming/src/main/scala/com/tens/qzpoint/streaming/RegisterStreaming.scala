package com.tens.qzpoint.streaming

import java.sql.ResultSet

import com.tens.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RegisterStreaming {
  private val groupid = "register_group_test"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "tens")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext = ssc.sparkContext
    val topics = Array("register_topic")
    val kafkaMap = Map(
      "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    //sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中
    ssc.checkpoint("user/tens/sparkstreaming/checkpiont")
    //查询MySQL中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection.get
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val modle = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(modle, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数 判断本地是否有偏移量 有则根据偏移量继续消费 无则重新消费
    val stream = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    val resultDstream = stream.filter(_.value().split("\t").length == 3)
      .mapPartitions(partitions => {
        partitions.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val app_name = arr(1) match {
            case "1" => "pc"
            case "2" => "APP"
            case _ => "other"
          }
          (app_name, 1)
        })
      })
    resultDstream.cache()
    //"=================每6s间隔1分钟内的注册数据================="
    resultDstream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()

    //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    resultDstream.updateStateByKey(updateFunc).print()

    //处理完业务逻辑后 手动提交offset维护到本地MySQL中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection.get
      try {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }


}
