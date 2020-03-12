package com.tens.qzpoint.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val ssc = new SparkContext(conf)
    ssc.textFile("/user/tens/kafka/register.log", 10)
      .foreachPartition(partition => {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("register_topic", item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
