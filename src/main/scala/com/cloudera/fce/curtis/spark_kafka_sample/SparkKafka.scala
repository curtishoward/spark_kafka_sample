package com.cloudera.fce.curtis.spark_kafka_sample

import java.time.Instant

import org.apache.spark.sql.SparkSession  
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._  
import org.apache.spark.streaming._

// Usage:  SparkKafka [kafka-brokers] 

object SparkKafka {
  def main(args: Array[String]) {
    val kafkaBrokers  = args(0)

    val sparkConf   = new SparkConf().setAppName("SparkKafka")
    val spark       = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc         = new StreamingContext(spark.sparkContext, Seconds(5))

    val topicsSet       = Set(args(1)) 
    val kafkaParams     = Map[String, String]("bootstrap.servers"  -> kafkaBrokers,
                                              "group.id"           -> "spark_kafka_sample_group_id",
                                              "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
                                              "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer") 

    val dstream         = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, 
                                     ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    dstream.foreachRDD { rdd =>
       import spark.implicits._
       val dataFrame = rdd.map(rec => (Instant.now.getEpochSecond, rec.value)).toDF("timestamp", "message_string")
       dataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
