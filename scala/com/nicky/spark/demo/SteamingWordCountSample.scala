package com.nicky.spark.demo

import kafka.serializer._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

object SteamingWordCountSample extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
  val ssc = new StreamingContext(conf, Seconds(5))

  val topics = Set("my-topic")
  //val topics = Map("my-topic" -> 2)
  val bootstrapServers = "localhost:9092" // kafka
  //val bootstrapServers = "localhost:2181" // zookeeper
  val comsumerGroup = "spark-streaming-consumer-group"

  val consumerConfig = Map[String, String](
    "bootstrap.servers" -> bootstrapServers,
    "group.id" -> comsumerGroup)

  //val kafkaStream = KafkaUtils.createStream(ssc, bootstrapServers, comsumerGroup, topics, MEMORY_ONLY)
  val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, consumerConfig, topics)
  //kafkaStream.flatMap(x => x._2.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()
  /*kafkaStream.flatMap(x => x._2.split(" ")).print()*/ //.map(x => (x, 1)).reduceByKey(_ + _).print()
  ssc.checkpoint("checkpoint")

  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
  val wordcounts = lines.flatMap(x => x._2.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).updateStateByKey(updateFunc) //.saveAsTextFiles("C:\\Users\\35108\\output\\")
  //println(wordcounts)
  wordcounts.foreachRDD(x => x.foreach(println))
  //wordcounts.pprint()

  ssc.start()
  ssc.awaitTermination()

}

