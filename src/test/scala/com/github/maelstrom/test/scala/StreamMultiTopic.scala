package com.github.maelstrom.test.scala

import com.github.maelstrom.StreamProcessor
import com.github.maelstrom.consumer.OffsetManager
import com.github.maelstrom.controller.ControllerKafkaTopics
import com.github.maelstrom.test.java.LocalKafkaConsumerPoolFactory
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object StreamMultiTopic extends LazyLogging {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamMultiTopic")
    val sc = new SparkContext(sparkConf)
    val curator = OffsetManager.createCurator("127.0.0.1:2181")
    val pool = new LocalKafkaConsumerPoolFactory
    val brokerList = pool.getKafkaConsumerPool.getBrokerList
    val topics = new ControllerKafkaTopics[String, String](curator, brokerList, new StringDecoder(), new StringDecoder())
    topics.registerTopic("test_multi", "test")
    topics.registerTopic("test_multi", "test2")

    new StreamProcessor[String, String](sc, pool, topics) {
      final def process() {
        val rdd: RDD[(String, String)] = fetch()

        rdd.foreachPartition { partitionData =>
          partitionData.foreach { record =>
            logger.info("key=" + record._1 + " val=" + record._2)
          }
        }

        commit()
      }
    }.run()

    sc.stop()
  }
}
