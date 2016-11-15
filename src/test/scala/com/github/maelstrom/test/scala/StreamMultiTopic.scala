package com.github.maelstrom.test.scala

import com.github.maelstrom.StreamProcessor
import com.github.maelstrom.consumer.{KafkaConsumerPoolFactory, OffsetManager}
import com.github.maelstrom.controller.ControllerKafkaTopics
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object StreamMultiTopic {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamMultiTopic")
    val sc = new SparkContext(sparkConf)
    val curator = OffsetManager.createCurator("127.0.0.1:2181")
    val poolFactory = new KafkaConsumerPoolFactory[String, String]("127.0.0.1:9092", classOf[StringDecoder], classOf[StringDecoder])
    val topics = new ControllerKafkaTopics[String, String](sc, curator, poolFactory)
    topics.registerTopic("test_multi", "test")
    topics.registerTopic("test_multi", "test2")

    new StreamProcessor[String, String](topics) {
      final def process() {
        val rdd: RDD[(String, String)] = fetch()

        rdd.foreachPartition { partitionData =>
          partitionData.foreach { record =>
            logger.info(s"key=${record._1} val=]${record._2}")
          }
        }

        commit()
      }
    }.run()

    sc.stop()
  }
}
