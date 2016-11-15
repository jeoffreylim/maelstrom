package com.github.maelstrom.test.scala

import com.github.maelstrom.consumer.{KafkaConsumerPoolFactory, OffsetManager}
import com.github.maelstrom.controller.ControllerKafkaTopics
import com.github.maelstrom.{ProcessorRunner, StreamProcessor}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object StreamMultiTopicMultiProc {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamMultiTopicMultiProc")
    val sc = new SparkContext(sparkConf)
    val curator = OffsetManager.createCurator("127.0.0.1:2181")
    val poolFactory = new KafkaConsumerPoolFactory[String, String]("127.0.0.1:9092", classOf[StringDecoder], classOf[StringDecoder])
    val topics = new ControllerKafkaTopics[String, String](sc, curator, poolFactory)

    new ProcessorRunner().addProcessor(new StreamProcessor[String, String](topics.registerTopic("test_multi_proc", "test", 1000)) {
      final def process() {
        val rdd: RDD[(String, String)] = fetch()

        rdd.foreachPartition { partitionData =>
          partitionData.foreach { record =>
            logger.info(s"key=${record._1} val=${record._2}")
          }
        }

        commit()
      }
    }).addProcessor(new StreamProcessor[String, String](topics.registerTopic("test_multi_proc", "test2", 500)) {
      final def process() {
        val rdd: RDD[(String, String)] = fetch()

        rdd.foreachPartition { partitionData =>
          partitionData.foreach { record =>
            logger.info(s"key=${record._1} val=${record._2}")
          }
        }

        commit()
      }
    }).start()

    sc.stop()
  }
}
