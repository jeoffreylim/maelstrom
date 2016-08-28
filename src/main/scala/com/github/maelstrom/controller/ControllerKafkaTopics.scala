package com.github.maelstrom.controller

import java.util.concurrent.Executors

import com.github.maelstrom.KafkaRDDUtils
import com.github.maelstrom.consumer.KafkaConsumerPoolFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class ControllerKafkaTopics[K, V] (sc: SparkContext,
                                   curator: CuratorFramework,
                                   poolFactory: KafkaConsumerPoolFactory[_,_]) extends IControllerKafka[K, V] with Logging {
  final private val topicSet = mutable.Set[ControllerKafkaTopic[K, V]]()
  private lazy val executor = Executors.newFixedThreadPool(getTotalTopics)
  private implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  final def registerTopic(consumerGroup: String, topic: String): ControllerKafkaTopic[K, V] = {
    registerTopic(consumerGroup, topic, 5000)
  }

  final def registerTopic(consumerGroup: String, topic: String, maxQueue: Int = 5000): ControllerKafkaTopic[K, V] = {
    val controllerKafkaTopic = new ControllerKafkaTopic[K, V](sc, curator, poolFactory, consumerGroup, topic, maxQueue)
    if (topicSet.contains(controllerKafkaTopic)) throw new IllegalArgumentException("Already registered this kind of topic")
    topicSet.add(controllerKafkaTopic)
    controllerKafkaTopic
  }

  final def getTotalTopics: Int = {
    topicSet.size
  }

  final def getTotalPartitions: Int = {
    var totalPartitions: Int = 0

    for (kafkaTopic <- topicSet)
      totalPartitions += kafkaTopic.getPartitionCount

    totalPartitions
  }

  final def close() {
    for (kafkaTopic <- topicSet) kafkaTopic.close()
    executor.shutdown()
  }

  final def getLag: Long = {
    var lag: Long = 0L
    for (kafkaTopic <- topicSet) lag += kafkaTopic.getLag
    lag
  }

  final def commit() {
    for (kafkaTopic <- topicSet) kafkaTopic.commit()
  }

  final def getAllRDDs: List[RDD[(K, V)]] = {
    val lag: Long = getLag
    var futures = mutable.ListBuffer[Future[RDD[(K,V)]]]()

    for (controllerKafkaTopic <- topicSet) {
      if (controllerKafkaTopic.getLag > 0) {
        var offsets: Map[Int, (Long, Long)] = Map()
        val perEach: Long = Math.max(1, controllerKafkaTopic.maxQueue / controllerKafkaTopic.getPartitionCount)

        for (kafkaPartition <- controllerKafkaTopic.getPartitions) {
          if (lag > perEach) {
            kafkaPartition.setStopAtOffset(Math.min(kafkaPartition.getLastOffset + perEach, kafkaPartition.getLastOffset + kafkaPartition.getLag))
            logDebug(s"REAP PARTIAL: [${kafkaPartition.topic}]-[${kafkaPartition.partitionId}] : @${kafkaPartition.getStopAtOffset}")
          } else {
            kafkaPartition.setStopAtOffset(kafkaPartition.getLatestOffset)
            logDebug(s"REAP FULL: [${kafkaPartition.topic}]-[${kafkaPartition.partitionId}] : @${kafkaPartition.getLatestOffset}")
          }
          logDebug(s"OFFSET RANGE: [${kafkaPartition.topic}]-[${kafkaPartition.partitionId}] : ${kafkaPartition.getLastOffset}->${kafkaPartition.getStopAtOffset} = ${kafkaPartition.getStopAtOffset - kafkaPartition.getLastOffset}")
          offsets += (kafkaPartition.partitionId -> (kafkaPartition.getLastOffset, kafkaPartition.getStopAtOffset))
        }

        futures += Future {
          logDebug("LOADING RDD - " + controllerKafkaTopic.consumerGroup + ":" + controllerKafkaTopic.topic)
          KafkaRDDUtils.createKafkaRDD(sc, poolFactory, controllerKafkaTopic.topic, offsets)
            .persist(StorageLevel.MEMORY_ONLY)
            .setName("KafkaRDD-" + controllerKafkaTopic.consumerGroup + ":" + controllerKafkaTopic.topic)
            .asInstanceOf[RDD[(K, V)]]
        }
      }
    }

    Await.result(Future.sequence(futures), Duration.Inf)

    var rDDs = mutable.ListBuffer[RDD[(K, V)]]()

    futures.foreach(f =>
      f.value.get match {
        case Success(rdd) =>
          rDDs += rdd
        case Failure (error) =>
          throw error
      }
    )

    rDDs.toList
  }

  final def getRDD(): RDD[(K, V)] = {
    getAllRDDs.reduce(_ union _)
  }
}
