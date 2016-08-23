package com.github.maelstrom.controller

import java.util.concurrent.Executors

import com.github.maelstrom.KafkaRDDUtils
import com.github.maelstrom.consumer.IKafkaConsumerPoolFactory
import kafka.cluster.Broker
import kafka.serializer.Decoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class ControllerKafkaTopics[K, V] (curator: CuratorFramework,
                                   brokerList: java.util.List[Broker],
                                   keyDecoder: Decoder[K],
                                   valueDecoder: Decoder[V]) extends IControllerKafka[K, V] with Logging {
  final private val topicSet = scala.collection.mutable.Set[ControllerKafkaTopic[K, V]]()
  private lazy val executor = Executors.newFixedThreadPool(getTotalTopics)
  private implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  final def registerTopic(consumerGroup: String, topic: String): ControllerKafkaTopic[K, V] = {
    val controllerKafkaTopic: ControllerKafkaTopic[K, V] = new ControllerKafkaTopic[K, V](curator, brokerList, consumerGroup, topic, keyDecoder, valueDecoder)
    if (topicSet.contains(controllerKafkaTopic)) throw new IllegalArgumentException("Already registered this kind of topic")
    topicSet.add(controllerKafkaTopic)
    controllerKafkaTopic
  }

  final def getTotalTopics: Int = {
    topicSet.size
  }

  final def getTotalReceivers: Int = {
    var totalReceivers: Int = 0
    for (kafkaTopic <- topicSet)
      totalReceivers += kafkaTopic.getPartitionCount
    totalReceivers
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

  final def getAllRDDs(sc: SparkContext, consumerPoolFactory: IKafkaConsumerPoolFactory[_, _], maxQueue: Int): List[RDD[(K, V)]] = {
    val lag: Long = getLag
    val perEach: Long = Math.max(1, maxQueue / getTotalReceivers)

    logDebug(s"maxQueue=$maxQueue receivers=$getTotalReceivers perEach=$perEach")

    val futures = ListBuffer[Future[RDD[(K,V)]]]()

    for (controllerKafkaTopic <- topicSet) {
      if (controllerKafkaTopic.getLag > 0) {
        var offsets: Map[Int, (Long, Long)] = Map()

        for (kafkaPartition <- controllerKafkaTopic.getPartitions) {
          if (lag > maxQueue) {
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
          KafkaRDDUtils.createKafkaRDD(sc, consumerPoolFactory, controllerKafkaTopic.topic, offsets)
            .persist(StorageLevel.MEMORY_ONLY)
            .setName("KafkaRDD-" + controllerKafkaTopic.consumerGroup + ":" + controllerKafkaTopic.topic)
            .asInstanceOf[RDD[(K, V)]]
        }
      }
    }

    Await.result(Future.sequence(futures), Duration.Inf)

    val rDDs = ListBuffer[RDD[(K, V)]]()

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

  final def getRDD(sc: SparkContext, consumerPoolFactory: IKafkaConsumerPoolFactory[_, _], maxQueue: Int): RDD[(K, V)] = {
    getAllRDDs(sc, consumerPoolFactory, maxQueue).reduce(_ union _)
  }
}
