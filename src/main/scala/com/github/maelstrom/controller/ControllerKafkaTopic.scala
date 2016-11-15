package com.github.maelstrom.controller

import com.github.maelstrom.consumer.{KafkaConsumerPoolFactory, KafkaMetaData}
import com.github.maelstrom.{KafkaRDDUtils, Logging}
import kafka.javaapi.TopicMetadata
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

class ControllerKafkaTopic[K, V] (sc: SparkContext,
                                  curator: CuratorFramework,
                                  poolFactory: KafkaConsumerPoolFactory[_,_],
                                  val consumerGroup: String,
                                  val topic: String,
                                  val maxQueue: Int = 5000) extends IControllerKafka[K, V] with Logging {
  val partitionMap = mutable.Map[Integer, ControllerKafkaPartition[K, V]]()
  val metaData: java.util.Map[String, TopicMetadata] = KafkaMetaData.getTopicMetaData(poolFactory.getBrokerList, java.util.Collections.singletonList(topic))

  if (metaData.containsKey(topic)) {
    val topicMetadata: TopicMetadata = metaData.get(topic)
    val partionCount: Int = topicMetadata.partitionsMetadata.size

    for(i <- 0 until partionCount)
      partitionMap.put(i, new ControllerKafkaPartition[K, V](sc, curator, poolFactory, consumerGroup, topic, i))

  } else {
    throw new IllegalArgumentException("Topic " + topic + " not found")
  }

  override def hashCode: Int = {
    java.util.Objects.hash(consumerGroup, topic)
  }

  override def equals(other: Any): Boolean = {
    !(other == null || !other.isInstanceOf[ControllerKafkaTopic[_, _]]) && other.asInstanceOf[ControllerKafkaTopic[_, _]].topic == topic && other.asInstanceOf[ControllerKafkaTopic[_, _]].consumerGroup == consumerGroup
  }

  final def getPartitionCount: Int = {
    partitionMap.size
  }

  final def close() {
    for (kafkaPartition <- partitionMap.values) kafkaPartition.close()
  }

  final def getLag: Long = {
    var lag: Long = 0L
    for (kafkaPartition <- partitionMap.values) lag += kafkaPartition.getLatestOffset - kafkaPartition.getLastOffset
    lag
  }

  final def commit() {
    for (kafkaPartition <- partitionMap.values) kafkaPartition.commit()
  }

  def getPartitions: Iterable[ControllerKafkaPartition[K, V]] = {
    partitionMap.values
  }

  final def getRDD(): RDD[(K, V)] = {
    val lag: Long = getLag
    val perEach: Long = Math.max(1, maxQueue / getPartitionCount)
    var offsets: Map[Int, (Long, Long)] = Map()

    for (kafkaPartition <- getPartitions) {
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

    KafkaRDDUtils.createKafkaRDD(sc, poolFactory, topic, offsets)
      .persist(StorageLevel.MEMORY_ONLY)
      .setName("KafkaRDD-" + consumerGroup + ":" + topic)
      .asInstanceOf[RDD[(K, V)]]
  }
}
