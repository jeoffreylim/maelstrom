package com.github.maelstrom.controller

import com.github.maelstrom.KafkaRDDUtils
import com.github.maelstrom.consumer.{IKafkaConsumerPoolFactory, KafkaMetaData}
import kafka.cluster.Broker
import kafka.javaapi.TopicMetadata
import kafka.serializer.Decoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}


class ControllerKafkaTopic[K, V] (curator: CuratorFramework,
                                  brokerList: java.util.List[Broker],
                                  val consumerGroup: String,
                                  val topic: String,
                                  keyDecoder: Decoder[K],
                                  valueDecoder: Decoder[V]) extends IControllerKafka[K, V] with Logging {
  val partitionMap = scala.collection.mutable.Map[Integer, ControllerKafkaPartition[K, V]]()
  val metaData: java.util.Map[String, TopicMetadata] = KafkaMetaData.getTopicMetaData(brokerList, java.util.Collections.singletonList(topic))

  if (metaData.containsKey(topic)) {
    val topicMetadata: TopicMetadata = metaData.get(topic)
    val partionCount: Int = topicMetadata.partitionsMetadata.size

    for(i <- 0 until partionCount)
      partitionMap.put(i, new ControllerKafkaPartition[K, V](curator, brokerList, consumerGroup, topic, i, keyDecoder, valueDecoder))

  } else {
    throw new IllegalArgumentException("Topic " + topic + " not found")
  }

  override def hashCode: Int = {
    java.util.Objects.hash(this.consumerGroup, this.topic)
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

  final def getRDD(sc: SparkContext, consumerPoolFactory: IKafkaConsumerPoolFactory[_, _], maxQueue: Int): RDD[(K, V)] = {
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

    KafkaRDDUtils.createKafkaRDD(sc, consumerPoolFactory, topic, offsets)
      .persist(StorageLevel.MEMORY_ONLY)
      .setName("KafkaRDD-" + consumerGroup + ":" + topic)
      .asInstanceOf[RDD[(K, V)]]
  }
}
