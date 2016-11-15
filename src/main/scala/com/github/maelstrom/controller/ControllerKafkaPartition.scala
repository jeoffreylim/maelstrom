package com.github.maelstrom.controller

import com.github.maelstrom.{KafkaRDDUtils, Logging}
import com.github.maelstrom.consumer.{KafkaConsumer, KafkaConsumerPoolFactory, OffsetManager}
import kafka.serializer.Decoder
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext

class ControllerKafkaPartition[K, V](sc: SparkContext,
                                     curator: CuratorFramework,
                                     poolFactory: KafkaConsumerPoolFactory[_,_],
                                     val consumerGroup: String,
                                     val topic: String,
                                     val partitionId: Int,
                                     val maxQueue: Int = 5000) extends IControllerKafka[K, V] with Logging {
  final private val consumer: KafkaConsumer[K, V] = new KafkaConsumer[K, V](poolFactory.getBrokerList,
    consumerGroup, poolFactory.createKeyDecoder().asInstanceOf[Decoder[K]],
    poolFactory.createValueDecoder().asInstanceOf[Decoder[V]], topic, partitionId)
  final private val offsetManager: OffsetManager = new OffsetManager(curator, consumer, consumerGroup, topic, partitionId)
  private var stopAtOffset: Long = -1

  final def close() {
    consumer.close()
  }

  final def getLag: Long = {
    getLatestOffset - getLastOffset
  }

  final def getLastOffset: Long = {
    offsetManager.getLastOffset
  }

  final def getLatestOffset: Long = {
    consumer.getLatestOffset
  }

  final def getStopAtOffset: Long = {
    stopAtOffset
  }

  final def setStopAtOffset(stopAtOffset: Long) {
    this.stopAtOffset = stopAtOffset
  }

  final def commit() {
    if (stopAtOffset <= 0) return
    offsetManager.setLastOffset(stopAtOffset)
    offsetManager.storeLastOffset()
    stopAtOffset = -1
  }

  final def getRDD(): RDD[(K, V)] = {
    val lag: Long = getLag
    val perEach: Long = Math.max(1, maxQueue)
    var offsets: Map[Int, (Long, Long)] = Map()

    if (lag > maxQueue) {
      setStopAtOffset(Math.min(getLastOffset + perEach, getLastOffset + getLag))
      logDebug(s"REAP PARTIAL: [$topic]-[$partitionId] : @$getStopAtOffset")
    } else {
      setStopAtOffset(getLatestOffset)
      logDebug(s"REAP FULL: [$topic]-[$partitionId] : @$getLatestOffset")
    }

    logDebug(s"OFFSET RANGE: [$topic]-[$partitionId] : $getLastOffset->$getStopAtOffset = ${getStopAtOffset - getLastOffset}")
    offsets += (partitionId -> (getLastOffset, getStopAtOffset))

    KafkaRDDUtils.createKafkaRDD(sc, poolFactory, topic, offsets)
      .persist(StorageLevel.MEMORY_ONLY)
      .setName("KafkaRDD-" + consumerGroup + ":" + topic + ":" + partitionId)
      .asInstanceOf[RDD[(K, V)]]
  }
}
