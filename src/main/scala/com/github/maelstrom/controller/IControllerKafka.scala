package com.github.maelstrom.controller

import com.github.maelstrom.consumer.IKafkaConsumerPoolFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait IControllerKafka[K, V] {
  def close()
  def getLag: Long
  def commit()
  def getRDD(sc: SparkContext, consumerPoolFactory: IKafkaConsumerPoolFactory[_, _], maxQueue: Int): RDD[(K, V)]
}
