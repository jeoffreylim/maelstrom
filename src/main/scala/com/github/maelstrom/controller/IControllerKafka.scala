package com.github.maelstrom.controller

import org.apache.spark.rdd.RDD

trait IControllerKafka[K, V] {
  def close()
  def getLag: Long
  def commit()
  def getRDD: RDD[(K, V)]
}
