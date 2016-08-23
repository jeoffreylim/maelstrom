/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.maelstrom

import java.lang.{Integer => JInt, Long => JLong}
import java.util.{Map => JMap}

import com.github.maelstrom.consumer.{IKafkaConsumerPoolFactory, KafkaConsumerPool}
import kafka.message.MessageAndMetadata
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.Iterator
import scala.collection.JavaConversions._
import scala.reflect.ClassTag


/**
* Apache Spark + Kafka Integration
*
* @author Jeoffrey Lim
* @version 0.1
*/

object KafkaRDDUtils {
  def createJavaKafkaRDD[K:ClassTag, V:ClassTag](sc: SparkContext, poolFactory: IKafkaConsumerPoolFactory[K,V], topic: String, joffsets: JMap[JInt, (JLong, JLong)]): JavaRDD[(K,V)] = {
    createKafkaRDDJavaParams(sc, poolFactory, topic, joffsets).toJavaRDD()
  }

  def createKafkaRDDJavaParams[K:ClassTag, V:ClassTag](sc: SparkContext, poolFactory: IKafkaConsumerPoolFactory[K,V], topic: String, joffsets: JMap[JInt, (JLong, JLong)]): RDD[(K,V)] = {
    var offsets: Map[Int, (Long, Long)] = Map()
    joffsets.foreach(kv =>
      offsets += (kv._1.toInt ->(kv._2._1.toLong, kv._2._2.toLong))
    )
    new KafkaRDD(sc, poolFactory, topic, offsets)
  }

  def createKafkaRDD[K:ClassTag, V:ClassTag](sc: SparkContext, poolFactory: IKafkaConsumerPoolFactory[K,V], topic: String, offsets: Map[Int, (Long,Long)]): RDD[(K,V)] = {
    new KafkaRDD[K,V](sc, poolFactory, topic, offsets)
  }
}

class KafkaRDD[K:ClassTag, V:ClassTag](sc: SparkContext, val poolFactory: IKafkaConsumerPoolFactory[K,V], val topic: String, val offsets: Map[Int, (Long,Long)]) extends RDD[(K,V)](sc, Nil) {
  final def compute(partition: Partition, context: TaskContext): Iterator[(K,V)] = {
    val iterator: KafkaIterator[K,V] = new KafkaIterator[K,V](poolFactory.getKafkaConsumerPool, partition.asInstanceOf[KafkaPartition])
    context.addTaskCompletionListener(iterator)
    iterator
  }

  final def getPartitions: Array[Partition] = {
    val partitions = new Array[Partition](offsets.size)
    offsets.foreach {
      case (key, value) =>
        partitions(key) = new KafkaPartition(key, topic, key, value._1, value._2)
    }
    partitions
  }
}

class KafkaPartition(override val index: Int, val topic: String, val partition: Int, val startOffset: Long, val stopOffset: Long) extends Partition {
}

//Iterator based from org/apache/spark/util/NextIterator.scala
class KafkaIterator[K,V]
(val consumerPool: KafkaConsumerPool[K,V], val kafkaPartition: KafkaPartition)
  extends Iterator[(K,V)]
  with TaskCompletionListener {

  private var gotNext = false
  private var nextValue: (K,V) = _
  private var closed  = false
  protected var finished = false
  private var offset = kafkaPartition.startOffset
  private val consumer = consumerPool.getConsumer(kafkaPartition.topic, kafkaPartition.index)
  private var it: Iterator[MessageAndMetadata[K, V]] = null

  consumer.setCurrentOffset(kafkaPartition.startOffset)

  final def onTaskCompletion(context: TaskContext) {
    closeIfNeeded()
  }

  private def closeIfNeeded() {
    if (!closed) {
      closed = true
      consumerPool.returnConsumer(consumer)
    }
  }

  final def hasNext: Boolean = {
    if (!finished && !gotNext) {
      nextValue = getNext
      if (finished) {
        closeIfNeeded()
      } else {
        gotNext = true
      }
    }
    !finished
  }

  private def getNext: (K,V) = {
    if (it != null && it.hasNext) {
      val nv = it.next
      return (nv.key(), nv.message())
    }

    if (offset < kafkaPartition.stopOffset) {
      if (it == null || !it.hasNext) {
        it = consumer.receive(kafkaPartition.stopOffset)
        offset = consumer.getCurrentOffset
      }
      val nv = it.next
      (nv.key(), nv.message())
    } else {
      finished = true
      null.asInstanceOf[(K,V)]
    }
  }

  def next: (K,V) = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}