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
 *//*
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

import java.io.Serializable
import java.util.concurrent.atomic.AtomicBoolean

import com.github.maelstrom.consumer.IKafkaConsumerPoolFactory
import com.github.maelstrom.controller.IControllerKafka
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Base class for stream processing as a single unit, whether it could be a single
  * or multi topic (which is unique in Kafka). Many combinations can be done in
  * Kafka depending on the intended purpose. For example, one could read on the same
  * topic but have 2 different consumer groups & processors: a processor for storing data to DB,
  * and another for filtering messages and sending emails.
  *
  * @author Jeoffrey Lim
  * @version 0.1
  */
abstract class StreamProcessor[K, V](@transient val sc: SparkContext,
                                     @transient val pool: IKafkaConsumerPoolFactory[K, V],
                                     @transient val controller: IControllerKafka[K, V]) extends Runnable with Serializable {
  final private val isStopCalled: AtomicBoolean = new AtomicBoolean(false)

  final def run() {
    while (!isStopCalled.getAndSet(false)) {
      if (getLag > 0 && shouldProcess) process()
      else Thread.sleep(100L)
    }
    controller.close()
    stopped()
  }

  /**
    * Stop the stream processor
    */
  final def stop() {
    isStopCalled.getAndSet(true)
  }

  /**
    * Get the lag of the registered controller
    * @return
    */
  final def getLag: Long = {
    controller.getLag
  }

  /**
    * Default fetch retrieves only maximum of 5k messages by default
    * @return RDD
    */
  def fetch(): RDD[(K, V)] = {
    fetch(5000)
  }

  /**
    * Fetch all messages from the registered controller
    * @param max maximum number of messages to retrieve
    * @return RDD
    */
  def fetch(max: Int): RDD[(K, V)] = {
    controller.getRDD(sc, pool, max)
  }

  /**
    * Extend this function to provide other conditions before triggering processing.
    * An example of this is to check maximum lag (bucket full), or trigger every X time elapsed.
    * This would be useful for most especially for data aggregation.
    *
    * @return true if stream processing should proceed
    */
  protected def shouldProcess: Boolean = {
    true
  }

  /**
    * Main processing routine. This is where you fetch data as KafkaRDDs and use Spark to operate on the data.
    */
  def process()

  /**
    * Helper function to commit the offsets
    */
  final def commit() {
    controller.commit()
  }

  /**
    * Extend this if you want to be notified if stream processing has halted.
    */
  protected def stopped() {
  }
}