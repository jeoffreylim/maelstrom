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

package com.github.maelstrom.consumer;

import java.io.Serializable;

/*
* Basic Usage:
*
* <pre>
* {
* @code
* class MyKafkaConsumerPoolFactory implements IKafkaConsumerPoolFactory {
*   @Override
*   public KafkaConsumerPool<String, AvroDataModel> getKafkaConsumerPool() {
*       return ConsumerPoolHolder.instance;
*   }
*
*   private static class ConsumerPoolHolder {
*       private static final KafkaConsumerPool<AvroDataModel> instance =
*           new KafkaConsumerPool<>(....);
*   }
* }
* }
* </pre>
*
* The reason behind this is: Spark does not 'yet' have setup hook or shutdowwn hook for expensive resource objects
* that needs to run in the distributed cluster (spark workers).
*
* Refer TO:
*   https://issues.apache.org/jira/browse/SPARK-650 (setup-hook not available)
*   https://issues.apache.org/jira/browse/SPARK-1107 (shutdown hook not available)
*
* @author Jeoffrey Lim
* @version 0.1
* */
public interface IKafkaConsumerPoolFactory<K,V> extends Serializable {
    KafkaConsumerPool<K,V> getKafkaConsumerPool();
}
