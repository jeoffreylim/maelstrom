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

import com.github.benmanes.multiway.LoadingMultiwayPool;
import com.github.benmanes.multiway.MultiwayPoolBuilder;
import com.github.benmanes.multiway.ResourceLifecycle;
import com.github.benmanes.multiway.ResourceLoader;
import kafka.cluster.Broker;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Jeoffrey Lim
 * @version 0.2
 */
public final class KafkaConsumerPool<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerPool.class);

    private final int maxSize;
    private final int expireAfterMins;
    private final List<Broker> brokerList;
    private final Decoder<K> keyMapper;
    private final Decoder<V> valueMapper;
    private final LoadingMultiwayPool<Tuple2<String, Integer>, KafkaConsumer<K,V>> pool;

    public KafkaConsumerPool(final int maxSize, final int expireAfterMins, final String brokers, final Decoder<K> keyMapper, final Decoder<V> valueMapper) {
        this.maxSize = maxSize;
        this.expireAfterMins = expireAfterMins;
        this.brokerList = KafkaMetaData.createBrokerList(brokers);
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.pool = createPool();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    LOG.info("Shutdown Hook: Shutting down KafkaConsumerPool");
                    shutdown();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });
    }

    public List<Broker> getBrokerList() {
        return brokerList;
    }

    private LoadingMultiwayPool<Tuple2<String, Integer>, KafkaConsumer<K,V>> createPool() {
        return MultiwayPoolBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expireAfterMins, TimeUnit.MINUTES)
                .lifecycle(new ResourceLifecycle<Tuple2<String, Integer>, KafkaConsumer<K,V>>() {
                    @Override
                    public void onRemoval(@SuppressWarnings("UnusedParameters") Tuple2<String, Integer> key, KafkaConsumer<K,V> consumer) {
                        LOG.info("CLOSE @ POOL: KafkaConsumer: {}", key);
                        consumer.close();
                    }
                })
                .build(new ResourceLoader<Tuple2<String, Integer>, KafkaConsumer<K,V>>() {
                    @Override
                    public KafkaConsumer<K,V> load(Tuple2<String, Integer> key) {
                        LOG.info("CREATE @ POOL: new KafkaConsumer: {}", key);
                        KafkaConsumer<K,V> consumer;
                        try {
                            consumer = new KafkaConsumer<>(brokerList, "spark-kafka-consumer-pool", keyMapper, valueMapper, key._1(), key._2());
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                            throw e;
                        }
                        return consumer;
                    }
                });
    }

    public final KafkaConsumer<K,V> getConsumer(final String topic, final int partition) {
        LOG.debug("GET CONSUMER: KafkaConsumer: [{}:{}]", topic, partition);
        return pool.borrow(new Tuple2<>(topic, partition));
    }

    public final void returnConsumer(final KafkaConsumer<K,V> consumer) {
        LOG.debug("RETURN CONSUMER: KafkaConsumer: [{}:{}]", consumer.topic, consumer.partitionId);
        pool.release(consumer);
    }

    public final void shutdown() {
        LOG.info("SHUTTING DOWN POOL");
        pool.invalidateAll();
        pool.cleanUp();
    }
}
