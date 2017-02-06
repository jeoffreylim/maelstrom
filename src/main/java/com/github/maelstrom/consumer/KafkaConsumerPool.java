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

import com.google.common.cache.*;
import kafka.cluster.Broker;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.objectpool.ConcurrentPool;
import org.vibur.objectpool.PoolObjectFactory;
import org.vibur.objectpool.PoolService;
import org.vibur.objectpool.util.ConcurrentLinkedQueueCollection;
import scala.Tuple2;

import javax.annotation.Nonnull;
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
    private final LoadingCache<Tuple2<String, Integer>, PoolService<KafkaConsumer<K,V>>> pool;

    KafkaConsumerPool(final int maxSize, final int expireAfterMins, final String brokers, final Decoder<K> keyMapper, final Decoder<V> valueMapper) {
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

    @SuppressWarnings("unused")
    public List<Broker> getBrokerList() {
        return brokerList;
    }

    private LoadingCache<Tuple2<String, Integer>, PoolService<KafkaConsumer<K,V>>> createPool() {
        return CacheBuilder.newBuilder()
                .maximumSize(10_000)
                .expireAfterAccess(expireAfterMins, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<Tuple2<String, Integer>, PoolService<KafkaConsumer<K,V>>>() {
                    @Override
                    public void onRemoval(@Nonnull final RemovalNotification<Tuple2<String, Integer>, PoolService<KafkaConsumer<K,V>>> removalNotification) {
                        LOG.info("CLOSE @ POOL: KafkaConsumer: {}", removalNotification.getKey());
                        if (removalNotification.getValue() != null)
                            (removalNotification.getValue()).close();
                    }
                })
                .build(new CacheLoader<Tuple2<String, Integer>, PoolService<KafkaConsumer<K,V>>>() {
                    @Override
                    public PoolService<KafkaConsumer<K,V>> load(@Nonnull final Tuple2<String, Integer> key) throws Exception {
                        LOG.info("CREATE @ POOL: new KafkaConsumer: {}", key);

                        return new ConcurrentPool<>(
                                new ConcurrentLinkedQueueCollection<KafkaConsumer<K,V>>(),
                                new KafkaConsumerFactory<>(
                                        brokerList, keyMapper, valueMapper, key._1(), key._2()
                                ),
                                1,
                                maxSize,
                                false);
                    }
                });
    }

    public final PoolService<KafkaConsumer<K,V>> getConsumer(final String topic, final int partition) {
        LOG.debug("GET CONSUMER: KafkaConsumer: [{}:{}]", topic, partition);
        return pool.getUnchecked(new Tuple2<>(topic, partition));
    }

    public final void returnConsumer(final PoolService<KafkaConsumer<K,V>> pool, final KafkaConsumer<K,V> consumer) {
        LOG.debug("RETURN CONSUMER: KafkaConsumer: [{}:{}]", consumer.topic, consumer.partitionId);
        pool.restore(consumer);
    }

    @SuppressWarnings("WeakerAccess")
    public final void shutdown() {
        LOG.info("SHUTTING DOWN POOL");
        pool.invalidateAll();
        pool.cleanUp();
    }

    private static class KafkaConsumerFactory<K,V> implements PoolObjectFactory<KafkaConsumer<K,V>> {
        private final List<Broker> brokerList;
        private final Decoder<K> keyMapper;
        private final Decoder<V> valueMapper;
        private final String topic;
        private final Integer partition;

        KafkaConsumerFactory(final List<Broker> brokerList,
                             final Decoder<K> keyMapper,
                             final Decoder<V> valueMapper,
                             final String topic,
                             final Integer partition) {
            this.brokerList = brokerList;
            this.keyMapper = keyMapper;
            this.valueMapper = valueMapper;
            this.topic = topic;
            this.partition = partition;
        }

        @Override
        public final KafkaConsumer<K,V> create() {
            try {
                LOG.info("CREATE @ KafkaConsumer: {}:{}", topic, partition);
                return new KafkaConsumer<>(brokerList, "spark-kafka-consumer-pool", keyMapper, valueMapper, topic, partition);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw e;
            }
        }

        @Override
        public final boolean readyToTake(final KafkaConsumer<K,V> obj) {
            return true;
        }

        @Override
        public final boolean readyToRestore(final KafkaConsumer<K,V> obj) {
            return true;
        }

        @Override
        public final void destroy(final KafkaConsumer<K,V> consumer) {
            LOG.info("CLOSE @ KafkaConsumer: {}:{}", topic, partition);
            consumer.close();
        }
    }
}
