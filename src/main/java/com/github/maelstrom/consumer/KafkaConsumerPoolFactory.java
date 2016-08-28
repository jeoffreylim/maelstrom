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

import kafka.cluster.Broker;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/*
* Kafka Consumer Pool Factory
*
* This class gets serialized over to the Spark Executors to initialize a Kafka Consumer Object Pool
*
* @author Jeoffrey Lim
* @version 0.2
* */
public final class KafkaConsumerPoolFactory<K,V> implements Serializable {
    private static final int DEFAULT_POOL_SIZE = 1000;
    private static final int DEFAULT_EXPIRE_AFTER_MINUTES = 5;

    public final int maxSize;
    public final int expireAfterMins;
    public final String brokers;
    public final Class keyMapper;
    public final Class valueMapper;
    public final Properties verifiableProperties;
    private List<Broker> brokerList;

    public KafkaConsumerPoolFactory(final String brokers, final Class keyMapper, final Class valueMapper) {
        this(DEFAULT_POOL_SIZE, DEFAULT_EXPIRE_AFTER_MINUTES, brokers, keyMapper, valueMapper, new Properties());
    }

    public KafkaConsumerPoolFactory(final String brokers, final Class keyMapper, final Class valueMapper, final Properties verifiableProperties) {
        this(DEFAULT_POOL_SIZE, DEFAULT_EXPIRE_AFTER_MINUTES, brokers, keyMapper, valueMapper, verifiableProperties);
    }

    public KafkaConsumerPoolFactory(final int maxSize, final int expireAfterMins, final String brokers, final Class keyMapper, final Class valueMapper) {
        this(maxSize, expireAfterMins, brokers, keyMapper, valueMapper, new Properties());
    }

    public KafkaConsumerPoolFactory(final int maxSize, final int expireAfterMins, final String brokers, final Class keyMapper, final Class valueMapper, final Properties verifiableProperties) {
        this.maxSize = maxSize;
        this.expireAfterMins = expireAfterMins;
        this.brokers = brokers;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.verifiableProperties = verifiableProperties;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(maxSize, expireAfterMins, brokers, keyMapper, valueMapper);
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;

        final KafkaConsumerPoolFactory other = (KafkaConsumerPoolFactory) obj;

        return this.maxSize == other.maxSize &&
                this.expireAfterMins == other.expireAfterMins &&
                this.keyMapper.getClass() == other.keyMapper.getClass() &&
                this.valueMapper.getClass() == other.valueMapper.getClass();
    }

    public final List<Broker> getBrokerList() {
        if (brokerList == null)
            brokerList = KafkaMetaData.createBrokerList(brokers);
        return brokerList;
    }

    @SuppressWarnings("unchecked")
    public final Decoder<K> createKeyDecoder() {
        try {
            return (Decoder<K>)keyMapper.getDeclaredConstructor(VerifiableProperties.class).newInstance(verifiableProperties != null ? new VerifiableProperties(verifiableProperties) : null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create key decoder: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public final Decoder<V> createValueDecoder() {
        try {
            return (Decoder<V>)valueMapper.getDeclaredConstructor(VerifiableProperties.class).newInstance(verifiableProperties != null ? new VerifiableProperties(verifiableProperties) : null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create value decoder: " + e.getMessage(), e);
        }
    }

    public final KafkaConsumerPool<K,V> createKafkaConsumerPool() {
        return new KafkaConsumerPool<>(maxSize, expireAfterMins, brokers, createKeyDecoder(), createValueDecoder());

    }
}
