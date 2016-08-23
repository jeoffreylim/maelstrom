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

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Heavily inspired by Yahoo Pistachio's Kafka Consumer
 * - https://github.com/lyogavin/Pistachio/blob/master/src/main/java/com/yahoo/ads/pb/kafka/KafkaSimpleConsumer.java
 *
 * @author Jeoffrey Lim
 * @version 0.1
 */
public final class KafkaConsumer<K,V> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private static final long EARLIEST_TIME = kafka.api.OffsetRequest.EarliestTime();
    private static final long LATEST_TIME = kafka.api.OffsetRequest.LatestTime();

    private static final InfiniteRetryStrategy<OffsetResponse> offsetResponseRetryer = new InfiniteRetryStrategy<>();
    private static final InfiniteRetryStrategy<FetchResponse> fetchResponseRetryer = new InfiniteRetryStrategy<>();

    private final List<Broker> brokersList;
    private final Decoder<K> keyDecoder;
    private final Decoder<V> valueDecoder;

    public final String consumerGroup;
    public final String topic;
    public final int partitionId;

    private final LeaderBrokerChecker leaderBrokerChecker;
    private SimpleConsumer consumer;
    private final String clientId;

    private long currentOffset;

    public KafkaConsumer(final List<Broker> brokerList, final String consumerGroup,
                         final Decoder<K> keyDecoder, final Decoder<V> valueDecoder,
                         final String topic, final int partitionId) {
        this.brokersList = brokerList;
        this.consumerGroup = consumerGroup;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.topic = topic;
        this.partitionId = partitionId;

        this.clientId = KafkaMetaData.createClientId(consumerGroup, topic, partitionId);
        this.leaderBrokerChecker = new LeaderBrokerChecker(brokersList, topic, partitionId);

        connect();
    }

    private void connect() {
        LOG.info("Create consumer: {}-[{}:{}]", consumerGroup,topic, partitionId);

        if (consumer != null)
            consumer.close();

        consumer = KafkaMetaData.getLeaderBroker(brokersList, clientId, topic, partitionId);
    }

    public final void close() {
        LOG.info("Close consumer: {}-[{}:{}] Last offset: {})", consumerGroup, topic, partitionId, currentOffset);

        //clean up consumer
        if (consumer != null)
            consumer.close();

        //clean up broker leader checker
        leaderBrokerChecker.stop();
    }

    private void verifyBroker() {
        if (leaderBrokerChecker.hasChanged()) {
            LOG.warn("{}-[{}-{}] Broker changed. Creating new Consumer", consumerGroup, topic, partitionId);
            connect();
        }
    }

    @SuppressWarnings("squid:MethodCyclomaticComplexity")
    private void checkNeedNewLeader(final short errorCode) {
        LOG.warn("Error fetching data from the Broker: [{}:{}] Topic: {}-[{}:{}]@{} Error: {}", consumer.host(), consumer.port(), consumerGroup, topic, partitionId, currentOffset, errorCode);

        boolean needNewLeader = false;

        if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
            PartitionMetadata partitionMetadata = KafkaMetaData.getPartitionMetadata(consumer, Collections.singletonList(topic), partitionId);

            if (partitionMetadata == null || !LeaderBrokerChecker.isSameBroker(
                    KafkaMetaData.findNewLeader(brokersList, partitionMetadata.leader(), topic, partitionId).leader(),
                    partitionMetadata.leader())) {
                needNewLeader = true;
            } else {
                long earliestOffset = getEarliestOffset();
                long latestOffset = getLatestOffset();

                if (latestOffset < 0 || earliestOffset < 0)
                    needNewLeader = true;
                else if (currentOffset > latestOffset)
                    throw new KafkaException("Offset Out of Higher Bound for [" + topic + ":" + partitionId + "@" + currentOffset + "] latest:" + latestOffset);
                else if (currentOffset < earliestOffset)
                    throw new KafkaException("Offset Out of Lower Bound for [" + topic + ":" + partitionId + "@" + currentOffset + "] earliest:" + earliestOffset);
            }
        } else {
            needNewLeader = true;
        }

        if (needNewLeader)
            connect();
    }

    /**
     * Iterator of incoming messages halting at stopAtOffset
     *
     * @param stopAtOffset offset to stop retrieving data
     * @return an iterator of MessageAndMetadata key/value
     */
    @SuppressWarnings({"squid:MethodCyclomaticComplexity", "squid:S1188"})
    public Iterator<MessageAndMetadata<K,V>> receive(final long stopAtOffset) {
        //always verify broker if changed...
        verifyBroker();

        // The fetch API is used to fetch a chunk of one or more logs for some topic-partitions.
        // Logically one specifies the topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages.
        // In general, the return messages will have offsets larger than or equal to the starting offset.
        final FetchRequest request = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partitionId, currentOffset, 100000000).maxWait(1000).build();
        final FetchResponse response = fetchResponseRetryer.retryInfinitely(new Callable<FetchResponse>() {
            @Override
            public FetchResponse call() {
                try {
                    FetchResponse response = consumer.fetch(request);
                    if (response != null && !response.hasError())
                        return response;

                    short errorCode = response != null ? response.errorCode(topic, partitionId) : -1;

                    if (errorCode == ErrorMapping.RequestTimedOutCode())
                        return response;
                    else
                        checkNeedNewLeader(errorCode);
                } catch (Exception e) {
                    LOG.warn("Exception while fetch {}-[{}:{}]", consumerGroup, topic, partitionId, e);
                    connect();
                }

                return null;
            }
        });

        if (response.hasError())
            return Collections.emptyListIterator();

        List<MessageAndMetadata<K, V>> ret = new LinkedList<>();

        long lastOffset = currentOffset;

        for (MessageAndOffset messageAndOffset : response.messageSet(topic, partitionId)) {
            // However, with compressed messages, it's possible for the returned messages to have offsets smaller than the starting offset.
            // The number of such messages is typically small and the caller is responsible for filtering out those messages.
            final long messageOffset = messageAndOffset.offset();

            if (messageOffset >= lastOffset && messageOffset < stopAtOffset) {
                //set next offset
                currentOffset = messageAndOffset.nextOffset();
                Message message = messageAndOffset.message();
                MessageAndMetadata messageAndMetadata = new MessageAndMetadata<>(topic, partitionId, message, messageOffset, keyDecoder, valueDecoder);

                if (LOG.isDebugEnabled())
                    LOG.info("Kafka consumed message: [{}:{}]@{} - {}={}", topic, partitionId, messageOffset, messageAndMetadata.key(), messageAndMetadata.message());

                ret.add(messageAndMetadata);
            }
        }

        return ret.iterator();
    }

    public final long getCurrentOffset() {
        return currentOffset;
    }

    public final void setCurrentOffset(final long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public long getEarliestOffset() {
        return getOffset(EARLIEST_TIME).offsets(topic, partitionId)[0];
    }

    public long getLatestOffset() {
        return getOffset(LATEST_TIME).offsets(topic, partitionId)[0];
    }

    private OffsetResponse getOffset(final long whichTime) {
        verifyBroker();

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        final OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);

        return offsetResponseRetryer.retryInfinitely(new Callable<OffsetResponse>() {
            @Override
            public OffsetResponse call() throws Exception {
                try {
                    OffsetResponse response = consumer.getOffsetsBefore(request);
                    if (response != null) {
                        long[] offsets = response.offsets(topic, partitionId);
                        if (offsets != null && offsets.length > 0)
                            return response;
                    }
                    LOG.warn("Error fetching offset data: {}-[{}:{}] Error code: {}", consumerGroup, topic, partitionId, response != null ? response.errorCode(topic, partitionId) : -1);
                } catch (Exception e) {
                    LOG.warn("Error fetching offset data: {}-[{}:{}] Exception: {}", consumerGroup, topic, partitionId, e.getMessage(), e);
                }

                connect(); //always reconnect in case offset cannot be obtained

                return null;
            }
        });
    }
}
