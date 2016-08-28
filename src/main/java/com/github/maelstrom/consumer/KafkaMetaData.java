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
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Jeoffrey Lim
 * @version 0.2
 */
public class KafkaMetaData {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetaData.class);
    private static final String META_DATA_CLIENT_ID = "-metadata";

    private static final InfiniteRetryStrategy<List<Broker>> brokerListRetryer = new InfiniteRetryStrategy<>();
    private static final InfiniteRetryStrategy<Map<String, TopicMetadata>> topicMetaResponseRetryer = new InfiniteRetryStrategy<>();
    private static final InfiniteRetryStrategy<PartitionMetadata> pmResponseRetryer = new InfiniteRetryStrategy<>();
    private static final InfiniteRetryStrategy<SimpleConsumer> consumerResponseRetryer = new InfiniteRetryStrategy<>();

    private KafkaMetaData() {
    }

    public static List<Broker> createBrokerList(final String seedBrokers) {
        return brokerListRetryer.retryInfinitely(new Callable<List<Broker>>() {
            @Override
            public List<Broker> call() {
                try {
                    Set<Broker> seedBrokersSet = new HashSet<>();

                    for (String seedBroker : seedBrokers.split(",")) {
                        String[] hostAndPort = seedBroker.split(":", 2);
                        String host = hostAndPort[0];
                        int port = Integer.parseInt(hostAndPort[1]);

                        //brute-force way to obtain ConsumerMetadataKey
                        //to support older kafka servers
                        SimpleConsumer consumer = null;

                        try {
                            consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, META_DATA_CLIENT_ID);
                            final TopicMetadataRequest req = new TopicMetadataRequest(new ArrayList<String>());
                            TopicMetadataResponse resp = consumer.send(req);

                            List<TopicMetadata> topicMetadataList = resp.topicsMetadata();

                            for (TopicMetadata metaData : topicMetadataList) {
                                for (PartitionMetadata pm : metaData.partitionsMetadata()) {
                                    Broker leader = pm.leader();
                                    //validate leader
                                    if (leader != null && leader.host() != null)
                                        seedBrokersSet.add(leader);
                                }
                            }
                        } catch (Exception e) {
                            LOG.error("Error communicating with Broker [{}:{}] to fetch topic meta data. Reason: {}", host, port, e.getMessage(), e);
                        } finally {
                            if (consumer != null)
                                consumer.close();
                        }
                    }

                    List<Broker> seedBrokersList = new CopyOnWriteArrayList<>(seedBrokersSet);

                    Collections.shuffle(seedBrokersList);

                    for (Broker broker: seedBrokersList)
                        LOG.info("FOUND BROKER [{}-{}:{}]", broker.id(), broker.host(), broker.port());

                    return seedBrokersList;
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    return null;
                }
            }
        });
    }

    public static String createClientId(final String consumerGroup, final String topic, final int partition) {
        String hostName = System.getProperty("os.name").toLowerCase().contains("win") ?
                System.getenv("COMPUTERNAME") :
                System.getenv("HOSTNAME");

        if (hostName == null || hostName.isEmpty()) {
            try {
                hostName = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                LOG.warn("[{}] Can't determine local hostname", partition, e);
                hostName = "unknown.host";
            }
        }

        return consumerGroup + "-" + topic + "-" + partition + "-" + hostName;
    }

    @SuppressWarnings("squid:S1188")
    public static Map<String, TopicMetadata> getTopicMetaData(final List<Broker> brokerList, final List<String> topics) {
        final TopicMetadataRequest req = new TopicMetadataRequest(topics);

        return topicMetaResponseRetryer.retryInfinitely(new Callable<Map<String, TopicMetadata>>() {
            @Override
            public Map<String, TopicMetadata> call() throws Exception {
                for (Broker seed : brokerList) {
                    SimpleConsumer consumer = null;

                    try {
                        consumer = new SimpleConsumer(seed.host(), seed.port(), 100000, 64 * 1024, META_DATA_CLIENT_ID);
                        TopicMetadataResponse resp = consumer.send(req);

                        List<TopicMetadata> topicMetadataList = resp.topicsMetadata();

                        Map<String, TopicMetadata> topicMetaData = new HashMap<>();

                        for (TopicMetadata metaData : topicMetadataList)
                            topicMetaData.put(metaData.topic(), metaData);

                        return topicMetaData;
                    } catch (Exception e) {
                        LOG.error("Error communicating with Broker [{}] to fetch topic meta data. Reason: {}", seed, e.getMessage(), e);
                    } finally {
                        if (consumer != null)
                            consumer.close();
                    }
                }

                return null;
            }
        });
    }

    public static PartitionMetadata getPartitionMetadata(final SimpleConsumer consumer, final List<String> topics, final int partitionId) {
        try {
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            TopicMetadataResponse resp = consumer.send(req);

            List<TopicMetadata> topicMetadataList = resp.topicsMetadata();

            for (TopicMetadata metaData : topicMetadataList) {
                for (PartitionMetadata part : metaData.partitionsMetadata()) {
                    if (part.partitionId() == partitionId) {
                        return part;
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Unable to fetch partition meta data from host[{}:{}] [{}:{}]", consumer.host(), consumer.port(), topics, partitionId, e);
        }

        return null;
    }

    @SuppressWarnings({"squid:S1188", "squid:S135", "squid:MethodCyclomaticComplexity"})
    public static PartitionMetadata findNewLeader(final List<Broker> brokerList, final Broker currentBroker, final String topic, final int partitionId) {
        final List<String> topics = Collections.singletonList(topic);

        return pmResponseRetryer.retryInfinitely(new Callable<PartitionMetadata>() {
            @Override
            public final PartitionMetadata call() throws Exception {
                for (final Broker broker : brokerList) {
                    SimpleConsumer consumer = null;

                    try {
                        consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, META_DATA_CLIENT_ID);
                        PartitionMetadata partitionMetadata = getPartitionMetadata(consumer, topics, partitionId);

                        //skip "unstable" workers not having information about the topic & partition
                        if (partitionMetadata == null || partitionMetadata.leader() == null)
                            continue;

                        //attempt find within the list
                        if (currentBroker == null && LeaderBrokerChecker.isSameBroker(broker, partitionMetadata.leader()))
                            return partitionMetadata;

                        //ignore "self" lookup, and only look at replica brokers
                        if (LeaderBrokerChecker.isSameBroker(currentBroker, partitionMetadata.leader()))
                            continue;
                        //if reported from a replica broker that it is no longer the leader, return new leader broker information
                        if (!LeaderBrokerChecker.isSameBroker(currentBroker, partitionMetadata.leader()))
                            return partitionMetadata;
                    } catch (Exception e) {
                        LOG.error("Error communicating with Broker [{}] to find Leader for [{}:{}] Reason: {}", broker, topic, partitionId, e.getMessage(), e);
                    } finally {
                        if (consumer != null)
                            consumer.close();
                    }
                }

                return null;
            }
        });
    }

    @SuppressWarnings("squid:S1188")
    public static SimpleConsumer getLeaderBroker(final List<Broker> brokerList, final String clientId, final String topic, final int partitionId) {
        final List<String> topics = Collections.singletonList(topic);

        return consumerResponseRetryer.retryInfinitely(new Callable<SimpleConsumer>() {
            @Override
            public final SimpleConsumer call() throws Exception {

                for (final Broker broker : brokerList) {
                    SimpleConsumer consumer = null;

                    try {
                        consumer = new SimpleConsumer(broker.host(), broker.port(), 100000, 64 * 1024, clientId);
                        PartitionMetadata partitionMetadata = getPartitionMetadata(consumer, topics, partitionId);

                        //skip "unstable" workers not having information about the topic & partition
                        if (partitionMetadata == null || partitionMetadata.leader() == null)
                            continue;

                        LOG.info("broker [{}:{}] = consumer [{}:{}] = partition-leader: [{}:{}]", broker.host(), broker.port(), consumer.host(), consumer.port(), partitionMetadata.leader().host(), partitionMetadata.leader().port());

                        if (LeaderBrokerChecker.isSameBroker(broker, partitionMetadata.leader())) {
                            SimpleConsumer leaderConsumer = consumer;
                            consumer = null;
                            return leaderConsumer;
                        }
                    } catch (Exception e) {
                        LOG.error("Error communicating with Broker [{}] to find Leader for [{}:{}] Reason: {}", broker, topic, partitionId, e.getMessage(), e);
                    } finally {
                        if (consumer != null)
                            consumer.close();
                    }
                }

                return null;
            }
        });
    }
}
