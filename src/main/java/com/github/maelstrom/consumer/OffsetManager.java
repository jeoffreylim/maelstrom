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

import kafka.common.KafkaException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;

/**
 * @author Jeoffrey Lim
 * @version 0.2
 */
public class OffsetManager {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetManager.class);

    private final CuratorFramework curator;
    protected final String consumerGroup;
    protected final String topic;
    protected final int partitionId;

    private final String zkPath;
    private long lastOffset;
    private long lastCommitOffset;

    public OffsetManager(final CuratorFramework curator, final KafkaConsumer consumer, final String consumerGroup, final String topic, final int partitionId) {
        this.curator = curator;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.partitionId = partitionId;

        this.zkPath = "/consumers/" + consumerGroup + "/offsets/" + this.topic + "/" + this.partitionId;

        this.lastOffset = getStoredLastOffset();
        this.lastCommitOffset = lastOffset;

        long currentOffset = getLastOffset();
        long earliestOffset = consumer.getEarliestOffset();
        long latestOffset = consumer.getLatestOffset();

        if (latestOffset < 0 || earliestOffset < 0)
            throw new KafkaException("Unstable Kafka Connection");

        if (currentOffset == 0) {
            LOG.warn("{}-[{}:{}] No offset found starting from latest offset.", consumerGroup, topic, partitionId);
            setLastOffset(latestOffset);
            storeLastOffset(); //force commit
        } else {
            if (currentOffset < earliestOffset) {
                LOG.warn("Offset Out of Lower Bound for: {}-[{}:{}]@{} latest: {}", consumerGroup, topic, partitionId, currentOffset, latestOffset);
                setLastOffset(earliestOffset);
                storeLastOffset(); //force commit
            } else if (currentOffset > latestOffset) {
                LOG.warn("Offset Out of Higher Bound for: {}-[{}:{}]@{} latest: {}", consumerGroup, topic, partitionId, currentOffset, latestOffset);
                setLastOffset(latestOffset);
                storeLastOffset(); //force commit
            }
        }

        consumer.setCurrentOffset(getLastOffset());
        LOG.info("LAST OFFSET: {}-[{}:{}]@{}", consumerGroup, topic, partitionId, lastOffset);
    }

    public static CuratorFramework createCurator(final String zookeeper) {
        CuratorFramework curator = CuratorFrameworkFactory.builder().
                connectString(zookeeper).
                sessionTimeoutMs(120000).
                connectionTimeoutMs(120000).
                retryPolicy(new ExponentialBackoffRetry(1000, 29))
                .threadFactory(new ThreadFactory() {
                    @Override
                    public Thread newThread(@SuppressWarnings("NullableProblems") Runnable r) {
                        Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;
                    }
                }).build();
        curator.start();

        return curator;
    }

    private long getStoredLastOffset() {
        return InfiniteRetryStrategy.retryInfinitelyLong(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                try {
                    if (curator.checkExists().forPath(zkPath) == null) {
                        return 0L;
                    } else {
                        return Long.parseLong(new String(curator.getData().forPath(zkPath)));
                    }
                } catch (Exception e) {
                    LOG.warn("{}-[{}:{}] Error retrieving offset from ZooKeeper: {}", consumerGroup, topic, partitionId, e.getMessage(), e);
                    return null;
                }
            }
        });
    }

    public final long getLastOffset() {
        return lastOffset;
    }

    public final void setLastOffset(final long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public final void storeLastOffset() {
        if (lastCommitOffset == lastOffset)
            return;

        InfiniteRetryStrategy.retryInfinitelyBoolean(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    LOG.debug("STORE LAST OFFSET: {}-[{}:{}] @ {}", consumerGroup, topic, partitionId, lastOffset);

                    if (curator.checkExists().forPath(zkPath) == null)
                        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPath, String.valueOf(lastOffset).getBytes());
                    else
                        curator.setData().forPath(zkPath, String.valueOf(lastOffset).getBytes());

                    lastCommitOffset = lastOffset;

                    return true;
                } catch (Exception e) {
                    LOG.warn("{}-[{}:{}] Error storing offset in ZooKeeper: {}", consumerGroup, topic, partitionId, e.getMessage(), e);

                    return false;
                }
            }
        });
    }
}
