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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jeoffrey Lim
 * @version 0.2
 */
final class LeaderBrokerChecker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderBrokerChecker.class);

    private final String topic;
    private final int partitionId;

    private final List<Broker> brokerList;
    private final Thread internalThread;
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean hasChanged = new AtomicBoolean(false);

    private static final long CHECK_EVERY = 300000; //5 minutes || 30000; //30 secs

    LeaderBrokerChecker(final List<Broker> brokerList, final String topic, final int partitionId) {
        this.brokerList = brokerList;
        this.topic = topic;
        this.partitionId = partitionId;

        internalThread = new Thread(this);
        internalThread.setDaemon(true);
        internalThread.start();
    }

    final void stop() {
        if (!isStopped.getAndSet(true)) {
            try {
                internalThread.join();
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }
    }

    final boolean hasChanged() {
        return this.hasChanged.getAndSet(false);
    }

    private void setHasChanged() {
        hasChanged.getAndSet(true);
    }

    @Override
    public void run() {
        long lastRun = System.currentTimeMillis();
        Broker lastBroker = KafkaMetaData.findNewLeader(brokerList, null, topic, partitionId).leader();

        while (!isStopped.getAndSet(false)) {
            if ((System.currentTimeMillis() - lastRun) > CHECK_EVERY) {
                Broker checkBroker = KafkaMetaData.findNewLeader(brokerList, lastBroker, topic, partitionId).leader();
                LOG.debug("[{}:{}] CHECK BROKERS Latest: {} Current: {}", topic, partitionId, checkBroker.toString(), lastBroker.toString());

                // if *NOT* the same...
                if (!isSameBroker(lastBroker, checkBroker)) {
                    LOG.warn("[{}:{}] Broker changed. New: {} Old: {}", topic, partitionId, checkBroker.toString(), lastBroker.toString());
                    lastBroker = checkBroker;
                    setHasChanged();
                }

                lastRun = System.currentTimeMillis();
            } else {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public static boolean isSameBroker(final Broker lastBroker, final Broker checkBroker) {
        if (lastBroker == null || checkBroker == null)
            return false;

        return lastBroker.host().equals(checkBroker.host()) && lastBroker.port() == checkBroker.port();
    }
}
