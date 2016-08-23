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

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicates;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Jeoffrey Lim
 * @version 0.1
 */
@SuppressWarnings("unused")
public class InfiniteRetryStrategy<V> {

    private static final InfiniteRetryStrategy<Integer> intResponseRetryer = new InfiniteRetryStrategy<>();

    private static final InfiniteRetryStrategy<Long> longResponseRetryer = new InfiniteRetryStrategy<>();

    private static final InfiniteRetryStrategy<Boolean> booleanResponseRetryer = new InfiniteRetryStrategy<Boolean>() {
        @Override
        protected Retryer<Boolean> init(final RetryerBuilder<Boolean> builder) {
            builder.retryIfResult(Predicates.or(Predicates.isNull(), Predicates.equalTo(false)));
            return builder.build();
        }
    };

    public final Retryer<V> retryer;

    public InfiniteRetryStrategy() {
        this.retryer = init(RetryerBuilder.<V>newBuilder()
                .retryIfResult(Predicates.equalTo((V)null))
                .retryIfException()
                .withWaitStrategy(WaitStrategies.fibonacciWait(100, 2, TimeUnit.MINUTES))
                .withStopStrategy(StopStrategies.neverStop()));
    }

    protected Retryer<V> init(final RetryerBuilder<V> builder) {
        return builder.build();
    }

    public V retryInfinitely(final Callable<V> callable) {
        try {
            return retryer.call(callable);
        } catch (Exception t) {
            throw new IllegalStateException("Unable to perform infinite retry", t);
        }
    }

    /**
     * Integer Response Retryer
     */
    public static long retryInfinitelyInt(final Callable<Integer> callable) {
        return intResponseRetryer.retryInfinitely(callable);
    }

    /**
     * Long Response Retryer
     */
    public static long retryInfinitelyLong(final Callable<Long> callable) {
        return longResponseRetryer.retryInfinitely(callable);
    }

    /**
     * Boolean Response Retryer
     */
    public static boolean retryInfinitelyBoolean(final Callable<Boolean> callable) {
        return booleanResponseRetryer.retryInfinitely(callable);
    }
}
