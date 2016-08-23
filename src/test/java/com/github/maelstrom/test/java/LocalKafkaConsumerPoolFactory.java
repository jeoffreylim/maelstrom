package com.github.maelstrom.test.java;

import com.github.maelstrom.consumer.IKafkaConsumerPoolFactory;
import com.github.maelstrom.consumer.KafkaConsumerPool;
import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: jeoffrey
 * Date: 8/15/16
 * Time: 4:13 AM.
 */
public class LocalKafkaConsumerPoolFactory implements IKafkaConsumerPoolFactory<String,String> {
    private static final Logger LOG = LoggerFactory.getLogger(LocalKafkaConsumerPoolFactory.class);

    @Override
    public KafkaConsumerPool<String,String> getKafkaConsumerPool() {
        return ConsumerPoolHolder.instance;
    }

    private static class ConsumerPoolHolder {
        private static final int DEFAULT_POOL_SIZE = 200;
        private static final int DEFAULT_EXPIRE_AFTER_MINUTES = 5;

        private static final KafkaConsumerPool<String,String> instance =
                new KafkaConsumerPool<>(DEFAULT_POOL_SIZE,
                        DEFAULT_EXPIRE_AFTER_MINUTES,
                        "127.0.0.1:9092",
                        new StringDecoder(null),
                        new StringDecoder(null)
                );

        private ConsumerPoolHolder() {
            //SINGLETON
        }
    }
}
