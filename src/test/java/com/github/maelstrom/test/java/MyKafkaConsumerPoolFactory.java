package com.github.maelstrom.test.java;

import com.github.maelstrom.consumer.IKafkaConsumerPoolFactory;
import com.github.maelstrom.consumer.KafkaConsumerPool;
import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * User: jeoffrey
 * Date: 8/18/16
 * Time: 11:59 PM.
 */
public class MyKafkaConsumerPoolFactory implements IKafkaConsumerPoolFactory<String,String> {
        private static final Logger LOG = LoggerFactory.getLogger(MyKafkaConsumerPoolFactory.class);

    @Override
    public KafkaConsumerPool<String,String> getKafkaConsumerPool() {
        return MyKafkaConsumerPoolFactory.ConsumerPoolHolder.instance;
    }

    private static class ConsumerPoolHolder {
        private static final String env = System.getProperty("my.env");
        private static final Properties properties = load();
        private static final String BROKER_LIST = properties.getProperty(env + ".broker_list");
        private static final int DEFAULT_POOL_SIZE = 200;
        private static final int DEFAULT_EXPIRE_AFTER_MINUTES = 5;

        private static final KafkaConsumerPool<String,String> instance =
                new KafkaConsumerPool<>(DEFAULT_POOL_SIZE,
                        DEFAULT_EXPIRE_AFTER_MINUTES,
                        BROKER_LIST,
                        new StringDecoder(null),
                        new StringDecoder(null)
                );

        private ConsumerPoolHolder() {
            //SINGLETON
        }

        private static Properties load() {
            Properties props = new Properties();

            try {
                props.load(MyKafkaConsumerPoolFactory.class.getResourceAsStream("/myconfig.properties"));
            } catch (IOException e) {
                throw new RuntimeException("Config cannot be loaded", e);
            }
            return props;
        }
    }
}
