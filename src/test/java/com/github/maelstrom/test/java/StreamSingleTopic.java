package com.github.maelstrom.test.java;

import com.github.maelstrom.StreamProcessor;
import com.github.maelstrom.consumer.KafkaConsumerPoolFactory;
import com.github.maelstrom.consumer.OffsetManager;
import com.github.maelstrom.controller.ControllerKafkaTopic;
import com.github.maelstrom.controller.ControllerKafkaTopics;
import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Iterator;

public class StreamSingleTopic {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSingleTopic.class);

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamSingleTopic");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        CuratorFramework curator = OffsetManager.createCurator("127.0.0.1:2181");
        KafkaConsumerPoolFactory<String,String> poolFactory = new KafkaConsumerPoolFactory<>("127.0.0.1:9092", StringDecoder.class, StringDecoder.class);

        ControllerKafkaTopics<String,String> topics = new ControllerKafkaTopics<>(sc.sc(), curator, poolFactory);
        ControllerKafkaTopic<String,String> topic = topics.registerTopic("test_group", "test");

        new StreamProcessor<String,String>(topic) {
            @Override
            public final void process() {
                JavaRDD<Tuple2<String,String>> rdd = fetch().toJavaRDD();

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {
                    @Override
                    public final void call(final Iterator<Tuple2<String,String>> it) {
                        while (it.hasNext()) {
                            Tuple2<String,String> e = it.next();
                            LOG.info("key=" + e._1 + " message=" + e._2());
                        }
                    }
                });

                commit();
            }
        }.run();

        sc.sc().stop();
    }
}
