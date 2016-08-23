# Maelstrom

Maelstrom is an open source Kafka integration with Spark that is designed to be developer friendly, high performance
(sub-millisecond stream processing), scalable (consumes messges at Spark worker nodes), and is extremely reliable.

This library has been running stable in production environment and has been proven to be resilient to numerous
production issues.  

Thanks to [Adlogica](http://www.adlogica.com/)  for sharing to the open source community this project!

## Features

- Simple framework which follows Kafka semantics and best practices.
- High performance, with latencies down to sub-milliseconds.
- Scalable, where message consumption is received in the Spark worker nodes and not on the driver side.
- Throttling by specifying maximum number of messages to process per each "bulk receive" 
- Built-in offset management stored in Zookeeper. Numerous Kafka monitoring tools should work out of the box.
- Fault tolerant design, if in case stream processing fails: it would go back to the last processed offsets.
- Resilient to Kafka problems (automatic leader election detection, rebalance, etc)
- Kafka connection resource is pooled and re-used but always get validated if connected to the correct leader broker.

## The Nuts & Bolts

- The Kafka Consumer is built from scratch and is heavily inspired by Yahoo's Pistachio Kafka Consumer.
- With infinite retry strategy in the Kafka Consumer level (like in Pistachio), any issues related to Kafka
  gets rectified and resolved which makes the application automatically recover.
- Utilizing Ben Mane's multi-way pool, Kafka Consumers gets reused using (topic+partition) as keys.
- As message consumption runs in the Spark worker nodes, to achieve scalability simply add more partitions to your Kafka topic.

## Zero data loss

- As offset commits is done per each successful processing, failure scenario in between could happen: like abruptly
  terminating the application or external systems become unavailable.
- Ensure that your application has the ability to do graceful shutdown, this can be achieved by exposing an http
  endpoint and calling *stop* to the ProcessorRunner.
- Implement your own retry strategy in case of processing failure due to external dependencies

## Exactly once semantics

- Generate unique ID for every message.
- For near real-time stream processing: processors should write to another Kafka topic, and utilize Memcache/Redis 
  to achieve exactly once semantics (i.e. send e-mail functionality).
- For Batch processing: use Spark mapToPair + reduceByKey to remove duplicates. (*Pro tip*: repartition RDDs 
  to maximize Spark cluster hardware utilization) 

  
## Building Maelstrom from Source

Prerequisites for building Maelstrom:

* Git
* Maven
* Java 7 or 8

```
git clone https://github.com/jeoffreylim/maelstrom.git
cd maelstrom
mvn install
```

## Including Maelstrom in your project

Add the library in your project maven dependencies:

```
    <dependency>
        <groupId>com.github.maelstrom</groupId>
        <artifactId>maelstrom</artifactId>
        <version>0.1</version>
    </dependency>
```

## Setting up the Kafka ConsumerPool Factory

To be able to consume messages on the Spark worker side and re-use Kafka connections, a "static initializer" is required. 
This limitation is in Spark itself, please refer to:

https://issues.apache.org/jira/browse/SPARK-650 (Add a "setup hook" API for running initialization code on each executor)
https://issues.apache.org/jira/browse/SPARK-1107 (Add shutdown hook on executor stop to stop running tasks)

In the example below, a configuration can be read from your Spark application jar with multiple environments.

```java
class MyKafkaConsumerPoolFactory implements IKafkaConsumerPoolFactory<String,AvroDataModel> {

    @Override
    public KafkaConsumerPool<String,AvroDataModel> getKafkaConsumerPool() {
        return ConsumerPoolHolder.instance;
    }

    private static class ConsumerPoolHolder {
        private static final String env = System.getProperty("my.env");
        private static final Properties properties = load();
        private static final String BROKER_LIST = properties.getProperty(env + ".broker_list");

        private static final KafkaConsumerPool<String,String> instance =
                new KafkaConsumerPool<>(DEFAULT_POOL_SIZE,
                        DEFAULT_EXPIRE_AFTER_MINUTES,
                        BROKER_LIST,
                        new StringDecoder(null),
                        new AvroDataModelDecoder()
                );

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
```

In your project *resources/myconfig.properties*:

```
local.broker_list=127.0.0.1:9092
staging.broker_list=staging1:9092,staging2:9092,staging3:9092
production.broker_list=prod1:9092,prod2:9092,prod3:9092
```

And run your application (if _staging_):

```
spark-submit \
    ....
    --conf "spark.driver.extraJavaOptions=-Dmy.env=staging \
    --conf "spark.executor.extraJavaOptions=-Dmy.env=staging \
    --class <spark app>
```

## Java example

```java
SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamSingleTopic");
JavaSparkContext sc = new JavaSparkContext(sparkConf);

CuratorFramework curator = OffsetManager.createCurator("127.0.0.1:2181");
IKafkaConsumerPoolFactory<String,String> pool = new LocalKafkaConsumerPoolFactory();
List<Broker> brokerList = pool.getKafkaConsumerPool().getBrokerList();

ControllerKafkaTopics<String,String> topics = new ControllerKafkaTopics<>(curator, brokerList, new StringDecoder(null), new StringDecoder(null), String.class, String.class);
ControllerKafkaTopic<String,String> topic = topics.registerTopic("test_group", "test");

new StreamProcessor<String,String>(sc.sc(), pool, topic) {
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
```

## Scala example

```scala
val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamSingleTopic")
val sc = new SparkContext(sparkConf)
val curator = OffsetManager.createCurator("127.0.0.1:2181")
val pool = new LocalKafkaConsumerPoolFactory
val brokerList = pool.getKafkaConsumerPool.getBrokerList
val topics = new ControllerKafkaTopics[String, String](curator, brokerList, new StringDecoder(), new StringDecoder(), classOf[String], classOf[String])
val topic = topics.registerTopic("test_group", "test")

new StreamProcessor[String, String](sc, pool, topic) {
  final def process() {
    val rdd: RDD[(String, String)] = fetch()

    rdd.foreachPartition { partitionData =>
      partitionData.foreach { record =>
        logger.info("key=" + record._1 + " val=" + record._2)
      }
    }

    commit()
  }
}.run()

sc.stop()
```

## TODO:

- Message handler parameter for decoding K, V (to enable the developer to include Topic & Partition information)
- Automatic partition resize detection
- Improve Leader Broker checker to use only a single thread.
- Custom offset storage (HBase, Mecache/Redis, MapDB - for performance junkies)
- Offset Management Utility
- Ability to store data in KafkaRDD

## License

This code is distributed using the Apache license, Version 2.0.

## Author

- Jeoffrey Lim