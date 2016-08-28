# Maelstrom

Maelstrom is an open source Kafka integration with Spark that is designed to be developer friendly, high performance
(millisecond stream processing), scalable (consumes messages at Spark worker nodes), and is extremely reliable.

This library has been running stable in production environment and has been proven to be resilient to numerous
production issues.  

Thanks to [Adlogica](http://www.adlogica.com/)  for sharing to the open source community this project!

## Features

- Simple framework which follows Kafka semantics and best practices.
- High performance, with latencies down to milliseconds.
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
        <version>0.2</version>
    </dependency>
```

## Kafka Consumer on Executors

The KafkaConsumerPoolFactory gets serialized over the wire to the Spark executors and cache Kafka Consumers to
establish a persistent connection to Kafka brokers. Future version of Maelstrom is to make these Kafka Consumers
remain to a specific Spark executor to as much as possible: all registered Consumer Group + Topics + Partitions 
properly distributed to available Spark executors (Worker + Number of Executors).


| Config          | Description                                                                      | Default   |
|-----------------|----------------------------------------------------------------------------------|-----------|
| brokers         | The Kafka Broker List                                                            | n/a       |
| keyMapper       | Class for decoding Keys                                                          | n/a       |
| valueMapper     | Class for decoding Values                                                        | n/a       |
| maxSize         | Make this at least double the amount of all Consumer Group + Topics + Partitions | 1000      |
| expireAfterMins | Kafka Consumer that is unused will expire after the defined minutes              | 5 minutes |

## Java example

```java
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
```

## Scala example

```scala
val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamSingleTopic")
val sc = new SparkContext(sparkConf)
val curator = OffsetManager.createCurator("127.0.0.1:2181")
val poolFactory = new KafkaConsumerPoolFactory[String, String]("127.0.0.1:9092", classOf[StringDecoder], classOf[StringDecoder])
val topics = new ControllerKafkaTopics[String, String](sc, curator, poolFactory)
val topic = topics.registerTopic("test_group", "test")

new StreamProcessor[String, String](topic) {
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

## Example of a Multi-Topic + Multi Processor with throttling

One stream processor consumes on *test* topic with maximum 1000 records per mini-batch.
Second stream processor consumes on *test2* topic with maximum 500 records per mini-batch.

```scala
val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamMultiTopicMultiProc")
val sc = new SparkContext(sparkConf)
val curator = OffsetManager.createCurator("127.0.0.1:2181")
val poolFactory = new KafkaConsumerPoolFactory[String, String]("127.0.0.1:9092", classOf[StringDecoder], classOf[StringDecoder])
val topics = new ControllerKafkaTopics[String, String](sc, curator, poolFactory)

new ProcessorRunner().addProcessor(new StreamProcessor[String, String](topics.registerTopic("test_multi_proc", "test", 1000)) {
  final def process() {
    val rdd: RDD[(String, String)] = fetch()

    rdd.foreachPartition { partitionData =>
      partitionData.foreach { record =>
        logger.info("key=" + record._1 + " val=" + record._2)
      }
    }

    commit()
  }
}).addProcessor(new StreamProcessor[String, String](topics.registerTopic("test_multi_proc", "test2", 500)) {
  final def process() {
    val rdd: RDD[(String, String)] = fetch()

    rdd.foreachPartition { partitionData =>
      partitionData.foreach { record =>
        logger.info("key=" + record._1 + " val=" + record._2)
      }
    }

    commit()
  }
}).start()

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