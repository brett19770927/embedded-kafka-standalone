package com.ms.settlements.safe3.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class EmbeddedKafkaServer {

  private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaServer.class);
  private static final int DEFAULT_BROKER_PORT = 9092; // pick a random port
  private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";

  private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "60000";
  private static final String KAFKASTORE_DEBUG = "true";
  private static final String KAFKASTORE_INIT_TIMEOUT = "90000";

  private EmbeddedZooKeeper zookeeper;
  private EmbeddedKafka broker;
  private final Properties brokerConfig;
  private boolean running;

  public EmbeddedKafkaServer() {
    this(new Properties());
  }

  public EmbeddedKafkaServer(final Properties brokerConfig) {
    this.brokerConfig = new Properties();
    this.brokerConfig.putAll(brokerConfig);
  }

  public void start() throws Exception {
    log.debug("Initiating Embedded Kafka Server startup");
    log.debug("Starting a ZooKeeper instance...");
    zookeeper = new EmbeddedZooKeeper();
    log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

    final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
    log.debug("Starting a Kafka instance on port {} ...",
      effectiveBrokerConfig.getProperty(KafkaConfig.ListenersProp()));
    broker = new EmbeddedKafka(effectiveBrokerConfig);
    log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
      broker.brokerList(), broker.zookeeperConnect());

    running = true;
  }

  private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final EmbeddedZooKeeper zookeeper) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.putAll(brokerConfig);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
    effectiveConfig.put(KafkaConfig.ListenersProp(), String.format("PLAINTEXT://127.0.0.1:%s", DEFAULT_BROKER_PORT));
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
    effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    return effectiveConfig;
  }

  public void stop() {
    log.info("Stopping Embedded Kafka Server");
    try {
      if (broker != null) {
        broker.stop();
      }
      try {
        if (zookeeper != null) {
          zookeeper.stop();
        }
      } catch (final IOException fatal) {
        throw new RuntimeException(fatal);
      }
    } finally {
      running = false;
    }
    log.info("Embedded Kafka Server Stopped");
  }

  public String bootstrapServers() {
    return broker.brokerList();
  }

  public void createTopic(final String topic) throws InterruptedException {
    createTopic(topic, 1, (short) 1, Collections.emptyMap());
  }

  public void createTopic(final String topic, final int partitions, final short replication) throws InterruptedException {
    createTopic(topic, partitions, replication, Collections.emptyMap());
  }

  public void createTopic(final String topic,
    final int partitions,
    final short replication,
    final Map<String, String> topicConfig
  ) throws InterruptedException {
    broker.createTopic(topic, partitions, replication, topicConfig);
  }

  public boolean isRunning() {
    return running;
  }

  public static void main(String[] args) throws Exception {
      var kafkaCluster = new EmbeddedKafkaServer();
      kafkaCluster.start();
      kafkaCluster.createTopic("brett-test");
  }
}
