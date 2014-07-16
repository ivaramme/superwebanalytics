superwebanalytics
=================
This is a project implemented with Big Data book (http://www.manning.com/marz/)

# Testing topology:
Download and install kafka: http://kafka.apache.org/downloads.html

- Start zookeeper: ```<ZOOKEEPER_HOME>/bin/zkServer.sh start-foreground ../conf/zoo_sample.cfg```
- Start kafka: ```<KAFKA_HOME>/bin/kafka-server-start.sh config/server.properties```
- Create the kafka topic: ```<KAFKA_HOME>/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <TOPIC>```
- Send messages using kafka: ```<KAFKA_HOME>/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <TOPIC>```

_Note:_ nice kafka reference guide: http://kafka.apache.org/documentation.html#quickstart

- Start topology and send messages