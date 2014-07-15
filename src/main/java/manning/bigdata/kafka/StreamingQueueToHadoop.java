package manning.bigdata.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 12.03.14
 * Time: 22:19
 * To change this template use File | Settings | File Templates.
 */
public class StreamingQueueToHadoop {
    public String hdfsPath;
    public String zookeeper;
    public String topic;
    public int threads;

    public StreamingQueueToHadoop(String hdfsPath, String zookeeper, String topic, int threads) {
        this.hdfsPath = hdfsPath;
        this.zookeeper = zookeeper;
        this.topic = topic;
        this.threads = threads;
    }

    public void startStreaming() {
        ConsumerConnector consumerConnector;
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);


        //TODO:  without new group, messages are not received.  believe it has to do with the message offset...
        //properties.put("group.id", "group42"); //-" + Math.random() );

        //properties.put("group.id", "group1");
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(threads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        int thread = 0;
        for (final KafkaStream stream : streams) {
            new Thread(new ReadKafkaQueueAndWriteToHadoop(stream, thread, hdfsPath)).start();
            thread++;
        }

    }
}
