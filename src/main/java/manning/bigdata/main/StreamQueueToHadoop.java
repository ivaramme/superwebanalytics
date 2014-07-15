package manning.bigdata.main;

import manning.bigdata.ch3.PailMove;
import manning.bigdata.kafka.StreamingNewDataToQueue;
import manning.bigdata.kafka.StreamingQueueToHadoop;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;

/**
 * Created by bela on 18.03.14.
 */
public class StreamQueueToHadoop {
    public static void main(String[] args) throws ParseException {
        String zookeeper = "localhost:2181";
        String topic = "swa";

        int threads = 3;

        String zookeeperURL;
        try {
            zookeeperURL = System.getenv("ZOOKEEPER_URL");
        } catch (Exception e) {
            throw new RuntimeException("Invalid zookeeper Path: " + e.toString());
        }

        String hdfsURL;
        try {
            hdfsURL = System.getenv("HDFS_URL");
        } catch (Exception e) {
            throw new RuntimeException("Invalid hdfs Path");
        }

        String kafkaURL;
        try {
            kafkaURL = System.getenv("KAFKA_URL");
        } catch (Exception e) {
            throw new RuntimeException("Invalid kafka Path");
        }

        String hdfsPath = hdfsURL + "/tmp/newData5";
        String kafkaPath = kafkaURL;

        StreamingNewDataToQueue streamingNewDataToQueue = new StreamingNewDataToQueue(kafkaPath, topic);
        String dateStart = "01.01.2014|10:20:20";
        String dateEnd = "01.01.2015|10:20:20";
        String batch = "";
        String factType = "";
        streamingNewDataToQueue.generateAndStreamingDataToQueue(dateStart, dateEnd, batch, factType);

        StreamingQueueToHadoop streaming = new StreamingQueueToHadoop(hdfsPath, zookeeperURL, topic, threads);
        streaming.startStreaming();

    }
}
