package manning.bigdata.main;

import manning.bigdata.ch3.PailMove;
import manning.bigdata.kafka.StreamingNewDataToQueue;
import manning.bigdata.kafka.StreamingQueueToHadoop;

import java.text.ParseException;

/**
 * Created by bela on 18.03.14.
 */
public class StreamQueueToHadoop {
    public static void main(String[] args) throws ParseException {
        String zookeeper = "localhost:2181";
        String topic = "swa";

        int threads = 3;
        String hdfsPath = "hdfs://10.200.1.100:9000/tmp/newData5"; //PailMove.NEW_DATA_LOCATION;

//copy from StreamDataToQueue.main()

        String kafkaServer = "localhost:9092";

        StreamingNewDataToQueue streamingNewDataToQueue = new StreamingNewDataToQueue(kafkaServer, topic);
        String dateStart = "01.01.2014|10:20:20";
        String dateEnd = "01.01.2015|10:20:20";
        String batch = "";
        String factType = "";
        streamingNewDataToQueue.generateAndStreamingDataToQueue(dateStart, dateEnd, batch, factType);

        StreamingQueueToHadoop streaming = new StreamingQueueToHadoop(hdfsPath, zookeeper, topic, threads);
        streaming.startStreaming();

    }
}
