package manning.bigdata.storm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import manning.bigdata.storm.bolts.KafkaToHDFSBolt;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;



/**
 * Created by jagosan on 7/11/14.
 */
public class StormTopology {
    private static TopologyBuilder builder = new TopologyBuilder();
    private static String zookeeperURL;
    private static String hdfsURL;
    private static String kafkaURL;

    public static void main (String[] args){

        try {
            zookeeperURL = System.getenv("ZOOKEEPER_URL");
        } catch (Exception e) {
            throw new RuntimeException("Invalid zookeeper Path: " + e.toString());
        }

        try {
            hdfsURL = System.getenv("HDFS_URL");
        } catch (Exception e) {
            throw new RuntimeException("Invalid hdfs Path");
        }

        try {
            kafkaURL = System.getenv("KAFKA_URL");
        } catch (Exception e) {
            throw new RuntimeException("Invalid kafka Path");
        }

        BrokerHosts brokerHosts = new ZkHosts(zookeeperURL);
        SpoutConfig kSpoutConf = new SpoutConfig(brokerHosts, "swa", "/var/lib/zookeeper", "storm-test");

        builder.setSpout("source", new KafkaSpout(kSpoutConf));
        builder.setBolt("sink", new KafkaToHDFSBolt(hdfsURL + "/tmp/storm-test")).shuffleGrouping("source");

        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        cluster.submitTopology("pageview-test", conf, builder.createTopology());
        System.out.println("Storm cluster started");

    }
}