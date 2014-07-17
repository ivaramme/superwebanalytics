package manning.bigdata.storm.bolts;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailStructure;
import com.google.common.reflect.TypeToken;
import manning.bigdata.ch3.DataPailStructure;
import manning.bigdata.swa.*;
import manning.bigdata.util.DataDecoder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by jagosan on 7/14/14.
 */
public class KafkaToHDFSBolt extends BaseRichBolt {

    private String path;
    private final PailStructure structure =  new DataPailStructure();
    private final BlockingQueue<String> queue;
    private OutputCollector collector;
    private Pail<Data>.TypedRecordOutputStream output = null;

    public KafkaToHDFSBolt(String path) {
        System.out.println("HDFSThriftBolt hdfsPath = " + path);
        this.path = path;
        this.queue = new LinkedBlockingDeque<String>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        newBucket();
    }
    /*
     * Create new Pail & output stream on 
     */
    private void newBucket(){
        if(output != null) {
            try {
                output.close();
            } catch (Exception e) {
                System.out.println("no output to close"); }}

        try {
            String dirName = DateTimeFormat.forPattern("yyyyMMddHHmm")
                    .print(new DateTime());
            Pail pail = Pail.create(path + "/" + dirName, structure, false);

            output = pail.openWrite();

        } catch(IOException ioe){
            System.out.println("Unable to use Pail to write data: " + ioe.getMessage());
            System.exit(0);
        }
    }

    @Override
    public void execute(Tuple input) {
        if(input.getSourceStreamId().equals( Constants.SYSTEM_TICK_STREAM_ID )) {
            newBucket();

        } else {
            System.out.println("Received new tuple");
            //Convert bytes to string:
            byte[] valueBytes = (byte[]) input.getValueByField("bytes");
            String message = new String(valueBytes);

            try {
                JSONParser parser = new JSONParser();
                Object obj = parser.parse(message);
                JSONObject jsonObject = (JSONObject) obj;
                String messageType = (String) jsonObject.get("messagetype");

                Data data = DataDecoder.decodeJsonMessage(jsonObject, messageType);

                output.writeObject(data);

            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            //Acknowledge receipt of tuple
            collector.ack(input);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void cleanup() {
        try{
            output.close();
        } catch(IOException ioe){
            System.out.println("Unable to close Pail: " + ioe.getMessage());
            ioe.printStackTrace();
            System.exit(0);
        }
    }

    /**
     * Inject tick tuple into the stream every
     * @return
     */
    @Override

    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }


}
