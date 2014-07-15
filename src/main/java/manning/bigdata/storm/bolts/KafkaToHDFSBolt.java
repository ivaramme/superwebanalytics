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
    private Pail<Data>.TypedRecordOutputStream output;
    private AggregatorThread thread;

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

    private void newBucket(){
        try
        {
            output.close();
        }
        catch(Exception e)
        {
            System.out.println("no output to close");
        }
        try {

            String dirName = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .print(new DateTime());
            output = Pail.create(path + "/" + dirName, structure, false).openWrite();

        } catch(IOException ioe){
            System.out.println("Unable to use Pail to write data: " + ioe.getMessage());
            ioe.printStackTrace();
            System.exit(0);
        }
        //thread.workRemains = false;
        thread = new AggregatorThread(queue, output);
        thread.start();
    }

    @Override
    public void execute(Tuple input) {
        boolean flush = false;
        if(input.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID)) {
            flush = true;
        } else {
            System.out.println("Received new tuple");
            // Differ parsing and object creation to a separate thread
            queue.offer(input.getString(0));
            collector.ack(input);
        }
        // close, forcing write to disk:
        if(flush) try {
            System.out.println("flushing to hdfs");
            output.close();
            newBucket();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private Data decodeJsonMessage(JSONObject jsonObject, String messageType) {
        Data data = null;

        if (messageType.equals("page")) {
            data = decodePage(jsonObject);
        } else if (messageType.equals("person")) {
            data = decodePerson(jsonObject);
        } else if (messageType.equals("equiv")) {
            data = decodeEquiv(jsonObject);
        } else if (messageType.equals("pageview")) {
            data = decodePageView(jsonObject);
        }
        return data;
    }

    private Data decodePage(JSONObject jsonObject) {
        String pedigree = (String) jsonObject.get("pedigree");
        String url = (String) jsonObject.get("url");
        return null;
    }

    private Data decodePerson(JSONObject jsonObject) {
        System.out.println(jsonObject);
        String pedigree = (String) jsonObject.get("pedigree");
        String personId = (String) jsonObject.get("personid");
        String gender = (String) jsonObject.get("gender");

        GenderType genderType = null;
        if (gender.equals("MALE")) {
            genderType = GenderType.MALE;
        } else {
            genderType = GenderType.FEMALE;
        }

        String fullname = (String) jsonObject.get("fullname");
        String city = (String) jsonObject.get("city");
        String state = (String) jsonObject.get("state");
        String country = (String) jsonObject.get("country");

        PersonID personID = new PersonID();
        if (personId.startsWith("cookie")) {
            personID.setCookie(personId);
        } else {
            personID.setUser_id(Long.parseLong(personId));
        }

        PersonPropertyValue personPropertyValue = new PersonPropertyValue();
        Location location = new Location();
        location.setCity(city);
        location.setState(state);
        location.setCountry(country);
        personPropertyValue.setGender(genderType);
        personPropertyValue.setLocation(location);
        personPropertyValue.setFull_name(fullname);

        PersonProperty personProperty = new PersonProperty();
        personProperty.setProperty(personPropertyValue);
        personProperty.setId(personID);

        DataUnit dataUnit = new DataUnit();
        dataUnit.setPerson_property(personProperty);

        return getData(pedigree, dataUnit);
    }

    private Data getData(String timestamp, DataUnit dataUnit) {
        Pedigree pedigree = new Pedigree();
        pedigree.setTrue_as_of_secs(Integer.parseInt(timestamp));

        Data data = new Data();
        data.setPedigree(pedigree);
        data.setDataunit(dataUnit);

        return data;
    }

    private Data decodeEquiv(JSONObject jsonObject) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private Data decodePageView(JSONObject jsonObject) {
        return null;
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

    class AggregatorThread extends Thread {
        private final BlockingQueue<String> queue;
        private final Pail<Data>.TypedRecordOutputStream output;
        public Boolean workRemains = true;

        public AggregatorThread(BlockingQueue<String> queue, Pail<Data>.TypedRecordOutputStream output) {
            this.queue = queue;
            this.output = output;
        }

        public void run() {
            String message = "";
            Type stringStringMap = new TypeToken<Map<String, Object>>() {}.getType();

            // block until there are messages available
            while (workRemains) {
                try {
                    message = queue.take();
                    System.out.println("Processing new message: " + message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    writeToHadoop(message);
                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
        private void writeToHadoop(String jsonMessages) throws ParseException, IOException {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(jsonMessages);
            JSONObject jsonObject = (JSONObject) obj;
            String messageType = (String) jsonObject.get("messagetype");

            Data data = decodeJsonMessage(jsonObject, messageType);
            output.writeObject(data);

        }
    }
}
