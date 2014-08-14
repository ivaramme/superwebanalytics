package manning.bigdata.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 11.03.14
 * Time: 23:04
 * To change this template use File | Settings | File Templates.
 */
public class StreamingNewDataToQueue {
    private String kafkaServer;
    private String topic;
    private Long timestampStart;
    private Long timestampEnd;
    private String batch;
    private String factType;
    private Producer<String, String> producer;
    private int personCounter = 0;

    public StreamingNewDataToQueue(String kafkaServer, String topic) {
        this.kafkaServer = kafkaServer;
        this.topic = topic;
    }

    public void generateAndStreamingDataToQueue(String _dateStart, String _dateEnd, String batch, String factType) throws ParseException {
        setupKafka();

        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy|hh:mm:ss");
        Date dateStart = sdf.parse(_dateStart);
        Date dateEnd = sdf.parse(_dateEnd);
        this.timestampStart = dateStart.getTime() / 1000;
        this.timestampEnd = dateEnd.getTime() / 1000;

        new Thread( new Runnable(){
                @Override
                public void run() {
                    while (true) {
                        List<String> facts = new ArrayList<String>();

                        facts = genJSONFacts(timestampStart);
                        for (String fact : facts) {
                            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, fact);

                            producer.send(data);
                        }
                        timestampStart += 1000;
                        try {
                            Thread.sleep(1000);
                        }catch (Exception e) {
                            //Logger.getLogger()
                            System.out.println(e.getStackTrace());
                        }
                    }
                }
            }

        ).start();

    }

    private List<String> genJSONFacts(long timestampStart) {

        List<String> messages = new ArrayList<String>();
        int personCount = 1;
        messages.addAll(genJSONPersons(timestampStart, personCount));

        return messages;
    }

    private List<String> genJSONPersons(long timestampStart, int personCount) {
        List<String> people = new ArrayList<String>();
        String gender[] = { "MALE", "FEMALE" };
        Random random = new Random();

        JSONObject jsonObject = new JSONObject();

        for (int i = 1; i <= personCount; i++) {
            jsonObject.put("messagetype", "person");
            jsonObject.put("pedigree", Long.toString(timestampStart));
            jsonObject.put("personid", "cookie_" + personCounter);
            jsonObject.put("gender", gender[random.nextInt(2)]);
            jsonObject.put("fullname", "Piotr Belina " + random.nextInt(5));
            jsonObject.put("city", "Sopot");
            jsonObject.put("state", "Sopot");
            jsonObject.put("country", "Poland");

            people.add(jsonObject.toJSONString());
            jsonObject.clear();

            jsonObject.put("messagetype", "person");
            jsonObject.put("pedigree", Long.toString(timestampStart));
            jsonObject.put("personid", "" + personCounter);
            jsonObject.put("gender", gender[random.nextInt(2)]);
            jsonObject.put("fullname", "Piotr Belina");
            jsonObject.put("city", "Sopot");
            jsonObject.put("state", "Sopot");
            jsonObject.put("country", "Poland");

            people.add(jsonObject.toJSONString());
            jsonObject.clear();

            timestampStart++;
            personCounter++;
        }

        return people;
    }

    private void setupKafka() {
        Properties props = new Properties();

        props.put("metadata.broker.list", this.kafkaServer);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);



    }


}
