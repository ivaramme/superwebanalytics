package manning.bigdata.mapreduce;

import com.backtype.hadoop.pail.PailStructure;
import manning.bigdata.ch3.DataPailStructure;
import manning.bigdata.swa.Data;
import manning.bigdata.swa.PersonProperty;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ivaramme
 * Date: 8/13/14
 */
public class GenderMap extends MapReduceBase implements Mapper<Text, BytesWritable, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private Text gender = new Text();
    private int processedItems = 0;
    private final PailStructure structure =  new DataPailStructure();
    private Data data;
    private PersonProperty property;

    /**
     *
     * @param key
     * @param value the input value. Can be different from the output type
     * @param output collects mapped keys and values.
     * @param reporter reports status of the task
     * @throws java.io.IOException
     */
    public void map(Text key, BytesWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String _gender = "";
        try {
            // Deserialize raw data coming from HDFS using the structure
            data = (Data) structure.deserialize(value.getBytes());
            property = data.getDataunit().getPerson_property();

            System.out.println(property.getProperty().getSetField().getFieldName());
            System.out.println(property.getId());
            if(property.getProperty().isSetGender()) {
                _gender = data.getDataunit().getPerson_property().getProperty().getGender().getValue() == 1 ? "MALE" : "FEMALE";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if(0 == _gender.length())
            return;

        processedItems += 1;

        gender.set(_gender); // Set the String value to the 'TEXT' instance
        output.collect(gender, ONE);
    }
}
