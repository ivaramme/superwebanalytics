package manning.bigdata.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * User: ivaramme
 * Date: 8/6/14
 */
public class GenderReduce/*<K extends WritableComparable, V extends Writable>*/ extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     *
     * @param key
     * @param values collection of items to reduce
     * @param output collects mapped keys and combined values.
     * @param reporter reports status of the task (that is still alive)
     * @throws IOException
     */
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        System.out.println("Processing data for key: " + key);
        while (values.hasNext()) {
            // Increment the no. of values for this key
            sum += values.next().get();
            System.out.println(key + " -> " + sum);
        }

        output.collect(key, new IntWritable(sum));
    }
}
