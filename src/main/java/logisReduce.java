import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class logisReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, OutputCollector<Text, FloatWritable> output) throws IOException {
        float sum = 0;
        int count = 0;
        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }
        output.collect(key, new FloatWritable(sum / count));
    }
}
