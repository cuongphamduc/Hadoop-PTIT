import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;


public class logisMap extends Mapper<LongWritable, Text, Text, FloatWritable> {

    public static int count = 0;
    public static int num_features = 0;
    public static float lr = 0.0f;
    public static Float[] Xi = null;
    public static ArrayList<Float> theta_i = new ArrayList<Float>();

    @Override
    public void setup(Context context) {
        lr = context.getConfiguration().getFloat("lr", 0.0f);
        num_features = context.getConfiguration().getInt("numfe", 0);
        Xi = new Float[num_features + 1];
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Context context) throws IOException {
        ++count;
        String[] s = value.toString().split("\\,");

        theta_i.add(0.0f);

        if (count == 1) {
            for (int i = 1; i < s.length; i++) {
                theta_i.add(context.getConfiguration().getFloat("theta".concat(String.valueOf(i)), 0.0f));
            }
        }

        for (int i = 1; i < Xi.length; i++) {
            Xi[i] = Float.parseFloat(s[i]);
        }

        float sum = 0.0f;

        for (int i = 1; i < Xi.length; i++) {
            sum += (Xi[i] * theta_i.get(i));
        }

        float predict = (float) (1 / (1 + (Math.exp(-sum))));

        float Yi = Float.parseFloat(s[0]);

        if ((int) Yi == -1) {
            Yi = 0.0f;
        }

        for (int i = 1; i < Xi.length; i++) {
            float tmp = theta_i.get(i);
            theta_i.remove(i);
            theta_i.add(i, tmp + lr * (Yi - predict) * (Xi[i]));
        }

        for (int i = 1; i < theta_i.size(); i++) {
            output.collect(new Text("theta".concat(String.valueOf(i))), new FloatWritable(theta_i.get(i)));
        }
    }
}
