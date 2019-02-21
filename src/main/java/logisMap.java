import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;


public class logisMap extends Mapper<LongWritable, Text, Text, FloatWritable> {

    public static int count = 0;
    public static float lr = 0.0f;
    public static Float[] Xi = null;
    public static ArrayList<Float> theta_i = new ArrayList<Float>();

    @Override
    public void setup(Context context) {
        lr = context.getConfiguration().getFloat("lr", 0);
    }

    public void map(LongWritable key, Text value, Context context) {
        ++count;
        float predic = 0;
        String[] temp = value.toString().split("\\,");

        if (count == 1) {
            for (int i = 1; i < temp.length; i++) {
                theta_i.add(context.getConfiguration().getFloat("theta".concat(String.valueOf(i)), 0));
            }

            Xi = new Float[temp.length];
        }

        for (int i = 1; i < Xi.length; i++) {
            Xi[i] = Float.parseFloat(temp[i]);
        }

        float exp = 0;

        for (int i = 1; i < Xi.length; i++) {
            exp += (Xi[i] * theta_i.get(i));
        }

        predic = (float) (1 / (1 + (Math.exp(-exp))));

        float Yi = Float.parseFloat(temp[0]);

        for (int i = 1; i < Xi.length; i++) {
            float tmp = theta_i.get(i);
            theta_i.remove(i);
            theta_i.add(i, tmp + lr * (Yi - predic) * (Xi[i]));
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 1; i < theta_i.size(); i++) {
            context.write(new Text("theta" + i), new FloatWritable(theta_i.get(i)));
        }
    }
}
