import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
arr[0] : số lượng feature
arr[1] : learing rate
arr[2] : số lần lặp
arr[3] : input
arr[4] : output
 */

public class Run {
    public static int num_features;
    public static float lr;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        num_features = Integer.parseInt(args[0]);
        lr = Float.parseFloat(args[1]);

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Float[] theta = new Float[num_features + 1];

        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
            if (i == 0) {
                for (int j = 0; j < num_features; j++) {
                    theta[j] = (float) 0;
                }
            } else {
                int iter = 0;

                BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[4]))));
                String line1;

                while ((line1 = br1.readLine()) != null) {
                    String[] theta_line = line1.split("\t");
                    theta[iter] = Float.parseFloat(theta_line[1]);
                    iter++;
                }

                br1.close();
            }

            if (hdfs.exists(new Path(args[4]))) {
                hdfs.delete(new Path(args[4]), true);
            }

            hdfs.close();

            conf.setFloat("lr", lr);
            conf.setInt("numfe", num_features);

            for (int j = 1; j < num_features; j++) {
                conf.setFloat("theta".concat(String.valueOf(j)), theta[j]);
            }
            Job job = Job.getInstance(conf, "Calculation of Theta");
            job.setJarByClass(Run.class);

            FileInputFormat.setInputPaths(job, new Path(args[3]));
            FileOutputFormat.setOutputPath(job, new Path(args[4]));

            job.setMapperClass(logisMap.class);
            job.setReducerClass(logisReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            job.waitForCompletion(true);
        }
    }
}