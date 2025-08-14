import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    // Mapper Class
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int m = 2; // rows in Matrix A
        private int n = 2; // columns in Matrix A / rows in Matrix B
        private int p = 2; // columns in Matrix B

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Each line format: MatrixName i j value
            // Example: A 0 1 5  => A[0][1] = 5
            String[] parts = value.toString().split("\\s+");
            String matrixName = parts[0];
            int i = Integer.parseInt(parts[1]);
            int j = Integer.parseInt(parts[2]);
            int val = Integer.parseInt(parts[3]);

            if (matrixName.equals("A")) {
                // Emit for all columns of B
                for (int k = 0; k < p; k++) {
                    context.write(new Text(i + "," + k), new Text("A," + j + "," + val));
                }
            } else if (matrixName.equals("B")) {
                // Emit for all rows of A
                for (int k = 0; k < m; k++) {
                    context.write(new Text(k + "," + j), new Text("B," + i + "," + val));
                }
            }
        }
    }

    // Reducer Class
    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Integer> aMap = new HashMap<>();
            Map<Integer, Integer> bMap = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String matrixName = parts[0];
                int index = Integer.parseInt(parts[1]);
                int valueInt = Integer.parseInt(parts[2]);

                if (matrixName.equals("A")) {
                    aMap.put(index, valueInt);
                } else if (matrixName.equals("B")) {
                    bMap.put(index, valueInt);
                }
            }

            int sum = 0;
            for (int j : aMap.keySet()) {
                if (bMap.containsKey(j)) {
                    sum += aMap.get(j) * bMap.get(j);
                }
            }

            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    // Main method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MatrixMultiplication <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}