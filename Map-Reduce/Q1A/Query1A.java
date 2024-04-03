import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Query1A {

  public static class TokenizerMapper
        extends Mapper<Object, Text, Text, DoubleWritable>{
    private final static DoubleWritable REVENUE = new DoubleWritable();
    private Text store_name = new Text();
    private int date;

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // Get value of start and end date from configuration    
      Configuration conf = context.getConfiguration();
      final int start_date = Integer.parseInt(conf.get("start_date"));
      final int end_date = Integer.parseInt(conf.get("end_date"));

      // Tokenize input database file, splitting by line
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

      // For every line of input
      while (itr.hasMoreTokens()) {
        // Add a space to the end to ensure row would have at least 23 elements
        String token = itr.nextToken() + " ";
        // Split line by columns into an array
        String[] row = token.split("[|]");

        // Check that there is no null values in the required columns
        if (row[0].equals("") || row[7].equals("") || row[20].equals("")) {
            continue;
        } else {
            // Obtain necessary values from database line
            date = Integer.parseInt(row[0]);
            store_name.set("ss_store_sk_" + row[7]);
            REVENUE.set(Double.parseDouble(row[20]));

            // If date of transaction is between given date range, write out key-value
            // pair of ss_store_id and ss_net_paid
            if ((start_date <= date) && (date <= end_date)) {
                context.write(store_name, REVENUE);
            }
        }
      }
    }
  }

  public static class Combiner
        extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
        // Store total revenue from a single store
        double total_revenue = 0.0;

        for (DoubleWritable revenue: values) {
            // Add every transaction from the same store
            total_revenue += revenue.get();
        }
        context.write(key, new DoubleWritable(total_revenue));
    }
  }

  public static class FloatSumReducer
        extends Reducer<Text,DoubleWritable,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
      // Store total revenue from a single store
      double total_revenue = 0.0;

      for (DoubleWritable revenue : values) {
        // Add every transaction from the same store
        total_revenue += revenue.get();
      }
      context.write(key, new Text(Double.toString(total_revenue)));
    }
  }

  public static class SortMapper
        extends Mapper<Object, Text, DoubleWritable, Text>{
    private final static DoubleWritable REVENUE = new DoubleWritable();
    private Text store_name = new Text();


    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {   
      // Switch the key-value pair (key becomes value and vice versa)
      store_name.set(key.toString());
      REVENUE.set(Double.parseDouble(value.toString()));
      context.write(REVENUE, store_name);
    }
  }


  public static class TopKReducer
        extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {
    // Keep count of how many ouput lines as there will be a limit of K
    private int count = 0;
    private Text store_name = new Text();


    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
      // Get K from configuration
      Configuration conf = context.getConfiguration();
      final int K = Integer.parseInt(conf.get("K"));


      for (Text name : values) {
        store_name.set(name);


        // Ensure that output rows has not reach limit
        if (count < K) {
            context.write(store_name, key);
            count++;
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // Setup the first mapreduce job to get total revenue of each store
    Configuration total_revenue_conf = new Configuration();
    // Store the start and end date from input into configuration
    total_revenue_conf.set("start_date", args[1]);
    total_revenue_conf.set("end_date", args[2]);

    Job total_revenue = Job.getInstance(total_revenue_conf, "Get total revenue");
    total_revenue.setJarByClass(Query1A.class);
    total_revenue.setMapperClass(TokenizerMapper.class);
    total_revenue.setCombinerClass(Combiner.class);
    total_revenue.setReducerClass(FloatSumReducer.class);

    total_revenue.setOutputFormatClass(SequenceFileOutputFormat.class);
    total_revenue.setMapOutputKeyClass(Text.class);
    total_revenue.setMapOutputValueClass(DoubleWritable.class);
    total_revenue.setOutputKeyClass(Text.class);
    total_revenue.setOutputValueClass(Text.class);

    // Temporary directory for output of first job and input of second job
    Path tempPath = new Path("temporary/Query1A");
    FileInputFormat.addInputPath(total_revenue, new Path(args[3]));
    FileOutputFormat.setOutputPath(total_revenue, tempPath);

    // Run the first job
    total_revenue.waitForCompletion(true);

    // Setup the second mapreduce job to sort result based on specification
    Configuration sort_result_conf = new Configuration();
    // Store the K value from input into configuration
    sort_result_conf.set("K", args[0]);

    Job sort_result = Job.getInstance(sort_result_conf, "Sort result");
    sort_result.setJarByClass(Query1A.class);
    sort_result.setMapperClass(SortMapper.class);
    sort_result.setReducerClass(TopKReducer.class);

    sort_result.setInputFormatClass(SequenceFileInputFormat.class);
    sort_result.setMapOutputKeyClass(DoubleWritable.class);
    sort_result.setMapOutputValueClass(Text.class);
    sort_result.setOutputKeyClass(Text.class);
    sort_result.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(sort_result, tempPath);
    FileOutputFormat.setOutputPath(sort_result, new Path(args[4]));
    
    // Delete temporary directory
    tempPath.getFileSystem(sort_result_conf).deleteOnExit(tempPath);
    // Run second job then exit
    System.exit(sort_result.waitForCompletion(true) ? 0 : 1);
  }
}