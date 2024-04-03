import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ReduceJoin {
    // Mapper for store table
    public static class StoreMapper
        extends Mapper<Object, Text, IntWritable, Text>{
        private final static IntWritable STORE_ID = new IntWritable();
        private Text FLOOR_SPACE = new Text();

        @Override
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            // Tokenize store table, splitting by line    
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            // For every line of store table
            while (itr.hasMoreTokens()) {
                // Add a space to the end to ensure row would have at least 28 elements
                String token = itr.nextToken() + " ";
                // Split line by columns into an array
                String[] row = token.split("[|]");

                // Check that there is no null values in the required columns
                if (row[0].equals("") || row[7].equals("")) {
                    continue;
                } else {
                    // Obtain necessary values from database line
                    STORE_ID.set(Integer.parseInt(row[0]));
                    // Added FLR identifier for reducer to identify item is from store table
                    FLOOR_SPACE.set("FLR  " + row[7]);
                    // Write out store ID and its corresponding floor space as key-value pair
                    context.write(STORE_ID, FLOOR_SPACE);
                }
            }
        }
    }

    // Mapper for store sales table
    public static class StoreSalesMapper
        extends Mapper<Object, Text, IntWritable, Text>{
        private final static IntWritable STORE_ID = new IntWritable();
        private Text NET_PAID = new Text();
        int date;

        @Override
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException { 
            // Get value of start and end date from configuration
            Configuration conf = context.getConfiguration();
            int start_date = Integer.parseInt(conf.get("start_date"));
            int end_date = Integer.parseInt(conf.get("end_date")); 

            // Tokenize store sales table, splitting by line    
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            // For every line of store sales table
            while (itr.hasMoreTokens()) {
                // Add a space to the end to ensure row would have at least 23 elements
                String token = itr.nextToken() + " ";
                String[] row = token.split("[|]");

                // Check that there is no null values in the required columns
                if (row[0].equals("") || row[7].equals("") || row[20].equals("")) {
                    continue;
                } else {
                    // Obtain necessary values from database line
                    date = Integer.parseInt(row[0]);
                    STORE_ID.set(Integer.parseInt(row[7]));
                    // Added NET identifier for reducer to identify item is from store sales table
                    NET_PAID.set("NET  " + row[20]);
                    // If date of transaction is between given date range, write out
                    // key-value pair of ss_store_id and ss_net_paid
                    if ((start_date <= date) && (date <= end_date)) {
                        context.write(STORE_ID, NET_PAID);
                    }
                }
            }
        }
    }

    // Reducer to join values from store and store sales table
    public static class JoinReducer
        extends Reducer<IntWritable,Text,Text,Text> {
        
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
            // Keeps total net paid for the specific store
            Double total_net_paid = 0.0;
            // Keeps floor space value for the specific store
            Text floor_space = new Text();

            for (Text value : values) {
                // Separate value to identifier and column value
                String[] row = value.toString().split("  ");
                if (row[0].equals("FLR")) {
                    // If object is from store table, keep the value of its floor space
                    floor_space.set(row[1]);
                } else {
                    // If object is from store sales table, add its net paid value to total_net_paid
                    total_net_paid += Double.parseDouble(row[1]);
                }
            }
            // Write out the joined values from store and store sales table on store ID in the order of
            // (key: total net paid, value: floor space, store ID)
            context.write(new Text(Double.toString(total_net_paid)), new Text(floor_space + "," + Integer.toString(key.get())));
        }
    }

    public static class SortMapper
        extends Mapper<Text, Text, DoubleWritable, Text>{
        @Override
        public void map(Text key, Text value, Context context
                        ) throws IOException, InterruptedException {  
            // Negate the value of total net paid to sort key in descending order
            Double total_net_paid = (-1 * Double.parseDouble(key.toString()));
            context.write(new DoubleWritable(total_net_paid), value);
        }
    }

    public static class SortReducer
        extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
        // Keep count of how many ouput lines as there will be a limit of K
        int count = 0;

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
            // Get K from configuration
            Configuration conf = context.getConfiguration();
            int K = Integer.parseInt(conf.get("K"));
            // Tree map to store floor spaces and store ID (key: floor space, value: store ID)
            // to sort stores with equal value of total net paid in ascending order of floor space
            Map<Integer, String> floor_spaces = new TreeMap<Integer, String>();

            // Ensure output line has not reach limit
            if (count < K) {
                for (Text value : values) {
                    // Obtain store details
                    String[] row = value.toString().split(",");
                    // Add floor space and store ID into tree map
                    floor_spaces.put(Integer.parseInt(row[0]), "ss_store_sk_" + row[1]);
                }

                for (int value: floor_spaces.keySet()) {
                    // Ensure output line has not reach limit
                    if (count < K) {
                        Text DATA = new Text(Integer.toString(value) + ", " + floor_spaces.get(value));
                        // Write out key-value pair of (key: total net paid value: floor space, store ID)
                        // Also remove negated value of total net paid
                        context.write(new DoubleWritable(-1 * key.get()), DATA);
                        count++;
                    }
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Setup first mapreduce job to join the store and store sales table using a reducer. For every store,
        // store its floor space and total sales
        Configuration reduce_join_conf = new Configuration();
        // Store start and end date from input in the configuration
        reduce_join_conf.set("start_date", args[1]);
        reduce_join_conf.set("end_date", args[2]);

        Job reduce_join = Job.getInstance(reduce_join_conf, "Reduce side join");
        reduce_join.setJarByClass(ReduceJoin.class);
        reduce_join.setReducerClass(JoinReducer.class);

        reduce_join.setOutputFormatClass(SequenceFileOutputFormat.class);
        reduce_join.setMapOutputKeyClass(IntWritable.class);
        reduce_join.setMapOutputValueClass(Text.class);
        reduce_join.setOutputKeyClass(Text.class);
        reduce_join.setOutputValueClass(Text.class);

        // Temporary directory for output of first job and input of second job
        Path tempPath = new Path("temporary/Query2");
        // Sets the different mapper to its input: StoreMapper for stores table and StoreSalesMapper for store sales table
        MultipleInputs.addInputPath(reduce_join, new Path(args[3]), TextInputFormat.class, StoreSalesMapper.class);
        MultipleInputs.addInputPath(reduce_join, new Path(args[4]), TextInputFormat.class, StoreMapper.class);
        FileOutputFormat.setOutputPath(reduce_join, tempPath);

        // Run the first job
        reduce_join.waitForCompletion(true);

        // Setup the second mapreduce job to sort and output result based on specification
        Configuration sort_result_conf = new Configuration();
        // Store the K value from input into configuration
        sort_result_conf.set("K", args[0]);

        Job sort_result = Job.getInstance(sort_result_conf, "Sort join result");
        sort_result.setJarByClass(ReduceJoin.class);
        sort_result.setMapperClass(SortMapper.class);
        sort_result.setReducerClass(SortReducer.class);
        sort_result.setOutputKeyClass(DoubleWritable.class);
        sort_result.setOutputValueClass(Text.class);
        sort_result.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(sort_result, tempPath);
        FileOutputFormat.setOutputPath(sort_result, new Path(args[5]));

        // Delete temporary directory
        tempPath.getFileSystem(sort_result_conf).deleteOnExit(tempPath);
        // Run second job then exit
        System.exit(sort_result.waitForCompletion(true) ? 0 : 1);
    }
}

