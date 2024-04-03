import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MapJoin {
    
    public static class JoinMapper
        extends Mapper<Object, Text, IntWritable, Text>{
        // Hashmap accessible by all mapper to store the value of store IDs and its floor space
        private Map<Integer, String> store_table = new HashMap<Integer, String>();
        private int STORE_ID;
        private Text DETAILS = new Text();
        private int date;

        // Setup function to read through store table and record the store ID and floor space columns
        @Override
        public void setup(Context context
                        ) throws IOException, InterruptedException{
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
                // Get the store table
                File store = new File("./store");

                // Read every line of the store table
                try (BufferedReader br =  new BufferedReader(new FileReader(store))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Split line by columns into an array
                        String[] row = line.split("[|]");
                        // Check that there is no null values in the required columns
                        if (row[0].equals("") || row[7].equals("")) {
                            continue;
                        } else {
                            // Add store ID and its floor space into the hashmap
                            store_table.put(Integer.parseInt(row[0]), row[7]);
                        }
                    }
                }
            }
        }

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
                // Split line by columns into an array
                String[] row = token.split("[|]");

                // Check that there is no null values in the required columns
                if (row[0].equals("") || row[7].equals("") || row[20].equals("")) {
                    continue;
                } else {
                    // Obtain necessary values from database line
                    date = Integer.parseInt(row[0]);
                    STORE_ID = Integer.parseInt(row[7]);

                    // If date of transaction is between given date range, write out
                    // key-value pair of ss_store_id and the details (s_floor_space and ss_net_paid)
                    if ((start_date <= date) && (date <= end_date)) {
                        DETAILS.set(store_table.get(STORE_ID) + "," + row[20]);
                        context.write(new IntWritable(STORE_ID), DETAILS);
                    }
                }
            }
        }
    }

    public static class StoreCombiner
        extends Reducer<IntWritable,Text,IntWritable,Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
            // Store total revenue from a single store
            Double total_net_paid = 0.0;
            // Record the floor space of the store
            Text floor_space = new Text();

            for (Text value : values) {
                // Obtain item details (floor space, net paid)
                String[] row = value.toString().split(",");
                floor_space.set(row[0]);
                // Add net paid to total
                total_net_paid += Double.parseDouble(row[1]);
            }
            // Write out the key-value pair of (key: store ID, value: floor space, total net paid)
            context.write(key, new Text(floor_space + "," + Double.toString(total_net_paid)));
        }
    }

    public static class StoreReducer
        extends Reducer<IntWritable,Text,Text,Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
            // Store total revenue from a single store
            Double total_net_paid = 0.0;
            // Record the floor space of the store
            Text floor_space = new Text();

            for (Text value : values) {
                // Obtain item details (floor space, net paid)
                String[] row = value.toString().split(",");
                floor_space.set(row[0]);
                // Add net paid to total
                total_net_paid += Double.parseDouble(row[1]);
            }
            // Write out the key-value pair of (key: total net paid value: floor space, store ID)
            context.write(new Text(Double.toString(total_net_paid)), new Text(floor_space + "," + Integer.toString(key.get())));
        }
    }

    public static class SortMapper
        extends Mapper<Text, Text, DoubleWritable, Text>{
        @Override
        public void map(Text key, Text value, Context context
                        ) throws IOException, InterruptedException {  
            // Negate the value of total net paid to sort key in descending order
            Double total_net_paid = -1 * Double.parseDouble(key.toString());
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
        // Setup first mapreduce job to join the store and store sales table using a mapper. For every store,
        // store its floor space and total sales
        Configuration map_join_conf = new Configuration();
        // Store start and end date from input in the configuration
        map_join_conf.set("start_date", args[1]);
        map_join_conf.set("end_date", args[2]);

        Job map_join = Job.getInstance(map_join_conf, "Map side join");
        map_join.setJarByClass(MapJoin.class);
        map_join.setMapperClass(JoinMapper.class);
        map_join.setCombinerClass(StoreCombiner.class);
        map_join.setReducerClass(StoreReducer.class);
        map_join.addCacheFile(new URI(args[4] + "#store"));

        map_join.setOutputFormatClass(SequenceFileOutputFormat.class);
        map_join.setMapOutputKeyClass(IntWritable.class);
        map_join.setMapOutputValueClass(Text.class);
        map_join.setOutputKeyClass(Text.class);
        map_join.setOutputValueClass(Text.class);

        // Temporary directory for output of first job and input of second job
        Path tempPath = new Path("temporary/MapJoin");
        FileInputFormat.addInputPath(map_join, new Path(args[3]));
        FileOutputFormat.setOutputPath(map_join, tempPath);

        // Run the first job
        map_join.waitForCompletion(true);

        // Setup the second mapreduce job to sort and output result based on specification
        Configuration sort_result_conf = new Configuration();
        // Store the K value from input into configuration
        sort_result_conf.set("K", args[0]);

        Job sort_result = Job.getInstance(sort_result_conf, "sort result");
        sort_result.setJarByClass(MapJoin.class);
        sort_result.setMapperClass(SortMapper.class);
        sort_result.setReducerClass(SortReducer.class);

        sort_result.setInputFormatClass(SequenceFileInputFormat.class);
        sort_result.setOutputKeyClass(DoubleWritable.class);
        sort_result.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sort_result, tempPath);
        FileOutputFormat.setOutputPath(sort_result, new Path(args[5]));

        // Delete temporary directory
        tempPath.getFileSystem(sort_result_conf).deleteOnExit(tempPath);
        // Run second job then exit
        System.exit(sort_result.waitForCompletion(true) ? 0 : 1);
    }
}