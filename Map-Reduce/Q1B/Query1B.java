import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Query1B {
    
    public static class SalesMapper extends Mapper<Object, Text, Text, Text> {
        private String QUANTITY;
        private String item_id;
        private Text store_name = new Text();
        private Text DETAILS = new Text();
        private int date;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
                if (row[0].equals("") || row[2].equals("") || row[7].equals("") || row[10].equals("")) {
                    continue;
                } else {
                    // Obtain necessary values from database line
                    date = Integer.parseInt(row[0]);
                    item_id = "ss_item_sk_" + row[2];
                    store_name.set(row[7]);
                    QUANTITY = row[10];
                    // Store item ID and quantity separated by comma to be the value of
                    // output
                    DETAILS.set(item_id + "," + QUANTITY);

                    // If date of transaction is between given date range, write out
                    // key-value pair of ss_store_id and the details (item Id and quantity)
                    if ((start_date <= date) && (date <= end_date)) {
                        context.write(store_name, DETAILS);
                    }
                }
            }
        }
    }
    
    public static class SalesReducer extends Reducer<Text, Text, Text, Text> {
        private Text DETAILS = new Text();
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Keeps track of total store sales (of all items)
            int total_quantity = 0;
            // Hashmap to store total quantity sold per item in the store (key: item ID, value: quantity)
            Map<String, Integer> item_sales_map = new HashMap<String, Integer>();

            for (Text value:values) {
                // Obtain item details (item ID, quantity)
                String[] item_details = value.toString().split(",");
                String item_id = item_details[0];
                int item_quantity = Integer.parseInt(item_details[1]);
                if (item_sales_map.containsKey(item_id)) {
                    // If item has been stored previously, add total item quantity by the current quantity
                    item_sales_map.replace(item_id, item_sales_map.get(item_id) + item_quantity);
                } else {
                    // If item has not been stored, create one in the hashmap with the current quantity
                    item_sales_map.put(item_id, item_quantity);
                }
                // Increase total store sales with current quantity
                total_quantity += item_quantity;
            }

            // Write out details of each item sold (key: store ID, value: item ID, total item sales, total store sales)
            for (String id: item_sales_map.keySet()) {
                DETAILS.set(id + "," + item_sales_map.get(id) + "," + total_quantity);
                context.write(key, DETAILS);
            }
        }
    }

    public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
        private IntWritable TOTAL_QUANTITY = new IntWritable();
        private Text DETAILS = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // Obtain details of each item (item ID, total item sales, total store sales)
            String[] item_details = value.toString().split(",");
            // Negate the value of total store sales as this would sort it in descending order for the reducer
            TOTAL_QUANTITY.set(-1 * Integer.parseInt(item_details[2]));
            // Set the new item details (item ID, total item sales, store ID)
            DETAILS.set(item_details[0] + "," + item_details[1] + "," + key.toString());
            // Write out new value with the position of store ID and (negated) total store sales switched
            // (key: total store sales, value: item ID, total item sales, store ID)
            context.write(TOTAL_QUANTITY, DETAILS);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Text DETAILS = new Text();
        // Keeps track of N stores that has been written out
        int count_N = 0;
    
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Obtain values of M and N from configuration
            Configuration conf = context.getConfiguration();
            final int M = Integer.parseInt(conf.get("M"));
            final int N = Integer.parseInt(conf.get("N"));
            // A hash map to store the item maps of different stores in the case that multiple stores has the same
            // total sales value
            Map<Integer, Map<Integer,String>> store_maps = new HashMap<Integer, Map<Integer,String>>();

            if (count_N < N) {
                for (Text value:values) {
                    // Obtain item details
                    String[] item_details = value.toString().split(",");
                    String item_id = item_details[0];
                    int quantity = Integer.parseInt(item_details[1]);
                    int store_id = Integer.parseInt(item_details[2]);

                    if (store_maps.containsKey(store_id)) {
                        // If store has been collected, obtain its tree map and add the new item and quantity into
                        // the existing tree map
                        Map<Integer,String> item_sales_map = store_maps.get(store_id);
                        item_sales_map.put(quantity, item_id);
                    } else {
                        // If this is first item of the store, create a new tree map and store the first item and
                        // quantity into the map, then store the tree map in the hashmap assigned to its store ID
                        Map<Integer,String> item_sales_map = new TreeMap<Integer,String>();
                        item_sales_map.put(quantity, item_id);
                        store_maps.put(store_id, item_sales_map);
                    }
                }

                // For every store in the hashmap
                for (Integer store: store_maps.keySet()) {
                    // Ensure there is still space to write out store
                    if (count_N < N) {
                        // Obtain all the item IDs for the specific store
                        List<Integer> key_list = new ArrayList<Integer>(store_maps.get(store).keySet());
                        // Obtain the tree map containing all item IDs and quantity of this specific store
                        Map<Integer,String> item_sales_map = store_maps.get(store);
                        
                        int maxLen = M;
                        if (item_sales_map.size() <= M) {
                            // If store has less than M items sold, write out all items
                            maxLen = item_sales_map.size();
                        }
                        // Write out all items (first maxLen items in descending order)
                        for (int i=maxLen-1; i>=0; i--) {
                            DETAILS.set(item_sales_map.get(key_list.get(i)) + ", " + key_list.get(i));
                            // Write out store details (key: store ID, value: item ID, item sales)
                            context.write(new Text("ss_store_sk_" + Integer.toString(store)), DETAILS);
                        }
                        count_N++;
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // Setup the first mapreduce job to get sales data for each items in each store and
        // the total store sales
        Configuration sales_configuration = new Configuration();
        // Store start and end date from input in the configuration
        sales_configuration.set("start_date", args[2]);
        sales_configuration.set("end_date", args[3]);

        Job sales_job = Job.getInstance(sales_configuration, "Obtain sales data");
        sales_job.setJarByClass(Query1B.class);
        sales_job.setMapperClass(SalesMapper.class);
        sales_job.setCombinerClass(SalesReducer.class);
        sales_job.setReducerClass(SalesReducer.class);

        sales_job.setOutputFormatClass(SequenceFileOutputFormat.class);
        sales_job.setOutputKeyClass(Text.class);
        sales_job.setOutputValueClass(Text.class);

        // Temporary directory for output of first job and input of second job
        Path tempPath = new Path("temporary/Query1B");
        FileInputFormat.addInputPath(sales_job, new Path(args[4]));
        FileOutputFormat.setOutputPath(sales_job, tempPath);

        // Run the first job
        sales_job.waitForCompletion(true);

        // Setup the second job to sort and output result based on specification
        Configuration sort_sales_configuration = new Configuration();
        // Store M and N from input into the configuration
        sort_sales_configuration.set("M", args[0]);
        sort_sales_configuration.set("N", args[1]);
        
        Job sort_sales_job = Job.getInstance(sort_sales_configuration, "Sort sales data");
        sort_sales_job.setJarByClass(Query1B.class);
        sort_sales_job.setMapperClass(SortMapper.class);
        sort_sales_job.setReducerClass(SortReducer.class);

        sort_sales_job.setInputFormatClass(SequenceFileInputFormat.class);
        sort_sales_job.setMapOutputKeyClass(IntWritable.class);
        sort_sales_job.setMapOutputValueClass(Text.class);
        sort_sales_job.setOutputKeyClass(Text.class);
        sort_sales_job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sort_sales_job, tempPath);
        FileOutputFormat.setOutputPath(sort_sales_job, new Path(args[5]));

        // Delete temporary directory
        tempPath.getFileSystem(sort_sales_configuration).deleteOnExit(tempPath);
        // Run the second job then exit
        System.exit(sort_sales_job.waitForCompletion(true) ? 0 : 1);
    }
}