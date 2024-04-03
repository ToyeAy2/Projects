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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Query1C {

    //Mapper that reads in the input text and outputs <Text, DoubleWritable>
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{
        private final static DoubleWritable PAID = new DoubleWritable();
        private Text sold_date = new Text();
        private int date;
    
        //Method processes each line of the input text
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Gets the start and end dates
            Configuration conf = context.getConfiguration();
            final int start_date = Integer.parseInt(conf.get("start_date"));
            final int end_date = Integer.parseInt(conf.get("end_date"));
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            //Processes each record from the input
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken() + " ";
                String[] row = token.split("[|]");
                //If the ss_sold_date_sk or ss_net_paid_inc_tax is missing, skip the record
                if (row[0].equals("") || row[21].equals("")) {
                    continue;
                } else {
                    //Get the ss_sold_date_sk and ss_net_paid_inc_tax from the input record
                    date = Integer.parseInt(row[0]);
                    sold_date.set("ss_sold_date_sk_" + row[0]);
                    PAID.set(Double.parseDouble(row[21]));
                    //If the date is within the specified range, output the record
                    if ((start_date <= date) && (date <= end_date)) {
                        context.write(sold_date, PAID);
                    }
                }
            }

        }

    }

    //Combiner class that takes <Text, DoubleWritable> as input and outputs <Text, DoubleWritable
    public static class FirstCombiner
                extends Reducer<Text,DoubleWritable,Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double net_paid = 0.0;

            //Find the total ss_net_paid_inc_tax for each date
            for (DoubleWritable paid: values) {
                net_paid += paid.get();
            }
            //Output the ss_sold_date_sk and the corresponing ss_net_paid_inc_tax value
            context.write(key, new DoubleWritable(net_paid));
        }
    }

    //Reducer class that takes <Text, DoubleWritable> as input and outputs <Text, Text>
    public static class FloatSumReducer
                extends Reducer<Text,DoubleWritable,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double net_paid = 0.0;

            //Find total ss_net_paid_inc_tax for each ss_sold_date_sk
            for (DoubleWritable paid : values) {
                net_paid += paid.get();
            }
            //Write result to the output, with the first Text object as the key and sum as the second Text object
            context.write(key, new Text(Double.toString(net_paid)));
        }
    }

    //Mapper class that takes <Object, Text> as input and outputs <DoubleWritable and Text>
    public static class SortMapper
                extends Mapper<Object, Text, DoubleWritable, Text>{
        private final static DoubleWritable PAID = new DoubleWritable();
        private Text sold_date = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Set sold_date to the input key
            sold_date.set(key.toString());
            //Parse input value as a double and st it to PAID
            PAID.set(Double.parseDouble(value.toString()));

            //Write the key-value pair with PAID as the key and sold_date as the value
            //Multiply PAID by -1 to make Hadoop sort the output in descending order
            context.write(new DoubleWritable(-1 * PAID.get()), sold_date);
        }
    }

    //Combiner class that takes <DoubleWritable, Text> input and outputs <DoubleWritable, Text>
    public static class SecondCombiner
            extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                //Reverse the order of key and value by multiplying the key value by -1
                context.write(new DoubleWritable(-1 * key.get()), value);
            }
        }
    }

    //Redcuer class that takes <DoubleWritable, Text> as input and outputs <Text, DoubleWritable>
    public static class TopKReducer
            extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        //Counter for number of records processed
        private int count = 0;
        private Text sold_date = new Text();

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            //Retrieve value of k specified
            final int K = Integer.parseInt(conf.get("K"));

            //Iterate through values
            for (Text name : values) {
                sold_date.set(name);
                //If k records haven't been processed
                if (count < K) {
                    //Write the sold date and revenue to the output
                    context.write(sold_date, key);
                    count++;
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        //Create configuration object for the first job
        Configuration net_paid_conf = new Configuration();
        //Set the start adn end dates in thhe configuration object
        net_paid_conf.set("start_date", args[1]);
        net_paid_conf.set("end_date", args[2]);
        //Create a Path object for the temporary directory
        Path tempPath = new Path("temporary/Query1C");
    
        //Create the first job to get the toal revenue
        Job net_paid = Job.getInstance(net_paid_conf, "Get total revenue");
        //Set the jar file for the job
        net_paid.setJarByClass(Query1C.class);
        //Set the mapper, combiner and reducer classes for the job
        net_paid.setMapperClass(TokenizerMapper.class);
        net_paid.setCombinerClass(FirstCombiner.class);
        net_paid.setReducerClass(FloatSumReducer.class);
        //Set the ouput format class for the job
        net_paid.setOutputFormatClass(SequenceFileOutputFormat.class);
        //Set the key and value classes for the mapper and reducer
        net_paid.setMapOutputKeyClass(Text.class);
        net_paid.setMapOutputValueClass(DoubleWritable.class);
        net_paid.setOutputKeyClass(Text.class);
        net_paid.setOutputValueClass(Text.class);
        //Set the input and output paths for the job
        FileInputFormat.addInputPath(net_paid, new Path(args[3]));
        FileOutputFormat.setOutputPath(net_paid, tempPath);
        //Create a controlledjob object for the first job
        ControlledJob net_paid_control = new ControlledJob(net_paid_conf);
        net_paid_control.setJob(net_paid);
        //Wait for the first job to complete
        net_paid.waitForCompletion(true);
        //Create a configuration object for the second job
        Configuration sort_result_conf = new Configuration();
        //Set the value of K in the configuration object
        sort_result_conf.set("K", args[0]);
        //Create the second job to sort the results and get the top k dates
        Job sort_result = Job.getInstance(sort_result_conf, "Sort result");
        sort_result.setJarByClass(Query1C.class);
        //Set mapper, combiner and reducer classes for the job
        sort_result.setMapperClass(SortMapper.class);
        //sort_result.setSortComparatorClass(DoubleWritable.Comparator.class);
        sort_result.setCombinerClass(SecondCombiner.class);
        sort_result.setReducerClass(TopKReducer.class);
        sort_result.setNumReduceTasks(1);
        //Set input and output paths for the job
        sort_result.setInputFormatClass(SequenceFileInputFormat.class);
        sort_result.setMapOutputKeyClass(DoubleWritable.class);
        //Add the first job to the dependency list of the second job
        sort_result.setMapOutputValueClass(Text.class);
        sort_result.setOutputKeyClass(Text.class);
        sort_result.setOutputValueClass(DoubleWritable.class);
    
        FileInputFormat.addInputPath(sort_result, tempPath);
        FileOutputFormat.setOutputPath(sort_result, new Path(args[4]));
        ControlledJob sort_result_control = new ControlledJob(sort_result_conf);
        sort_result_control.setJob(sort_result);
    
        //Create a JobControl object to manage jobs
        JobControl job_control = new JobControl("job-control");
        job_control.addJob(net_paid_control);
        job_control.addJob(sort_result_control);
        sort_result_control.addDependingJob(net_paid_control);
        
        System.exit(sort_result.waitForCompletion(true) ? 0 : 1);
      }
}