import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a Program to calculate the total number of path2 records in te twitter dataset.
 */
public class CountStatistics extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(CountStatistics.class);

    public enum TOTAL_COUNT {
        COUNT
    }

    /**
     * This class represents a Mapper for the Filter Job, where the tuples that have both values below a max
     * value provided are emitted.
     */
    public static class MaxFilterMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if (values.length == 2) {
                int first = Integer.parseInt(values[0]);
                int second = Integer.parseInt(values[1]);

                int v = context.getConfiguration().getInt("MAX_VALUE", Integer.MAX_VALUE);
                if (first <= v && second <= v) {
                    context.write(new Text(values[0]), new Text(values[1]));
                }
            }
        }
    }


    /**
     * This class represents a reducer class for the Max Filter Mapper, that collects all the tuples from the mapper
     * phase and emit it into a file.
     */
    public static class MaxFilterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            for (final Text val : values) {
                context.write(key, val);
            }
        }
    }

    /**
     * This Mapper emits the number of incoming and outgoing edges for both the follower node and the node being
     * followed
     */
    public static class CountingMapper extends Mapper<Object, Text, Text, IntPair> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if (values.length == 2) {
                context.write(new Text(values[1]), new IntPair(1, 0));
                context.write(new Text(values[0]), new IntPair(0, 1));
            }
        }
    }

    /**
     * This class represents a reducer that counts the total number of path2. It adds the incoming edges and outoing
     * edges for a particular node and multiplies them to create a count which is added to the global counter.
     */
    public static class CountingReducer extends Reducer<Text, IntPair, NullWritable, NullWritable> {
        @Override
        public void reduce(final Text key, final Iterable<IntPair> values, final Context context) throws IOException, InterruptedException {
            int incomingCount = 0;
            int outgoingCount = 0;
            for (final IntPair pair : values) {
                incomingCount += pair.getFirst();
                outgoingCount += pair.getSecond();
            }

            long totalCount = incomingCount * outgoingCount;

            context.getCounter(TOTAL_COUNT.COUNT).increment(totalCount);
        }
    }

    /**
     * Main Method to call the driver.
     */
    public static void main(final String[] args) {

        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new CountStatistics(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("MAX_VALUE", "1000");

        maxFilterJob(conf, args);
        return CountingStatisticsJob(conf, args);

    }

    /**
     * This method creates a Max filter job that creates a tuple output where both the numbers in the tuple are less
     * than the max value provided.
     */
    private static void maxFilterJob(Configuration conf, String[] args) throws Exception {

        final Job filterJob = Job.getInstance(conf, "HashShuffle");
        filterJob.setJarByClass(CountStatistics.class);

        final Configuration jobConf = filterJob.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        filterJob.setMapperClass(MaxFilterMapper.class);
        filterJob.setReducerClass(MaxFilterReducer.class);
        filterJob.setNumReduceTasks(1);

        filterJob.setOutputKeyClass(Text.class);
        filterJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(filterJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(filterJob, new Path(args[1]));

        filterJob.waitForCompletion(true);

    }
    
    private static int CountingStatisticsJob(Configuration conf, String[] args) throws Exception {
        final Job countingJob = Job.getInstance(conf, "HashShuffle");
        countingJob.setJarByClass(CountStatistics.class);


        countingJob.setMapperClass(CountingMapper.class);
        countingJob.setReducerClass(CountingReducer.class);
        countingJob.setNumReduceTasks(1);

        countingJob.setOutputKeyClass(Text.class);
        countingJob.setOutputValueClass(IntPair.class);

        FileInputFormat.addInputPath(countingJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(countingJob, new Path(args[2]));

        if (countingJob.waitForCompletion(true)) {
            System.out.println("Total Count: " + countingJob.getCounters().findCounter(TOTAL_COUNT.COUNT).getValue());
            return 1;
        } else {
            return 0;
        }
    }
}

