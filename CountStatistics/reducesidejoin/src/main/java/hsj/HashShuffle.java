package hsj;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import pair.HybridTriple;
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
import pair.IntPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents an implementation of the Hash+Shuffle algorithm (Reduce-side join).
 */
public class HashShuffle extends Configured implements Tool {
    public enum TRIANGLE_COUNTER {
        COUNTER
    }

    private static final Logger logger = LogManager.getLogger(HashShuffle.class);

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
     * This class represents a Mapper that get two possible combinations of tuple and emits it. This mapper emits
     * Two tuples in this format
     * (x,y)  and (y,z) where x -> y and y -> z  (-> implies, points to)
     * It emits data from the same file as if it were from two different files.
     */
    public static class HashShuffleFollowerMapper extends Mapper<Object, Text, IntWritable, HybridTriple> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            if (values.length == 2) {
                int followerValue = Integer.parseInt(values[0]);
                int nodeToFollow = Integer.parseInt(values[1]);

                HybridTriple followedTriple = new HybridTriple(followerValue, nodeToFollow, 'S');
                HybridTriple followerTriple = new HybridTriple(followerValue, nodeToFollow, 'T');

                context.write(new IntWritable(followerValue), followedTriple);
                context.write(new IntWritable(nodeToFollow), followerTriple);
            }
        }
    }

    /**
     * This class represents a Reducer for the Two path mapper that collects the two tuples emitted for each entry
     * and groups them accordingly. It does an equi join on the data by saving inputs in two different lists and
     * comparing them.
     */
    public static class HashShuffleFollowerReducer extends Reducer<IntWritable, HybridTriple, NullWritable, Text> {
        private List<IntPair> listA = new ArrayList<>();
        private List<IntPair> listB = new ArrayList<>();

        @Override
        public void reduce(IntWritable key, Iterable<HybridTriple> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();


            for (HybridTriple hp : values) {
                if (hp.getThird() == 'S') {
                    listA.add(new IntPair(hp.getFirst(), hp.getSecond()));
                } else if (hp.getThird() == 'T') {
                    listB.add(new IntPair(hp.getFirst(), hp.getSecond()));
                }
            }

            executeJoinLogic(context);
        }


        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (IntPair value : listA) {
                    for (IntPair value2 : listB) {
                        StringBuilder builder = new StringBuilder();
                        builder.append(value2.getFirst() + "," + value.getSecond());
                        if (value2.getFirst() != value.getSecond())
                            context.write(null, new Text(builder.toString()));
                    }
                }
            }
        }
    }

    /**
     * This class represents a Mapper which scans through the original dataset and emits it without any processing.
     * It emits the data with an "Original" attribute implying it is the original data that has not been processed.
     */
    public static class TriangleOriginalDataMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            if (values.length == 2) {
                context.write(new Text(values[0] + "," + values[1]), new Text("Original"));
            }
        }

    }

    /**
     * This class represents a mapper that emits the data by reversing the order. This class helps in determining if
     * a triangle exists. It passes the data with a "New" String attribute implying the data is processed data.
     */
    public static class TriangleNewDataMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            if (values.length == 2) {
                if (values[1].equals("100") && values[0].equals("41")) {
                    System.out.println("TEST");
                }
                context.write(new Text(values[1] + "," + values[0]), new Text("New"));
            }
        }
    }


    /**
     * This class is used to check if the data being processed has triangles in it, it increments the counter
     * when it finds a triangle.
     */
    public static class TriangleReducer extends Reducer<Text, Text, NullWritable, Text> {
        public static int counter = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            boolean originalFlag = false;
            boolean newFlag = false;

            for (Text t : values) {
                if (t.toString().equals("Original")) {
                    originalFlag = true;
                } else if (t.toString().equals("New")) {
                    newFlag = true;
                    count++;
                }
            }

            if (originalFlag && newFlag) {
                counter += count;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(TRIANGLE_COUNTER.COUNTER).increment(counter / 3);
            counter = 0;
//            context.write(null, new Text("" + counter / 3));
        }
    }


    /**
     * This is the main driver program that handles all the mappers and the reducers.
     */
    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("MAX_VALUE", "70000");

        maxFilterJob(conf, args);

        pathTwoJob(conf, args);


        return triangleCountJob(conf, args);
    }

    /**
     * This method creates a Max filter job that creates a tuple output where both the numbers in the tuple are less
     * than the max value provided.
     */
    private void maxFilterJob(Configuration conf, String[] args) throws Exception {
        final Job filterJob = Job.getInstance(conf, "HashShuffle");
        filterJob.setJarByClass(HashShuffle.class);

        final Configuration jobConf = filterJob.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        filterJob.setMapperClass(MaxFilterMapper.class);
        filterJob.setReducerClass(MaxFilterReducer.class);

        filterJob.setOutputKeyClass(Text.class);
        filterJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(filterJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(filterJob, new Path(args[1]));

        filterJob.waitForCompletion(true);

    }


    /**
     * This method find the Two path for all the tuples.
     */
    private void pathTwoJob(Configuration conf, String[] args) throws Exception {
        final Job job = Job.getInstance(conf, "HashShuffle");
        job.setJarByClass(HashShuffle.class);


        job.setMapperClass(HashShuffleFollowerMapper.class);
        job.setReducerClass(HashShuffleFollowerReducer.class);


        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(HybridTriple.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }


    /**
     * This method calculates the Triangle count in the entire file.
     */
    private int triangleCountJob(Configuration conf, String[] args) throws Exception {
        final Job job2 = Job.getInstance(conf, "HashShuffle");
        job2.setJarByClass(HashShuffle.class);

        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, TriangleOriginalDataMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, TriangleNewDataMapper.class);

        job2.setReducerClass(TriangleReducer.class);


        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        if(job2.waitForCompletion(true)) {
            Log log = LogFactory.getLog(HashShuffle.class);
            log.info("Total Triangle Count: " + job2.getCounters().findCounter(TRIANGLE_COUNTER.COUNTER).getValue());
            return 1;
        } else {
            return 0;
        }
    }


    /**
     * Main Method to call the driver.
     */
    public static void main(final String[] args) {
        long start = System.currentTimeMillis();

        if (args.length != 4) {
            throw new Error("Four arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new HashShuffle(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }

        long end = System.currentTimeMillis();

        System.out.println("Total Time: " + (end - start) / 1000);
    }
}
