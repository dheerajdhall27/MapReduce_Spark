package rj;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * This class represents an implementation of the Replicated Join(Partition+Broadcast).
 */
public class ReplicatedJoin {

    /**
     * A global counter to get the total number of Triangles.
     */
    public enum TRIANGLE_COUNTER {
        COUNTER
    }

    private static final Logger logger = LogManager.getLogger(ReplicatedJoin.class);

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
     * This class represents a Mapper for the Replicated Join that creates data to be broadcasted to all workers
     * It does a self comparison between the two data sets and outputs the tuples that match.
     */
    public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {
        private static int counter = 0;
        private Map<Integer, HashSet<Integer>> followerMap = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

                if (files == null || files.length == 0) {
                    throw new RuntimeException(
                            "User information is not set in DistributedCache");
                }

                for (Path p : files) {
                    BufferedReader reader = new BufferedReader(new FileReader(p.getName() + "/part-r-00000"));
                    String line;

                    while ((line = reader.readLine()) != null) {
                        String[] values = line.split(",");

                        if (values.length == 2) {
                            int first = Integer.parseInt(values[0]);
                            int second = Integer.parseInt(values[1]);


                            if (!followerMap.containsKey(first)) {
                                HashSet<Integer> set = new HashSet<>();
                                set.add(second);
                                followerMap.put(first, set);
                            } else {
                                followerMap.get(first).add(second);
                            }
                        }
                    }
                }
                System.out.println("SETUP COMPLETED");

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(",");

            if (values.length == 2) {
                int first = Integer.parseInt(values[0]);
                int second = Integer.parseInt(values[1]);

                if (followerMap.containsKey(first) && followerMap.containsKey(second)) {
                    for (Integer i : followerMap.get(second)) {
                        if (followerMap.containsKey(i)) {
                            if (followerMap.get(i).contains(first)) {
                                counter++;
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("JOB COMPLETED SUCCESSFULLY!");
            context.getCounter(TRIANGLE_COUNTER.COUNTER).increment(counter / 3);
            counter = 0;
        }
    }

    /**
     * This is the main driver program that handles running all the jobs.
     */
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("MAX_VALUE", "210000");

        maxFilterJob(conf, otherArgs);
        triangleCountJob(conf, otherArgs);

        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (end - start) / 1000f);

    }

    /**
     * This method creates a Max filter job that creates a tuple output where both the numbers in the tuple are less
     * than the max value provided.
     */
    private static void maxFilterJob(Configuration conf, String[] args) throws Exception {

        final Job filterJob = Job.getInstance(conf, "HashShuffle");
        filterJob.setJarByClass(ReplicatedJoin.class);

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

    /**
     * This method calculates the Triangle count in the entire file.
     */
    private static int triangleCountJob(Configuration conf, String[] otherArgs) throws Exception {

        Job job = new Job(conf, "Replicated Join");
        job.setJarByClass(ReplicatedJoin.class);

        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(), job.getConfiguration());

        DistributedCache.setLocalFiles(job.getConfiguration(), otherArgs[1]);

        if (job.waitForCompletion(true)) {
            Log log = LogFactory.getLog(ReplicatedJoin.class);
            log.info("Total Triangle Count: " + job.getCounters().findCounter(TRIANGLE_COUNTER.COUNTER).getValue());
            return 0;
        } else {
            return 1;
        }
    }
}
