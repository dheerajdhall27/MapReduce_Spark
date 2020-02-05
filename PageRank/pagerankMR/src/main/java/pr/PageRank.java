package pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This class represents a way to calculate the page rank for a synthetic graph. For a given k it creates the chain.
 * Example k = 2
 * 1->2
 * 2->0
 * 3->4
 * 4->0
 * <p>
 * 0 represents the dangling nodes.
 */
public class PageRank extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRank.class);

    public enum ZERO_PROBABILITY {
        COUNTER
    }

    /**
     * This class represents a mapper for creating the graph. It maps the values in the way described above.
     */
    public static class GraphMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int k = context.getConfiguration().getInt("K", 0);
            int total = k * k;

            double initialPageRank = 1.0 / total;

            for (int i = 1; i < total + 1; i++) {
                if (i % k == 0) {
                    context.write(new Text("" + i), new Text(initialPageRank + ":0"));
                } else {
                    context.write(new Text("" + i), new Text(initialPageRank + ":" + (i + 1)));
                }
            }

            context.write(new Text("0"), new Text("0:null"));
        }

    }

    /**
     * This class represents a Reducer for the graph job, it generates the initial page rank for all the nodes.
     */
    public static class GraphReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            double PR = 0.0;
            for (Text value : values) {
                PR = Double.parseDouble(value.toString().split(":")[0]);
                builder.append(value.toString().split(":")[1]).append(",");
            }

            if (builder.length() > 0)
                builder.deleteCharAt(builder.length() - 1);

            context.write(new Text(key), new Text(PR + "->" + builder.toString()));
        }
    }


    /**
     * This class represents a mapper for the Page rank job that create the page rank for each node.
     */
    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

        private long counter = 0;

        @Override
        public void map(final Object key, Text value, final Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");

            context.write(new Text(data[0]), new Text("Vertex," + data[1]));
            double pageRank = Double.parseDouble(data[1].split("->")[0]);

            if (data[1].split("->")[1].equals("0")) {
                long pr = (long) (pageRank * Math.pow(10, 10));
                counter += pr;
            }
            context.write(new Text(data[1].split("->")[1]), new Text("" + pageRank));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(ZERO_PROBABILITY.COUNTER).increment(counter);
        }
    }


    /**
     * This class represents the reducer for the page rank job.
     */
    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private static long counter;

        @Override
        public void setup(Context context) throws InterruptedException, IOException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            counter = currentJob.getCounters().findCounter(ZERO_PROBABILITY.COUNTER).getValue();
        }

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            double runningSum = 0.0;
            String m = "";

            for (Text value : values) {
                if (value.toString().split(",")[0].equals("Vertex")) {
                    m = value.toString();
                } else {
                    runningSum += Double.parseDouble(value.toString());
                }
            }

            int k = context.getConfiguration().getInt("K", 0);
            int total = k * k;

            String[] data = m.split(",");
            if (!key.toString().equals("0")) {

                double value = (double) counter / Math.pow(10, 10);
                runningSum += value / total;
            }

            if (data.length > 1) {
                double pr = (0.15 / total) + 0.85 * runningSum;
                if (!data[0].equals("0"))
                    context.write(key, new Text(pr + "->" + data[1].split("->")[1]));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            counter = 0;
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            currentJob.getCounters().findCounter(ZERO_PROBABILITY.COUNTER).setValue(0);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("K", "1000");
        graphJob(conf, args);
        pageRankJob(conf, args);

        return 0;
    }

    private void graphJob(Configuration conf, String[] args) throws Exception {
        final Job graphJob = Job.getInstance(conf, "PageRank");
        graphJob.setJarByClass(PageRank.class);

        final Configuration jobConf = graphJob.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        graphJob.setMapperClass(GraphMapper.class);
        graphJob.setReducerClass(GraphReducer.class);

        graphJob.setOutputKeyClass(Text.class);
        graphJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(graphJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(graphJob, new Path(args[1] + "-0"));
        graphJob.waitForCompletion(true);
    }

    private void pageRankJob(Configuration conf, String[] args) throws Exception {
        int counter = 0;

        while (counter <= 9) {
            final Job pageRankJob = Job.getInstance(conf, "PageRank");
            pageRankJob.setJarByClass(PageRank.class);

            final Configuration jobConf = pageRankJob.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", ",");

            pageRankJob.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.setNumLinesPerSplit(pageRankJob, 40000);

            pageRankJob.setMapperClass(PageRankMapper.class);
            pageRankJob.setReducerClass(PageRankReducer.class);

            pageRankJob.setOutputKeyClass(Text.class);
            pageRankJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(pageRankJob, new Path(args[1] + "-" + counter));
            FileOutputFormat.setOutputPath(pageRankJob, new Path(args[1] + "-" + (counter + 1)));

            pageRankJob.waitForCompletion(true);
            counter++;
        }
    }

    public static void main(String[] args) {

        try {
            ToolRunner.run(new PageRank(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}

