package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Pair extends Configured implements Tool {

    public static class PairMapper
            extends Mapper<Object, // TODO: change Object to input key type
            Text, // TODO: change Object to input value type
            TextPair, // TODO: change Object to output key type
            LongWritable> { // TODO: change Object to output value type
        // TODO: implement mapper

        private static final LongWritable ONE = new LongWritable(1);
        private TextPair _pair = new TextPair();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().replaceAll("\\s+", " ").trim().split(System.getProperty("line.separator"));
            for (String line : lines) {
                String[] words = line.trim().split(" ");
                for (String word : words)
                    for (String neighbor : words)
                        if (!word.equals(neighbor)) {
                            _pair.set(new Text(word), new Text(neighbor));
                            context.write(_pair, ONE);
                        }
            }
        }
    }

    public static class PairReducer
            extends Reducer<TextPair, // TODO: change Object to input key type
            LongWritable, // TODO: change Object to input value type
            TextPair, // TODO: change Object to output key type
            LongWritable> { // TODO: change Object to output value type
        // TODO: implement reducer

        private static final LongWritable _count = new LongWritable();

        @Override
        public void reduce(TextPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                count += val.get();
            }

            _count.set(count);
            context.write(key, _count);
        }

    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }


    public static class FirstPartitioner extends Partitioner<TextPair, LongWritable> {
        @Override
        public int getPartition(TextPair key, LongWritable value, int numPartitions) {
            return Math.abs(key.getFirst().hashCode()) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);  // TODO: define new job instead of null using conf e setting a name

        job.setPartitionerClass(FirstPartitioner.class); // set custom partitioner so that keypair with the same first field will be partitioned to the same reducer
        job.setCombinerClass(PairReducer.class);

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // TODO: set map class and the map output key and value classes
        job.setMapperClass(PairMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(LongWritable.class);
        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(TextPair.class);
        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // TODO: add the input file as job input (from HDFS) to the variable inputFile
        FileInputFormat.addInputPath(job, inputPath);
        // TODO: set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, outputDir);
        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);
        // TODO: set the jar class
        job.setJarByClass(Pair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }
}
