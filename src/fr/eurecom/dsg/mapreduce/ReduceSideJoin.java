package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


public class ReduceSideJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private int numReducers;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(RSJMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(RSJReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setNumReduceTasks(numReducers);
        job.setJarByClass(DistributedCacheJoin.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public ReduceSideJoin(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
        System.exit(res);
    }
}


// TODO: implement mapper
class RSJMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable _col1 = new IntWritable();
    private IntWritable _col2 = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\n");
        int col1;
        int col2;
        for(String line : lines){
            if(line.length() > 2){
                col1 = Integer.parseInt(line.split("\t")[0]);
                col2 = Integer.parseInt(line.split("\t")[1]);
                _col1.set(col1); _col2.set(col2);
                context.write(_col1, _col2);
                context.write(_col2, _col1);
            }
        }
    }
}

// TODO: implement reducer
class RSJReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    private Text _join = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> joins = new ArrayList<Integer>();
        for (IntWritable i : values) {
            joins.add(i.get());
        }

        //joins = new ArrayList<Integer>(new HashSet(joins)); // If wanted to remove duplicates

        int n = joins.size();
        for(int i = 0; i < n - 1; i++)
            for(int j = i+1; j < n; j++){
                _join.set(String.format("%s\t%s", joins.get(i), joins.get(j)));
                context.write(key, _join);
            }
    }
}
