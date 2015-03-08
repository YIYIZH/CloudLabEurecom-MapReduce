package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 */
public class WordCountIMC extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf); // TODO: define new job instead of null using conf e setting
        // a name

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // TODO: set map class and the map output key and value classes
        job.setMapperClass(WCIMCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(WCIMCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // TODO: add the input file as job input (from HDFS)
        FileInputFormat.addInputPath(job, inputPath);
        // TODO: set the output path for the job results (to HDFS)
        FileOutputFormat.setOutputPath(job, outputDir);
        // TODO: set the number of reducers. This is optional and by default is 1
        job.setNumReduceTasks(numReducers);
        // TODO: set the jar class
        job.setJarByClass(WordCountIMC.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public WordCountIMC(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
        System.exit(res);
    }
}

class WCIMCMapper extends Mapper<Object, // TODO: change Object to input key
        // type
        Text, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    private Text _word = new Text();
    private static final IntWritable _nWords = new IntWritable(0);

    @Override
    protected void map(Object key, // TODO: change Object to input key type
                       Text value, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {

        // * TODO: implement the map method (use context.write to emit results). Use
        // the in-memory combiner technique
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        HashMap<String, Integer> pairs = new HashMap<String, Integer>();

        while(stringTokenizer.hasMoreTokens()){
            String w  = stringTokenizer.nextToken();
            int count = pairs.containsKey(w)?pairs.get(w) + 1 : 1;
            pairs.put(w, count);
        }

        for (Map.Entry<String, Integer> entry : pairs.entrySet()) {
            _word.set(entry.getKey());
            _nWords.set(entry.getValue());
            context.write(_word, _nWords);
        }
    }

}

class WCIMCReducer extends Reducer<Text, // TODO: change Object to input key
        // type
        IntWritable, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    private IntWritable _count = new IntWritable();

    @Override
    protected void reduce(Text key, // TODO: change Object to input key type
                          Iterable<IntWritable> values, // TODO: change Object to input value type
                          Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method (use context.write to emit results)
        int count = 0;
        for (IntWritable val : values){
            count += val.get();
        }
        _count.set(count);
        context.write(key, _count);
    }
}
