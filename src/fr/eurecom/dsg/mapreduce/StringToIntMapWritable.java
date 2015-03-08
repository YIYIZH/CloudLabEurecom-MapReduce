package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

    // TODO: add an internal field that is the real associative array
    private HashMap<String, Integer> _stripes;

    public void set(HashMap<String, Integer> value){
        _stripes = value;
    }

    public HashMap<String, Integer> get(){
        return _stripes;
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        // TODO: implement deserialization

        // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
        // StringToIntMapWritable when reading new records. Remember to initialize your variables
        // inside this function, in order to get rid of old data.
        int size = in.readInt();
        _stripes = new HashMap<String, Integer>(size);
        Text key = new Text();
        IntWritable value = new IntWritable();
        for(int i = 0; i < size; i++){
            key.readFields(in);
            value.readFields(in);
            _stripes.put(key.toString(), value.get());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO: implement serialization
        out.writeInt(_stripes.size());

        Text key = new Text();
        IntWritable value = new IntWritable();

        for(Map.Entry<String, Integer> entry : _stripes.entrySet()){
            key.set(entry.getKey());
            key.write(out);
            value.set(entry.getValue());
            value.write(out);
        }

    }
}
