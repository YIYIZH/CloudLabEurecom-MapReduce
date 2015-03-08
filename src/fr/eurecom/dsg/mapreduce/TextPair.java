package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API)
 * and Comparable to itself.
 */
public class TextPair implements WritableComparable<TextPair> {

// TODO: add the pair objects as TextPair fields
    private Text _first;
    private Text _second;

    public void set(Text first, Text second) {
        // TODO: implement the set method that changes the Pair content
        _first = first;
        _second = second;
    }

    public Text getFirst() {
        // TODO: implement the first getter
        return _first;
    }

    public Text getSecond() {
        // TODO: implement the second getter
        return _second;
    }

    public TextPair() {
        // TODO: implement the constructor, empty constructor MUST be implemented
        //       for deserialization
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        this.set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        this.set(first, second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO: write to out the serialized version of this such that
        //       can be deserializated in future. This will be use to write to HDFS
        _first.write(out);
        _second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO: read from in the serialized version of a Pair and deserialize it
        _first.readFields(in);
        _second.readFields(in);
    }

    @Override
    public int hashCode() {
        // TODO: implement hash
        return _first.hashCode()*163 + _second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        // TODO: implement equals
        if (o instanceof TextPair){
            TextPair com = (TextPair)o;
            return _first == com.getFirst() && _second == com.getSecond();
        }
        return false;
    }

    @Override
    public int compareTo(TextPair tp) {
        // TODO: implement the comparison between this and tp
        int com1 = _first.compareTo(tp.getFirst());
        if(com1 == 0){
            return _second.compareTo(tp.getSecond());
        }
        return com1;
    }

    @Override
    public String toString() {
        // TODO: implement toString for text output format
        return String.format("(%s, %s)", _first.toString(), _second.toString());
    }


// DO NOT TOUCH THE CODE BELOW

    /**
     * Compare two pairs based on their values
     */
    public static class Comparator extends WritableComparator {

        /**
         * Reference to standard Hadoop Text comparator
         */
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }

    /**
     * Compare just the first element of the Pair
     */
    public static class FirstComparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).getFirst().compareTo(((TextPair) b).getFirst());
            }
            return super.compare(a, b);
        }

    }
}
