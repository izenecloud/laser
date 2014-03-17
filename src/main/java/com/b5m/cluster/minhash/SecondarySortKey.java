package com.b5m.cluster.minhash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortKey implements WritableComparable<SecondarySortKey> {
    private Text key;
    private Text value;
    
    public SecondarySortKey(Text key, Text value) {
      this.key = key;
      this.value = value;
    }

    public SecondarySortKey() {
      this(new Text(), new Text());
    }

    public Text getKey() {
      return key;
    }
    
    public Text getValue() {
      return value;
    }
    
    public void write(DataOutput out) throws IOException {
      value.write(out);
      key.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      value.readFields(in);
      key.readFields(in);
    }
    
    public int compareTo(SecondarySortKey o) {
      int diff = key.compareTo(o.key);
      if (diff != 0) {
        return diff;
      } 
      return value.compareTo(o.value); 
    }

    public String toString() {
      return key + ":" + value;
    }

    public static class KeyPartitioner extends Partitioner<SecondarySortKey, Text> {
      @Override
      public int getPartition(SecondarySortKey key, Text value, int numPartitions) {
        return Math.abs(key.getKey().hashCode() * 127) % numPartitions;
      }
    }

    public static class GroupingComparator extends WritableComparator {
      protected GroupingComparator() {
        super(SecondarySortKey.class, true);
      }

      @Override
      public int compare(WritableComparable w1, WritableComparable w2) {
        SecondarySortKey sk1 = (SecondarySortKey) w1;
        SecondarySortKey sk2 = (SecondarySortKey) w2;
        return sk1.getKey().compareTo(sk2.getKey());
      }
    }
  }
