package com.b5m.cluster.minhash;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
  public IntArrayWritable() {
    super(IntWritable.class);
  }
  
  public String toString() {
    return Arrays.asList(toStrings()).toString();
  }
}