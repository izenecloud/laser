package com.b5m.cluster.minhash;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class SplitMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable line, Text value, Context ctx) throws IOException,
          InterruptedException {
    String[] values = value.toString().split(" ");
    if(values.length > 1)
    	ctx.write(new Text(values[0]), new Text(values[1]));
  }
}
