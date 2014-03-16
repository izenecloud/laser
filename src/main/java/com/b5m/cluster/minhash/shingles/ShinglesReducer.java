package com.b5m.cluster.minhash.shingles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.b5m.cluster.minhash.IntArrayWritable;

public class ShinglesReducer extends Reducer<Text, IntWritable, Text, IntArrayWritable> {
 
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     List<IntWritable> shingles = new ArrayList<IntWritable>();
     for (IntWritable x : values) {
      IntWritable shingle = new IntWritable();
      shingle.set(x.get());
      shingles.add(shingle);
    }
    IntArrayWritable out = new IntArrayWritable();
    out.set(shingles.toArray(new IntWritable[0]));
    context.write(key, out);
  }
}
