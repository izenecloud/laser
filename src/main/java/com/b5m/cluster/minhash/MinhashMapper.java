package com.b5m.cluster.minhash;

// This method is based on Broder '97 Syntactic Clustering of the Web 
// plus LSH as described on Rajaraman, Leskovec and Ullman 2012
// and code originally found on org.apache.mahout.clustering.minhash.MinHashMapper
// available under the Apache License 2.0.

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public final class MinhashMapper extends Mapper<Text, IntArrayWritable, Text, Text> {

	
	public static final String ROWS = "rows";
  private HashFunction lsh;
  
  private HashFunction[] functions;

  private int functionsCount;

  private int rows;
  
  private int[] hashValues; 

  @Override
  public void map(Text id, IntArrayWritable values, Context ctx) throws IOException,
          InterruptedException {
    for (int i = 0; i < functionsCount; i++) {
      hashValues[i] = Integer.MAX_VALUE;
    }
    for (int i = 0; i < functionsCount; i++) {
      HashFunction hf = functions[i];
      for (Writable wr : values.get()) {
        IntWritable value = (IntWritable) wr;
        int hash = hf.hashInt(value.get()).asInt();
        if (hash < hashValues[i]) {
          hashValues[i] = hash;
        }
      }
    }
    Text sketch = new Text();
    Hasher hasher = lsh.newHasher();
    int band = 0;
    for (int i = 0; i < functionsCount; i++) { 
      hasher.putInt(hashValues[i]);
      if (i > 0 && (i % rows) == 0) {
        sketch.set(band + "-" + hasher.hash().toString());
        write(id, sketch, ctx);
        hasher = lsh.newHasher();
        band++;
      }
    }
    sketch.set(band + "-" + hasher.hash().toString());
    write(id, sketch, ctx);
  }
  
  private void write(Text id, Text sketck, Context ctx) throws IOException, InterruptedException {
    ctx.write(sketck, id);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    this.functionsCount = 100;
    this.rows = context.getConfiguration().getInt(ROWS, 10);
    this.hashValues = new int[functionsCount];
    this.functions = new HashFunction[functionsCount];
    Random r = new Random(11);
    for (int i = 0; i < functionsCount; i++) {
      functions[i] = Hashing.murmur3_32(r.nextInt());
    }
    this.lsh = Hashing.murmur3_32(r.nextInt());
  }
}
