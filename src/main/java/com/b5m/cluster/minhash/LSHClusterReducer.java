package com.b5m.cluster.minhash;

//This method is based on Broder '97 Syntactic Clustering of the Web 
//plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class LSHClusterReducer extends Reducer<Text, Text, SecondarySortKey, Text> {

  private final Logger logger = Logger.getLogger(LSHClusterReducer.class);

  private float bands;

  private float threshold;
  
  private int topk;

  @Override
  public void reduce(Text pair, Iterable<Text> sketches, Context context) throws IOException, InterruptedException {
    int count = 0;
    Set<String> set = Sets.newHashSet();
    for (Text sketch : sketches) {
      count++;
      set.add(sketch.toString());
    }
    List<String> top = Ordering.natural().leastOf(set, topk);
    float fraction = count / bands;
    if (fraction > threshold) {
      String[] values = pair.toString().split("\\|");
      for (String str : top) {
        Text key = new Text(str);
        Text v0 = new Text(values[0]);
        context.write(new SecondarySortKey(key, v0), v0);
        Text v1 = new Text(values[1]);
        context.write(new SecondarySortKey(key, v1), v1);
      }
    }
  }

  @Override
  public void setup(Context context) {
    int functionsCount = 100;
    int rows = context.getConfiguration().getInt(HadoopLSHClustering.ROWS, 10);
    this.topk = context.getConfiguration().getInt(HadoopLSHClustering.TOP_K, 1);
    this.bands = functionsCount / rows;
    this.threshold = (float) Math.pow(1 / bands, 1 / (float) rows);
    logger.info(String.format("{b:%s, r:%s, t:%.4f}", bands, rows, threshold));
  }
}
