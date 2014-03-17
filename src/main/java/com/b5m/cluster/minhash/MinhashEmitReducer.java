package com.b5m.cluster.minhash;

//This method is based on Broder '97 Syntactic Clustering of the Web 
//plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class MinhashEmitReducer extends Reducer <SecondarySortKey, Text, Text, Text> {
  
  @Override
  public void reduce(SecondarySortKey key, Iterable<Text> ids, Context context) throws IOException, InterruptedException {
    List<Text> documents = Lists.newArrayList();
    Text first = null;
    for (Text txt :ids) {
      Text x = new Text(txt);
      if (first == null) {
        first = x;
      }
      for (Text text : documents) {
        context.write(new Text(String.format("%s|%s", text, x)), first);
      }
      documents.add(x);
    }
  }
}
