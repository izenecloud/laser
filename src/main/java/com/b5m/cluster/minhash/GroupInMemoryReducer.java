package com.b5m.cluster.minhash;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;

public class GroupInMemoryReducer extends Reducer<SecondarySortKey, Text, Text, TextArrayWritable> {

  @Override
  public void reduce(SecondarySortKey cluster, Iterable<Text> ids, Context context) throws IOException, InterruptedException {
    Set<Text> clustered = Sets.newLinkedHashSet();
    for (Text id : ids) {
      Text str = new Text(id);
      clustered.add(str);
    }        
    TextArrayWritable out = new TextArrayWritable();
    out.set(clustered.toArray(new Text[0]));
    context.write(cluster.getKey(), out);
  }
}
