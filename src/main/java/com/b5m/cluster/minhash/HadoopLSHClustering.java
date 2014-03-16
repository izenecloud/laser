package com.b5m.cluster.minhash;

//This method is based on Broder '97 Syntactic Clustering of the Web 
//plus LSH as described on Rajaraman, Leskovec and Ullman 2012

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopLSHClustering extends Configured implements Tool {

  public static final String ROWS = "rows";

  public static final String BANDS = "bands";

  public static final String TOP_K = "top-k";

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Path data = new Path(args[0]);
    int rows = Integer.parseInt(args[2]);
    conf.setInt(ROWS, rows);
    int bands = Integer.parseInt(args[3]);
    conf.setInt(BANDS, bands);
    boolean group = (args.length > 4) ? Boolean.parseBoolean(args[4]) : true;
    String suffix = String.format("%s-%s", rows, bands);
    Path sketches = new Path(args[1] + "-sketches-" + suffix);
    sketches.getFileSystem(conf).delete(sketches, true);
    Path clusters = new Path(args[1] + "-clusters-" + suffix);
    clusters.getFileSystem(conf).delete(clusters, true);
    try {
      Path out = new Path(args[1] + "-" + suffix);
      out.getFileSystem(conf).delete(out, true);
      return (computeMinhashes(data, sketches, conf) &&
              computeClusters(sketches, clusters, conf) &&
              groupClusters(clusters, out, conf, group)) ? 0 : 1;
    } finally {
/*      sketches.getFileSystem(conf).deleteOnExit(sketches);
      clusters.getFileSystem(conf).deleteOnExit(clusters);
*/    }
  }

  private static boolean computeMinhashes(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "compute-minhashes");
    job.setJarByClass(HadoopLSHClustering.class);
    job.setMapperClass(MinhashEmitMapper.class);
    job.setPartitionerClass(SecondarySortKey.KeyPartitioner.class);
    job.setGroupingComparatorClass(SecondarySortKey.GroupingComparator.class);
    job.setMapOutputKeyClass(SecondarySortKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(MinhashEmitReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true);
  }

  private static boolean computeClusters(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "compute-clusters");
    job.setJarByClass(HadoopLSHClustering.class);
    job.setMapperClass(Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(LSHClusterReducer.class);
    job.setOutputKeyClass(SecondarySortKey.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    
    return job.waitForCompletion(true);
  }

  private static boolean groupClusters(Path data, Path out, Configuration conf, boolean group)
          throws Exception {
    Job job = new Job(conf, "group-clusters");
    job.setJarByClass(HadoopLSHClustering.class);
    job.setMapperClass(Mapper.class);
    job.setPartitionerClass(SecondarySortKey.KeyPartitioner.class);
    job.setGroupingComparatorClass(SecondarySortKey.GroupingComparator.class);
    job.setMapOutputKeyClass(SecondarySortKey.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    if (group) {
      job.setReducerClass(GroupInMemoryReducer.class);
      job.setOutputValueClass(TextArrayWritable.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
    } else {
      job.setReducerClass(GroupReducer.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(TextOutputFormat.class);
    }
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopLSHClustering(), args);
    System.exit(res);
  }
}