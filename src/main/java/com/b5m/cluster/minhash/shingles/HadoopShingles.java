package com.b5m.cluster.minhash.shingles;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.b5m.cluster.minhash.IntArrayWritable;

public class HadoopShingles extends Configured implements Tool{

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Path data = new Path(args[0]);
    Path out = new Path(args[1]);
    FileSystem fs = out.getFileSystem(conf);
    fs.delete(out);
    conf.setInt("k", Integer.parseInt(args[2]));
    computeShingles(data, out, conf);
    return 0;
  }

  private static void computeShingles(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "hadoop-shingles");
    job.setJarByClass(HadoopShingles.class);
    job.setMapperClass(ShinglesMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(ShinglesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntArrayWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);
    job.waitForCompletion(false);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new HadoopShingles(), args);
    System.exit(res);
  }
}