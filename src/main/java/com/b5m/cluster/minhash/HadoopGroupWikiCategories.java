package com.b5m.cluster.minhash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopGroupWikiCategories {

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(HadoopGroupWikiCategories.class);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Path data = new Path(otherArgs[0]);
    FileSystem fs = FileSystem.get(new Configuration());
    
    Path out = new Path(otherArgs[1]);
    fs.delete(out);
    groupCategories(data, out, conf);
  }
  private static void groupCategories(Path data, Path out, Configuration conf) throws Exception {
    Job job = new Job(conf, "hadoop-wiki-categories");
    
    job.setJarByClass(HadoopGroupWikiCategories.class);
    job.setMapperClass(SplitMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(GroupReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.setInputPaths(job, data);
    FileOutputFormat.setOutputPath(job, out);

    job.waitForCompletion(true);
  }
}