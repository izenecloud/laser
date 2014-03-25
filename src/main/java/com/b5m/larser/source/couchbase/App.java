package com.b5m.larser.source.couchbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
	public static class DumpMapper extends
			Mapper<BytesWritable, BytesWritable, Text, Text> {
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// NOTHING
		}

		public void close() throws IOException {

		}

		public void map(BytesWritable kbw, BytesWritable vbw,
				 Context context) {
			String key = new String(kbw.getBytes());
			String val = new String(vbw.getBytes());
			System.out.println("K,V: " + key + ", " + val);
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		conf.set(CouchbaseConfig.CB_INPUT_CLUSTER, "http://127.0.0.1:8091/");

		final Job job = new Job(conf);
		FileOutputFormat.setOutputPath(job, new Path("tmp/couchbase"));
		job.setJarByClass(App.class);
		job.setMapperClass(DumpMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CouchbaseInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}
}