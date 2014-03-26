package com.b5m.couchbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.testng.annotations.Test;

import com.b5m.larser.source.couchbase.CouchbaseConfig;
import com.b5m.larser.source.couchbase.CouchbaseInputFormat;

public class TestCouchbaseInputFormat {
	public static class DumpMapper extends
			Mapper<BytesWritable, BytesWritable, Text, Text> {
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// NOTHING
		}

		public void close() throws IOException {

		}

		public void map(BytesWritable kbw, BytesWritable vbw, Context context)
				throws IOException, InterruptedException {
			String key = new String(kbw.getBytes());
			String val = new String(vbw.getBytes());
			context.write(new Text(key), new Text(val));
		}
	}

	@Test
	public void hadoopJob() throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();

		conf.set(CouchbaseConfig.CB_INPUT_CLUSTER, "http://localhost:8091/");
		conf.set(CouchbaseConfig.CB_INPUT_BUCKET, "default");

		final Job job = new Job(conf);
		FileOutputFormat.setOutputPath(job, new Path("couchbase"));
		job.setJarByClass(TestCouchbaseInputFormat.class);
		job.setMapperClass(DumpMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(CouchbaseInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);

		HadoopUtil.delete(conf, new Path("couchbase"));
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}
	}
}