package com.b5m.couchbase;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.b5m.conf.Configuration;

import static org.testng.Assert.*;

public class CouchbaseInputFormatIT {
	private static final String PROPERTIES = "src/test/properties/laser.properties.examble";
	private org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
	private FileSystem fs;

	@BeforeTest
	public void setup() throws IOException {
		Path path = new Path(PROPERTIES);
		fs = path.getFileSystem(conf);
		Configuration.getInstance().load(path, fs);
	}

	@Test
	public void test() throws IOException, ClassNotFoundException,
			InterruptedException {
//		conf.set("mapred.job.queue.name", "sf1");
//		conf.set(CouchbaseConfig.CB_INPUT_CLUSTER, com.b5m.conf.Configuration
//				.getInstance().getCouchbaseCluster());
//		conf.set(CouchbaseConfig.CB_INPUT_BUCKET, com.b5m.conf.Configuration
//				.getInstance().getCouchbaseBucket());
//		conf.set(CouchbaseConfig.CB_INPUT_PASSWORD, com.b5m.conf.Configuration
//				.getInstance().getCouchbasePassword());
//		Job job = Job.getInstance(conf);
//		job.setJarByClass(CouchbaseInputFormatIT.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setInputFormatClass(CouchbaseInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		
//		FileOutputFormat.setOutputPath(job, new Path("couchbase"));
//
//		job.setMapperClass(CouchbaseIterationMapper.class);
//		job.setReducerClass(Reducer.class);
//		assertTrue(job.waitForCompletion(true));
	}
}
