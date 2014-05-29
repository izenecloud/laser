package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.SequentialAccessSparseVector;

import com.b5m.flume.B5MEvent;

public class WebScaleMessageConsumer extends LaserMessageConsumer {

	public WebScaleMessageConsumer(String collection, Path output,
			FileSystem fs, Configuration conf) throws IOException {
		super(collection, output, fs, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean write(B5MEvent b5mEvent) throws IOException {
		// TODO
		// user feature
		// item feature
		// item id
		// action
		append(new Text(""), new RequestWritable(
				new SequentialAccessSparseVector(),
				new SequentialAccessSparseVector(), -1));
		return false;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
	}
}
