package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.b5m.flume.B5MEvent;

public class WebScaleMessageConsumer extends LaserMessageConsumer {

	public WebScaleMessageConsumer(String collection, Path output,
			FileSystem fs, Configuration conf) throws IOException {
		super(collection, output, fs, conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean write(B5MEvent b5mEvent) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}
}
