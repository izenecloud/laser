package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class TestAdmmOptimizerDriver {
	public static void main(String[] args) throws Exception {
		Path signalPath = new Path(args[1]);
		Configuration conf = new Configuration();
		FileSystem fs = signalPath.getFileSystem(conf);
		//TrainData.randomSequence(signalPath, fs, conf);
		AdmmOptimizerDriver.run(args);
	}

}
