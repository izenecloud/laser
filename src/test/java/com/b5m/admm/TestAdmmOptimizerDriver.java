package com.b5m.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestAdmmOptimizerDriver {
	public static void main(String[] args) throws Exception {
		// AdmmOptimizerDriverArguments admmOptimizerDriverArguments = new
		// AdmmOptimizerDriverArguments();
		// AdmmOptimizerDriver.parseArgs(args, admmOptimizerDriverArguments);
		//
		// String signalDataLocation = admmOptimizerDriverArguments
		// .getSignalPath();
		// Path signalPath = new Path(signalDataLocation);
		// Configuration conf = new Configuration();
		// FileSystem fs = signalPath.getFileSystem(conf);
		// TrainData.randomSequence(signalPath, fs, conf);
		// TrainData.randomSequence(new Path("args"), signalPath, conf);
		// TrainData.random(signalPath, fs);
		AdmmOptimizerDriver.run(args);
	}

}
