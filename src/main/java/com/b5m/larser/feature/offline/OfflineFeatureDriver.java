package com.b5m.larser.feature.offline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OfflineFeatureDriver {
	public static int run(Path input, Path output, Configuration conf)
			throws IOException {
		FileSystem fs = input.getFileSystem(conf);
		fs.rename(input, new Path(output, input.getName()));
		return 0;
	}
}
