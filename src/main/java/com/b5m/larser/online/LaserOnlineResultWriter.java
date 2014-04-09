package com.b5m.larser.online;

import static com.b5m.admm.AdmmIterationHelper.getFileLength;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import com.b5m.lr.LrIterationMapContextWritable;
import com.b5m.msgpack.MsgpackVector;
import com.b5m.msgpack.RpcClient;

import static com.b5m.HDFSHelper.*;

public class LaserOnlineResultWriter {
	public void write(Configuration conf, FileSystem hdfs, Path hdfsFilePath)
			throws IOException {
		if (!hdfs.exists(hdfsFilePath)) {
			return;
		}
		for (Path file : getFilePaths(conf, hdfs, hdfsFilePath)) {
			int inputSize = getFileLength(hdfs, file);
			if (0 >= inputSize) {
				continue;
			}
			SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, file,
					conf);
			LrIterationMapContextWritable contextWritable = new LrIterationMapContextWritable();
			IntWritable user = new IntWritable();
			while (reader.next(user, contextWritable)) {
				double[] x = contextWritable.get().getX();
				MsgpackVector eta = new MsgpackVector(x.length - 2);
				for (int i = 2; i < x.length; i++) {
					eta.set(i - 1, x[i]);
				}

				LaserOnlineModel model = new LaserOnlineModel(user.get(), x[1],
						eta);
				RpcClient.getInstance().updateLaserOnlineModel(model);

			}
			reader.close();
		}
	}
}
