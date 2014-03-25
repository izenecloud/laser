package com.b5m.larser.online;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.b5m.admm.TrainDataRecordReader;

public class OnlineTrainInputFormat extends InputFormat<Writable, Writable> {

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();

		Configuration conf = job.getConfiguration();
		Path[] inputPath = FileInputFormat.getInputPaths(job);

		int sample = conf.getInt("com.b5m.laser.online.item.dimension", 0);
		int numMapTasks = conf.getInt("com.b5m.laser.online.num.mapTasks", 0);
		int samplePerMapTask = sample / numMapTasks;
		conf.setInt("com.b5m.laser.online.sample.per.mapTask", samplePerMapTask);
		for (int i = 0; i < numMapTasks; i++) {
			splits.add(new FileSplit(inputPath[0], 0, 1, null));
		}
		return splits;
	}

	@Override
	public RecordReader<Writable, Writable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new TrainDataRecordReader();
	}
}
