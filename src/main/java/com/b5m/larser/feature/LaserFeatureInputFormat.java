package com.b5m.larser.feature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.b5m.flume.B5MEvent;

public class LaserFeatureInputFormat extends FileInputFormat<LongWritable, B5MEvent>{

	@Override
	public RecordReader<LongWritable, B5MEvent> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new LaserFeatureRecordReader();
	}

}
