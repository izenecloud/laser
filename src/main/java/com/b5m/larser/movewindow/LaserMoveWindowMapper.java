package com.b5m.larser.movewindow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;

public class LaserMoveWindowMapper extends
		Mapper<LongWritable, VectorWritable, LongWritable, VectorWritable> {
	private Long sTime;
	private Long eTime;
	private RecordWriter<LongWritable, VectorWritable> writer;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		sTime = conf.getLong("laser.move.window.start.time", 0);
		eTime = conf.getLong("laser.move.window.end.time", 0);
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		conf.set("laser.move.window.output.name", inputSplit.getPath()
				.getName());
		try {
			writer = (RecordWriter<LongWritable, VectorWritable>) context
					.getOutputFormatClass().newInstance()
					.getRecordWriter(context);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	protected void map(LongWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		if ((key.get() >= sTime) && (key.get() <= eTime)) {
			writer.write(key, value);
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		writer.close(context);

	}
}
