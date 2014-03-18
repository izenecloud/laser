package com.b5m.larser.movewindow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class LaserMoveWindowOutputFormat<K, V> extends
		SequenceFileOutputFormat<K, V> {

	protected SequenceFile.Writer getSequenceWriter(TaskAttemptContext context,
			Class<?> keyClass, Class<?> valueClass) throws IOException {
		Configuration conf = context.getConfiguration();

		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		if (getCompressOutput(context)) {
			// find the kind of compression to do
			compressionType = getOutputCompressionType(context);
			// find the right codec
			Class<?> codecClass = getOutputCompressorClass(context,
					DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
		}
		// get the path of the temporary output file
		Path file = getDefaultWorkFile(context, "");
		FileSystem fs = file.getFileSystem(conf);
		return SequenceFile.createWriter(fs, conf, file, keyClass, valueClass,
				compressionType, codec, context);
	}

	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		String outputName = context.getConfiguration().get(
				"laser.move.window.output.name");
		if (null == outputName) {
			return new Path(committer.getWorkPath(), getUniqueFile(context,
					getOutputName(context), extension));
		}
		return new Path(committer.getWorkPath(), outputName);
	}
}
