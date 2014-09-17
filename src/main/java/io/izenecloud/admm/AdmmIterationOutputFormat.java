package io.izenecloud.admm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class AdmmIterationOutputFormat<K, V> extends
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
		Class<?> valClass = conf.getClass(
				"com.b5m.admm.iteration.output.class", valueClass);
		return SequenceFile.createWriter(fs, conf, file, keyClass, valClass,
				compressionType, codec, context);
	}

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {

		final SequenceFile.Writer out = getSequenceWriter(context,
				context.getOutputKeyClass(), context.getOutputValueClass());

		return new RecordWriter<K, V>() {

			public void write(K key, V value) throws IOException {

				out.append(key, value);
			}

			public void close(TaskAttemptContext context) throws IOException {
				out.close();
			}
		};
	}

	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		String outputName = context.getConfiguration().get(
				"com.b5m.admm.iteration.output.name");
		if (null == outputName) {
			return new Path(committer.getWorkPath(), "Z");
		}
		return new Path(FileOutputFormat.getOutputPath(context), outputName);
	}
}
