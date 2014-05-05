package com.b5m.msgpack;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MsgpackOutputFormat<K, V> extends OutputFormat<K, V> {

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new OutputCommitter() {
			public void abortTask(TaskAttemptContext taskContext) {
			}

			public void cleanupJob(JobContext jobContext) {
			}

			public void commitTask(TaskAttemptContext taskContext) {
			}

			public boolean needsTaskCommit(TaskAttemptContext taskContext) {
				return false;
			}

			public void setupJob(JobContext jobContext) {
			}

			public void setupTask(TaskAttemptContext taskContext) {
			}

			
			public boolean isRecoverySupported() {
				return true;
			}

			
			public void recoverTask(TaskAttemptContext taskContext)
					throws IOException {
				// Nothing to do for recovering the task.
			}
		};
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MsgpackRecordWriter<K, V>(context);
	}

}
