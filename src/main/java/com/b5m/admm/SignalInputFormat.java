package com.b5m.admm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class SignalInputFormat extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable {
	private JobConf conf;

    private CompressionCodecFactory compressionCodecs = null;

    /*
    Implement this per JobConfigurable interface
     */
    public void configure(JobConf conf) {
    	this.conf = conf;
        compressionCodecs = new CompressionCodecFactory(conf);
    }
    @Override
    protected long computeSplitSize(long goalSize, long minSize, long blockSize) {
    	int numMapTasks = conf.getInt("mapred.num.map.tasks", 1);
    	long splitSize = goalSize / numMapTasks;
    	long mapperJvmHeapSize = conf.getLong("mapred.mapper.jvm.heap.size", blockSize);
    	return Math.max(minSize, Math.min(splitSize, mapperJvmHeapSize));
    }
    
    /*
    Return a record reader that will cause each map task to operate on an entire file (and not just a single line)
     */
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        return new WholeFileRecordReader(job, (FileSplit) genericSplit);
    }
}