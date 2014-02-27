package com.b5m.admm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class SignalInputFormat extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;

    /*
    Implement this per JobConfigurable interface
     */
    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    /*
    Override this so that files are not split up - each mapper should work on a whole file
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
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