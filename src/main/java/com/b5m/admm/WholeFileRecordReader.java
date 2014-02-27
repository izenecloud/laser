package com.b5m.admm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

import static org.apache.hadoop.mapred.LineRecordReader.LineReader;

/**
 * Treats keys as offset in file and value as line.
 */
public class WholeFileRecordReader implements RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(WholeFileRecordReader.class.getName());
    private static final int INT_SIZE_IN_BYTES = 4;
    private static final int B1_OFFSET = 24;
    private static final int B2_OFFSET = 16;
    private static final int B3_OFFSET = 8;
    private static final int BITWISE_AND_VALUE = 0xff;
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;

    public WholeFileRecordReader(Configuration job, FileSplit split) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            fileIn.seek(split.getLength() - INT_SIZE_IN_BYTES);
            byte b4 = fileIn.readByte();
            byte b3 = fileIn.readByte();
            byte b2 = fileIn.readByte();
            byte b1 = fileIn.readByte();
            int fileLength =
                    ((b1 & BITWISE_AND_VALUE) << B1_OFFSET) | ((b2 & BITWISE_AND_VALUE) << B2_OFFSET) | ((b3 & BITWISE_AND_VALUE) << B3_OFFSET) | (b4 & BITWISE_AND_VALUE);
            end = start + fileLength;
            fileIn.seek(0);
            in = new LineReader(codec.createInputStream(fileIn), job);
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }
        if (skipFirstLine) {
            // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    
    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    /**
     * Read a line.
     */
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        try {
            key.set(pos);
            Text lineValue = new Text();
            int newSize = 0;
            StringBuilder resultBuffer = new StringBuilder();
            while (pos < end) {
                int lineSize = in.readLine(lineValue,
                        maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
                if (lineSize == 0) {
                    return newSize > 0;
                }
                pos += lineSize;
                newSize += lineSize;
                resultBuffer.append(lineValue.toString()).append("\n");
                if (lineSize > maxLineLength) {
                    // line too long. try again
                    LOG.info("Skipped line of size " + lineSize + " at pos " + (pos - lineSize));
                }
            }
            value.set(resultBuffer.toString());
            lineValue.clear();
            return newSize > 0;
        } catch (IOException ignored) {
        }
        return false;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}