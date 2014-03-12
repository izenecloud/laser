package com.b5m.larser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class AC {
	private int lineLen;
	private MappedByteBuffer mappedBuffer;
	RandomAccessFile file;
	private static final Integer N = 1000;
	private static final Integer D = 10000000;
	private static final Random random = new Random();

	public AC(Path path, Configuration conf) throws IOException {
		file = new RandomAccessFile(new File(path.toString()),
				"r");
		long inputSize = file.length();
		lineLen = file.readLine().length() + 1;
		//FileChannel fc = file.getChannel();
		//mappedBuffer = fc.map(MapMode.READ_ONLY, 0, inputSize);
	}

	public String readLine(long i) throws IOException {
		long curOffset = file.getFilePointer();
		long offset = i * lineLen;
		file.seek(offset - curOffset);
		return file.readLine();
		//byte[] lineBuffer = new byte[lineLen];
		//mappedBuffer.get(lineBuffer, i * lineLen, lineLen);
		//return String.valueOf(lineBuffer);
	}
	
	public long test() throws IOException {
		long sTime = System.nanoTime();
		for (int i = 0; i < N; i++) {
			int index = random.nextInt() / D;
			if (0 > index) {
				index *= -1;
			}
			String line = readLine(index);
			//System.out.println(line);
		}
		return System.nanoTime() - sTime;
	}
}
