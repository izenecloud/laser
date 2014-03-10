package com.b5m.larser.demo;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ItemFeature {
	private static final Integer D = 500000;
	private static final Long N = (long) 10000000;
	private static final Integer NON_ZERO_N = 3;
	private static final Random random = new Random();
	
	static public void random(Path path, FileSystem fs) throws IOException {
		FSDataOutputStream output = fs.create(path);
		for (long i = 0; i < N; i++) {
			String feature = new String();
			for (int n = 0; n < NON_ZERO_N; n++) {
				int index = random.nextInt() / D;
				if (0 > index) {
					index *= -1;
				}
				feature += Integer.toString(index) + ":" + Double.toString(random.nextDouble()) + "\t";				
			}
			output.writeBytes(feature + "\n");
			if (i % 10000 == 0) {
				System.out.println(i);
			}
		}
		output.close();
	}
}
