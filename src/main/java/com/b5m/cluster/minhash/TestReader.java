package com.b5m.cluster.minhash;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class TestReader {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Path path = new Path("/user/alex/categories-out-5-20/part-r-00000");
		FileSystem fs = path.getFileSystem(conf);
		TextArrayWritable ta = new TextArrayWritable();
		Text t = new Text();
		Text t2 = new Text();
		IntArrayWritable ia = new IntArrayWritable();
		/*SequenceFile.Reader rd = new SequenceFile.Reader(fs, path, conf);
		
		while(rd.next(t,ta))
		{
			System.out.println("t:"+t+"ar:"+ta.toString());
			break;
		}*/
		
		Path path2 = new Path("/user/alex/categories-out-sketches-5-20/part-r-00000");
		SequenceFile.Reader rd2 = new SequenceFile.Reader(fs, path2, conf);
		int count = 0;
		while(rd2.next(t, t2)){
			System.out.println("t:"+t+"t2:"+t2);
			if(count++ > 100)
			break;
		}
	}

}
