package com.b5m.cluster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.minhash.HashFactory.HashType;
import org.apache.mahout.clustering.minhash.MinHashDriver;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
//import org.apache.mahout.common.MahoutTestCase;
//import org.apache.mahout.common.commandline.MinhashOptionCreator;

public class MinHashClustering {

	public static final double[][] reference = { { 1, 2, 3, 4, 5 },
			{ 2, 1, 3, 6, 7 }, { 3, 7, 6, 11, 8, 9 }, { 4, 7, 8, 9, 6, 1 },
			{ 5, 8, 10, 4, 1 }, { 6, 17, 14, 15 }, { 8, 9, 11, 6, 12, 1, 7 },
			{ 10, 13, 9, 7, 4, 6, 3 }, { 3, 5, 7, 9, 2, 11 }, { 13, 7, 6, 8, 5 } };

	private FileSystem fs;
	private Path input;
	private Path output;

	/* Convert reference points to Writable vector */
	public static List<VectorWritable> getPointsWritable(double[][] raw) {
		List<VectorWritable> points = new ArrayList<VectorWritable>();
		for (double[] fr : raw) {
			Vector vec = new SequentialAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(new VectorWritable(vec));
		}
		return points;
	}

	@Before
	public void setup() throws Exception {
		Configuration conf = new Configuration();
		
		//fs = FileSystem.get(conf);
		List<VectorWritable> points = getPointsWritable(reference);
		input = new Path("hdfs://172.16.5.192:9000/user/kevinlin/minhash/input/");
		fs = input.getFileSystem(conf);	 
		output = new Path("hdfs://172.16.5.192:9000/user/kevinlin/minhash/output/");
////		input = getTestTempDirPath("points");
//		output = new Path(getTestTempDirPath(), "output");
		Path pointFile = new Path(input, "points");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, pointFile,
				Text.class, VectorWritable.class);
		int id = 0;
		for (VectorWritable point : points) {
			writer.append(new Text("Id-" + id++), point);
		}
		writer.close();
	}
	public String optKey(String param){
		return "--"+param;
	}
	/* Make command line arguments for the Minhash driver */
	private String[] makeArguments(int minClusterSize, int minVectorSize,
			int numHashFunctions, int keyGroups, String hashType) throws IOException {
	//	String [] args = {};
		String[] args = { optKey(DefaultOptionCreator.INPUT_OPTION),
				input.toString(), optKey(DefaultOptionCreator.OUTPUT_OPTION),
				output.toString(), optKey(MinHashDriver.MIN_CLUSTER_SIZE),
				minClusterSize + "", optKey(MinHashDriver.MIN_VECTOR_SIZE),
				minVectorSize + "", optKey(MinHashDriver.HASH_TYPE), hashType,
				optKey(MinHashDriver.NUM_HASH_FUNCTIONS), numHashFunctions + "",
				optKey(MinHashDriver.KEY_GROUPS), keyGroups + "",
				optKey(MinHashDriver.NUM_REDUCERS), "1",
				optKey(MinHashDriver.DEBUG_OUTPUT) };
		System.out.println(Arrays.toString(args));
		return args;
	}

	/* get all the values of a vector */
	private Set<Integer> getValues(Vector vector) {
		Iterator<Vector.Element> itr = (Iterator<Element>) vector.all();
		Set<Integer> values = new HashSet<Integer>();
		while (itr.hasNext()) {
			values.add((int) itr.next().get());
		}
		return values;
	}

	/*
	 * Verify that the clusters in the output have item pairs none of which has
	 * similarity below the threshold 0.4
	 */
	private void verify(Path output) throws Exception {
		Configuration conf = new Configuration();
		Path outputFile = new Path(output, "part-r-00000");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, outputFile, conf);
		Text clusterId = new Text();
		VectorWritable point = new VectorWritable();
		List<Vector> clusteredItems = new ArrayList<Vector>();
		String prevClusterId = "";
		while (reader.next(clusterId, point)) {
			if (!prevClusterId.equals(clusterId.toString())) {
				if (clusteredItems.size() > 1) {
					// run pair-wise similarity test on items in a cluster
					for (int i = 0; i < clusteredItems.size(); i++) {
						Set<Integer> itemSet1 = getValues(clusteredItems.get(i));
						for (int j = i + 1; j < clusteredItems.size(); j++) {
							Set<Integer> itemSet2 = getValues(clusteredItems.get(j));
							Set<Integer> union = new HashSet<Integer>();
							union.addAll(itemSet1);
							union.addAll(itemSet2);
							Set<Integer> intersect = new HashSet<Integer>();
							intersect.addAll(itemSet1);
							intersect.retainAll(itemSet2);
							double similarity = intersect.size() / (double) union.size();
							System.out.println("similar"+similarity);
							Assert.assertTrue("Sets failed min similarity test, Set1: "
									+ itemSet1 + " Set2: " + itemSet2, similarity > 0.4);
						}
					}
				}
				clusteredItems.clear();
				prevClusterId = clusterId.toString();
			} else {
				clusteredItems.add(point.get().clone());
			}
		}
	}

	@Test
	public void testLinearMinHashMRJob() throws Exception {
		// This Fails
		// String[] args = makeArguments(2, 3, 21, 3, HashType.linear.toString());
		// This works
		 final String[] args = makeArguments(2, 3, 40, 5, HashType.LINEAR.toString());
		 System.setProperty("HADOOP_USER_NAME", "kevinlin"); 
		 System.setProperty("user.name", "kevinlin"); 

	 Configuration conf = new Configuration();
	 conf.set("hadoop.job.ugi", "kevinlin");
		int ret = ToolRunner.run(conf, new MinHashDriver(), args);
		System.out.println("Verifying linear hash results" +ret);
		verify(output);
	}

	@Test
	public void testPolynomialMinHashMRJob() throws Exception {
		String[] args = makeArguments(2, 3, 21, 3, HashType.POLYNOMIAL.toString());
		int ret = ToolRunner.run(new Configuration(), new MinHashDriver(), args);
		
		System.out.println("Verifying polynomial hash results"+ret);
		verify(output);
	}

	@Test
	public void testMurmurMinHashMRJob() throws Exception {
		String[] args = makeArguments(2, 3, 21, 3, HashType.MURMUR.toString());
		int ret = ToolRunner.run(new Configuration(), new MinHashDriver(), args);
		System.out.println("verifying murmur hash results");
		verify(output);
	}

}

