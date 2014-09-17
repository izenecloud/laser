package io.izenecloud.admm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static io.izenecloud.HDFSHelper.*;

public final class AdmmIterationHelper {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final Pattern COMPILE = Pattern.compile(",");
	private static final Logger LOG = Logger
			.getLogger(AdmmIterationHelper.class.getName());

	private AdmmIterationHelper() {
	}

	@SuppressWarnings("deprecation")
	public static AdmmMapperContext readPreviousAdmmMapperContext(
			String splitId, Path previousIntermediateOutputLocationPath,
			FileSystem fs, Configuration conf) throws IOException {
		Path previousUPath = new Path(previousIntermediateOutputLocationPath,
				"U-" + splitId);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, previousUPath,
				conf);
		Writable key = NullWritable.get();
		DoubleArrayWritable uval = new DoubleArrayWritable();
		reader.next(key, uval);
		double[] uInitial = uval.get();
		reader.close();

		Path previousXPath = new Path(previousIntermediateOutputLocationPath,
				"X-" + splitId);
		reader = new SequenceFile.Reader(fs, previousXPath, conf);
		DoubleArrayWritable xval = new DoubleArrayWritable();
		reader.next(key, xval);
		double[] xUpdated = xval.get();
		reader.close();

		Path previousZPath = new Path(previousIntermediateOutputLocationPath,
				"Z");
		SequenceFile.Reader reduceContextReader = new SequenceFile.Reader(fs,
				previousZPath, conf);
		AdmmReducerContextWritable reduceContextWritable = new AdmmReducerContextWritable();
		reduceContextReader.next(key, reduceContextWritable);
		reduceContextReader.close();

		AdmmReducerContext reduceContext = reduceContextWritable.get();
		double[] zUpdated = reduceContext.getZUpdated();
		for (int i = 0; i < zUpdated.length; i++) {
			uInitial[i] += xUpdated[i] - zUpdated[i]; // uUpdated
		}

		AdmmMapperContext mapperContext = new AdmmMapperContext(null, null,
				uInitial, xUpdated, zUpdated, reduceContext.getRho(),
				reduceContext.getLambdaValue(),
				reduceContext.getPrimalObjectiveValue(), 0.0, 0.0);
		return mapperContext;
	}

	@SuppressWarnings("deprecation")
	public static double calculateS(Path prevOutput, FileSystem fs,
			Configuration conf, double[] xUpdatedAverage) throws IOException {
		double[] xPreviousAverage = null;
		Writable key = NullWritable.get();
		long count = 0;

		DoubleArrayWritable val = new DoubleArrayWritable();
		for (Path file : getFilePaths(prevOutput, "X-*", fs)) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			reader.next(key, val);
			double[] xUpdated = val.get();
			if (null == xPreviousAverage) {
				xPreviousAverage = new double[xUpdated.length];
			}
			for (int i = 0; i < xUpdatedAverage.length; i++) {
				xPreviousAverage[i] += xUpdated[i];
			}
			reader.close();
			count++;
		}

		double result = 0.0;
		// iteration 0
		if (null == xPreviousAverage) {

			for (int i = 0; i < xUpdatedAverage.length; i++) {
				result += Math.pow(xUpdatedAverage[i], 2);
			}

		} else {

			for (int i = 0; i < xPreviousAverage.length; i++) {
				xPreviousAverage[i] /= count;
			}

			for (int i = 0; i < xPreviousAverage.length; i++) {
				result += Math.pow(xPreviousAverage[i] - xUpdatedAverage[i], 2);
			}
		}

		return result;
	}

	public static double calculateR(Path output, FileSystem fs,
			Configuration conf, double[] xUpdatedAverage) throws IOException {
		double result = 0.0;
		Writable key = NullWritable.get();
		DoubleArrayWritable val = new DoubleArrayWritable();
		for (Path file : getFilePaths(output, "X-*", fs)) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			reader.next(key, val);
			double[] thisXUpdated = val.get();
			for (int j = 0; j < xUpdatedAverage.length; j++) {
				result += Math.pow(thisXUpdated[j] - xUpdatedAverage[j], 2);
			}
			reader.close();
		}
		return result;
	}

	public static AdmmReducerContext readPreviousAdmmReducerContext(
			Path previousIntermediateOutputLocationPath, FileSystem fs,
			Configuration conf) throws IOException {
		Path previousZPath = new Path(previousIntermediateOutputLocationPath,
				"<Z>");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, previousZPath,
				conf);
		AdmmReducerContextWritable reduceContextWritable = new AdmmReducerContextWritable();
		Writable key = NullWritable.get();

		reader.next(key, reduceContextWritable);
		reader.close();
		return reduceContextWritable.get();
	}

	public static String admmMapperContextToJson(AdmmMapperContext context)
			throws IOException {
		return OBJECT_MAPPER.writeValueAsString(context);
	}

	public static String admmReducerContextToJson(AdmmReducerContext context)
			throws IOException {
		return OBJECT_MAPPER.writeValueAsString(context);
	}

	public static String mapToJson(Map<String, String> vector)
			throws IOException {
		return OBJECT_MAPPER.writeValueAsString(vector);
	}

	public static String arrayToJson(double[] array) throws IOException {
		return OBJECT_MAPPER.writeValueAsString(array);
	}

	public static double[] jsonToArray(String json) throws IOException {
		return OBJECT_MAPPER.readValue(json, double[].class);
	}

	public static Map<String, String> jsonToMap(String json) throws IOException {
		return OBJECT_MAPPER.readValue(json, HashMap.class);
	}

	public static AdmmMapperContext jsonToAdmmMapperContext(String json)
			throws IOException {
		return OBJECT_MAPPER.readValue(json, AdmmMapperContext.class);
	}

	public static AdmmReducerContext jsonToAdmmReducerContext(String json)
			throws IOException {
		return OBJECT_MAPPER.readValue(json, AdmmReducerContext.class);
	}

	public static Set<Integer> getColumnsToExclude(String columnsToExcludeString) {
		String[] columnsToExcludeArray;
		if (columnsToExcludeString == null || columnsToExcludeString.isEmpty()) {
			columnsToExcludeArray = new String[0];
		} else {
			columnsToExcludeArray = COMPILE.split(columnsToExcludeString);
		}

		Set<Integer> columnsToExclude = new HashSet<Integer>();
		for (String col : columnsToExcludeArray) {
			columnsToExclude.add(Integer.parseInt(col));
		}
		return columnsToExclude;
	}

	public static String removeIpFromHdfsFileName(String fileString) {
		return Integer.toString(fileString.hashCode());
	}

	public static String fsDataInputStreamToString(FSDataInputStream in,
			int inputSize) throws IOException {
		byte[] fileContents = new byte[inputSize];
		IOUtils.readFully(in, fileContents, 0, fileContents.length);
		String keyValue = new Text(fileContents).toString();
		return keyValue; // output from the last reduce job will be key | value
	}

	public static int getFileLength(FileSystem fs, Path thisFilePath)
			throws IOException {
		return (int) fs.getFileStatus(thisFilePath).getLen();
	}

	public static Map<String, String> readParametersFromHdfs(FileSystem fs,
			Path previousIntermediateOutputLocationPath, int iteration) {
		Map<String, String> splitToParameters = new HashMap<String, String>();
		try {
			splitToParameters = new HashMap<String, String>();
			if (fs.exists(previousIntermediateOutputLocationPath)) {
				FileStatus[] fileStatuses = fs
						.listStatus(previousIntermediateOutputLocationPath);
				for (FileStatus fileStatus : fileStatuses) {
					Path thisFilePath = fileStatus.getPath();
					if (!thisFilePath.getName().contains("_SUCCESS")
							&& !thisFilePath.getName().contains("_logs")) {
						FSDataInputStream in = fs.open(thisFilePath);
						int inputSize = getFileLength(fs, thisFilePath);
						if (inputSize > 0) {
							String value = fsDataInputStreamToString(in,
									inputSize);
							Map<String, String> additionalSplitToParameters = jsonToMap(value);
							splitToParameters
									.putAll(additionalSplitToParameters);
						}
					}
				}
			}
		} catch (IOException e) {
			LOG.log(Level.FINE, e.toString());
		}
		return splitToParameters;
	}
}
