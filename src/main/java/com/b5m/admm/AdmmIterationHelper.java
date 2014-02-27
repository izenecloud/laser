package com.b5m.admm;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public final class AdmmIterationHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern COMPILE = Pattern.compile(",");
    private static final Logger LOG = Logger.getLogger(AdmmIterationHelper.class.getName());
    private static final Pattern TAB_PATTERN = Pattern.compile("\t");
    private static final Pattern NEWLINE_PATTERN = Pattern.compile("\\n");
    private static final Pattern SPACE_PATTERN = Pattern.compile("\\s+");

    private AdmmIterationHelper() {
    }

    public static String admmMapperContextToJson(AdmmMapperContext context) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(context);
    }

    public static String admmReducerContextToJson(AdmmReducerContext context) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(context);
    }

    public static String admmStandardErrorReducerContextToJson(AdmmStandardErrorsReducerContext context) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(context);
    }

    public static String mapToJson(Map<String, String> vector) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(vector);
    }

    public static String arrayToJson(double[] array) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(array);
    }

    public static Map<String, String> jsonToMap(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, HashMap.class);
    }

    public static AdmmMapperContext jsonToAdmmMapperContext(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, AdmmMapperContext.class);
    }

    public static AdmmReducerContext jsonToAdmmReducerContext(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, AdmmReducerContext.class);
    }

    public static AdmmStandardErrorsReducerContext jsonToAdmmStandardErrorsReducerContext(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, AdmmStandardErrorsReducerContext.class);
    }

    public static double[][] createMatrixFromDataString(String dataString, Set<Integer> columnsToExclude, boolean addIntercept)
            throws ArrayIndexOutOfBoundsException {
        String[] rows = NEWLINE_PATTERN.split(dataString);
        int numRows = rows.length;
        int numColumns = SPACE_PATTERN.split(rows[0]).length - columnsToExclude.size();
        int interceptOffset = 0;
        if (addIntercept) {
            interceptOffset = 1;
        }
        double[][] data = new double[numRows][numColumns + interceptOffset];

        int[] columnArray = new int[numColumns];
        int newColumnArrayIndex = 0;
        for (int i = 0; i < SPACE_PATTERN.split(rows[0]).length; i++) {
            if (!columnsToExclude.contains(i)) {
                columnArray[newColumnArrayIndex] = i;
                newColumnArrayIndex++;
            }
        }

        for (int i = 0; i < numRows; i++) {
            String[] elements = SPACE_PATTERN.split(rows[i]);
            if (addIntercept) {
                data[i][0] = 1;
            }
            for (int j = 0; j < numColumns; j++) {
                // s1, s2, ..., y
                newColumnArrayIndex = columnArray[j];
                try {
                    data[i][j + interceptOffset] = Double.parseDouble(elements[newColumnArrayIndex]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    LOG.log(Level.FINE,
                            String.format(
                                    "i value: %d, j value: %d, newColumnArrayIndex: %d, data rows: %d, data cols: %d, elements size: %d\n",
                                    i,
                                    j,
                                    newColumnArrayIndex,
                                    data.length,
                                    data[i].length,
                                    elements.length));
                    throw e;
                }
            }
        }
        return data;
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
        if (fileString.contains("hdfs")) {
            int indexOfSecondForwardSlash = fileString.indexOf("/") + 1; // add 1 to get index of second forward slash
            int indexOfThirdForwardSlash = fileString.indexOf("/", indexOfSecondForwardSlash + 1);

            return fileString.substring(0, indexOfSecondForwardSlash) + fileString
                    .substring(indexOfThirdForwardSlash, fileString.length());
        } else {
            return fileString;
        }
    }

    public static String fsDataInputStreamToString(FSDataInputStream in, int inputSize) throws IOException {
        byte[] fileContents = new byte[inputSize];
        IOUtils.readFully(in, fileContents, 0, fileContents.length);
        String keyValue = new Text(fileContents).toString();
        return keyValue; // output from the last reduce job will be key | value
    }

    public static int getFileLength(FileSystem fs, Path thisFilePath) throws IOException {
        return (int) fs.getFileStatus(thisFilePath).getLen();
    }

    public static Map<String, String> readParametersFromHdfs(FileSystem fs, Path previousIntermediateOutputLocationPath,
                                                             int iteration) {
        Map<String, String> splitToParameters = new HashMap<String, String>();
        try {
            splitToParameters = new HashMap<String, String>();
            if (fs.exists(previousIntermediateOutputLocationPath)) {
                FileStatus[] fileStatuses = fs.listStatus(previousIntermediateOutputLocationPath);
                for (FileStatus fileStatus : fileStatuses) {
                    Path thisFilePath = fileStatus.getPath();
                    if (!thisFilePath.getName().contains("_SUCCESS") && !thisFilePath.getName().contains("_logs")) {
                        FSDataInputStream in = fs.open(thisFilePath);
                        int inputSize = getFileLength(fs, thisFilePath);
                        if (inputSize > 0) {
                            String value = fsDataInputStreamToString(in, inputSize);
                            Map<String, String> additionalSplitToParameters = jsonToMap(value);
                            splitToParameters.putAll(additionalSplitToParameters);
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
