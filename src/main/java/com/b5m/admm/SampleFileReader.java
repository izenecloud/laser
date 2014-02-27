package com.b5m.admm;

import org.apache.commons.lang.ArrayUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Pattern;

public class SampleFileReader {

    private final Pattern compile;
    private final String separator;
    private final Set<Integer> columnsToSkip;

    public SampleFileReader() {
        this.separator = " ";
        compile = Pattern.compile(" ");
        this.columnsToSkip = new HashSet<Integer>();
    }

    public SampleFileReader(String separator, Set<Integer> columnsToSkip) {
        this.separator = separator;
        compile = Pattern.compile(separator);
        this.columnsToSkip = new HashSet<Integer>(columnsToSkip);
    }

    // read file and return 1d array
    public double[] readLabels(String filePath) {
        List<Double> labelsArrayList = new ArrayList<Double>();
        try {
            Scanner in = new Scanner(new BufferedInputStream(new FileInputStream(filePath)));
            while (in.hasNext()) {
                labelsArrayList.add(Double.parseDouble(in.next()));
            }
        } catch (FileNotFoundException ignored) {

        }

        return ArrayUtils.toPrimitive(labelsArrayList.toArray(new Double[labelsArrayList.size()]));
    }

    // read file and return 2d array
    public double[][] readFeatures(String filePath) {
        List<List<Double>> featuresArrayList = new ArrayList<List<Double>>();
        try {
            Scanner in = new Scanner(new BufferedInputStream(new FileInputStream(filePath)));
            while (in.hasNext()) {
                List<Double> rowArrayList = new ArrayList<Double>();
                String[] stringsInRow = compile.split(in.nextLine());
                for (int colNumber = 0; colNumber < stringsInRow.length; colNumber++) {
                    String featureValue = stringsInRow[colNumber];
                    if (!columnsToSkip.contains(colNumber)) {
                        rowArrayList.add(Double.parseDouble(featureValue));
                    }
                }
                featuresArrayList.add(rowArrayList);
            }
        } catch (FileNotFoundException ignored) {

        }

        double[][] features = new double[featuresArrayList.size()][];
        int rowNumber = 0;
        for (List<Double> featureArray : featuresArrayList) {
            features[rowNumber] = ArrayUtils.toPrimitive(featureArray.toArray(new Double[featureArray.size()]));
            rowNumber++;
        }

        return features;
    }
}