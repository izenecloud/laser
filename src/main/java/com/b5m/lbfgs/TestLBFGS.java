package com.b5m.lbfgs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.regex.Pattern;

import org.apache.mahout.math.IndexException;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;

import com.b5m.admm.AdmmMapperContext;
/* copyright 2002 by Robert Dodier
*
* This program is free software; you can redistribute it and/or modify
* it under either option (1) or (2) below.
* (1) The GNU General Public License as published by the Free Software
* Foundation; either version 2 of the License, or (at your option)
* any later version.
* (2) The Apache license, version 2.
*/

import edu.stanford.nlp.optimization.DiffFunction;
import edu.stanford.nlp.optimization.QNMinimizer;


public class TestLBFGS
{
	 private static final Pattern TAB_PATTERN = Pattern.compile("\t");
	    private static final Pattern NEWLINE_PATTERN = Pattern.compile("\\n");
	    private static final Pattern SPACE_PATTERN = Pattern.compile("\\s+");
	    private static final Pattern COLON_PATTERN = Pattern.compile(":");
		public static Matrix createMatrixFromDataString(
				String dataString, int numFeatures, Set<Integer> columnsToExclude,
				boolean addIntercept) throws IndexException {
			String[] rows = NEWLINE_PATTERN.split(dataString);
			int numRows = rows.length;

			Matrix data = new SparseRowMatrix(numRows, numFeatures + 1);

			for (int i = 0; i < numRows; i++) {
				String[] elements = TAB_PATTERN.split(rows[i]);
				if (addIntercept) {
					data.set(i, 0, 1.0);
				}
				for (int j = 0; j < elements.length - 1; ++j) {
					String[] element = COLON_PATTERN.split(elements[j]);
					Integer featureId = Integer.parseInt(element[0]);
					if (columnsToExclude.contains(featureId)) {
						continue;
					}
					Double feature = Double.parseDouble(element[1]);
					try {
						data.set(i, featureId, feature);
					} catch (IndexException e) {
						throw e;
					}
				}
				if (elements.length >= 2) {
					data.set(i, numFeatures,
							Double.parseDouble(elements[elements.length - 1]));
				}
			}//System.out.println(data.toString());
			return data;
		}
		String loadData(String path) throws IOException{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			String data ;
			StringBuffer data2 = new StringBuffer();
			int line = 0;
			while (  (data = br.readLine()) != null) {
				data2.append("\n").append(data);
			}
			
			return data2.substring(1).toString();
		}
		public static void main(String[] args) throws IOException {
			// TODO Auto-generated method stub
			
			TestLBFGS  tbfgs= new TestLBFGS();
			String value=tbfgs.loadData("D://Download/sparse-logreg_features");
			Set<Integer> columnsToExclude = new HashSet();
	        Matrix inputSplitData = tbfgs.createMatrixFromDataString(value, 4, columnsToExclude, true);
	        AdmmMapperContext context = new AdmmMapperContext("0", inputSplitData);
	        LogisticL2DiffFunction myFunction =
	                new LogisticL2DiffFunction(context.getA(),
	                        context.getB(),
	                        context.getRho(),
	                        context.getUInitial(),
	                        context.getZInitial());
	        QNMinimizer lbfgs = new QNMinimizer();
	        double[] optimum = lbfgs.minimize((DiffFunction)myFunction, 1e-10, context.getXInitial());
	        System.out.println(optimum);
		}
		
}
