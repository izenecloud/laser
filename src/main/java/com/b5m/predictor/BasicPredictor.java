package com.b5m.predictor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.regex.Pattern;

import org.apache.mahout.math.IndexException;
import org.apache.mahout.math.Matrix;

import com.b5m.conf.PropertiesLoader;

public class BasicPredictor implements IPredictor {
	double[] features;
	public BasicPredictor(String address){
		Load(address);
	}
	public void Load(String address) {
		BufferedReader br;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(address)));
			String data = br.readLine();
			if(data == null){
				System.out.println("Lost training data!");
				System.exit(0);
			}
			data = data.substring(1, data.length()-1);
			String [] featuresString = data.split(",");
			features = new double[featuresString.length];
			for(int i = 0; i < featuresString.length;  i++){
				String tmpFeature = featuresString[i];
				tmpFeature = tmpFeature.substring(1, tmpFeature.length()-1);
				double feature = Double.parseDouble(tmpFeature);
				features[i] = feature;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public double Predict(Matrix m) {
		return 0;
	}

	public void PredictTest(String path){
		
		 Pattern TAB_PATTERN = Pattern.compile("\t");
		 Pattern COLON_PATTERN = Pattern.compile(":");
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
			String data ;
			int line = 0;
			double odds = 0;
			
			while (  (data = br.readLine()) != null) {
				String[] elements = TAB_PATTERN.split(data);
				double testResult = 0;
				double predictResult = 0;
				for (int j = 0; j < elements.length - 1; ++j) {
					String[] element = COLON_PATTERN.split(elements[j]);
					Integer featureId = Integer.parseInt(element[0]);
					Double feature = Double.parseDouble(element[1]);
					odds += feature*features[featureId];
				}
				predictResult = 1/(1+Math.exp(-odds));
				if (elements.length >= 2) {
					testResult = Double.parseDouble(elements[elements.length - 1]);
				}
				System.out.println("line:"+line+" Result: "+testResult+" PredictResult: "+ predictResult);
				line++;
				if(line > 20){
					return;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*if(args.length < 2)
		{
			System.out.println("please input the property config address");
		}*/
		PropertiesLoader p = new PropertiesLoader("global.properties");
		BasicPredictor bp = new BasicPredictor(p.getTrainResultPath());
		bp.PredictTest("D://Download/logreg_features_sparse");
	
		//bp.Predict();
	}

}
