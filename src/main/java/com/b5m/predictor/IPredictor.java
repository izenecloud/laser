package com.b5m.predictor;

import org.apache.mahout.math.Matrix;

public interface IPredictor {
	void Load(String path);
	double Predict(Matrix m);
}
