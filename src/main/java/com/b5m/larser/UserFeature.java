package com.b5m.larser;

import java.util.Random;

public class UserFeature {
	private static final Integer D = 100;
	private static final Random random = new Random();
	public static double[] random() {
		double[] x = new double[D];
		for (int i = 0; i < D; i++) {
			x[i] = random.nextDouble();
		}
		return x;
	}
}
