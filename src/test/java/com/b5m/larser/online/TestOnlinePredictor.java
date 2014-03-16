package com.b5m.larser.online;

import java.util.Random;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestOnlinePredictor {
	private static final int USER_FEATURE_DIMENSION = 100;
	private static final Random random = new Random();


	LaserOnlinePredictor predictor;
	@BeforeTest
	public void setup() throws Exception {
		 predictor = new LaserOnlinePredictor();
	}

	@AfterTest
	public void close() {

	}

	@Test
	public void test() {
		Vector user = new DenseVector(USER_FEATURE_DIMENSION);
		for (int col = 0; col < USER_FEATURE_DIMENSION; col++) {
			user.set(col, random.nextDouble());
		}
		for (int i = 0; i < 1000; i++) {
			predictor.topN(user, 5);
		}
	}
	
	public static void main(String[] args) {
		LaserOnlinePredictor predictor = new LaserOnlinePredictor();
		Vector user = new DenseVector(USER_FEATURE_DIMENSION);
		for (int col = 0; col < USER_FEATURE_DIMENSION; col++) {
			user.set(col, random.nextDouble());
		}
		long sTime = System.nanoTime();
		for (int i = 0; i < 1000; i++) {
			predictor.topN(user, 5);
		}
		System.out.println((System.nanoTime() - sTime) / 1000);
	}
}
