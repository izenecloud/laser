package com.b5m.larser.demo;

import java.util.Random;

public class Betas {
	private double[] betas;
	private static final Integer D = 500000;
	private static final Random random = new Random();
	public Betas() {
		betas = new double[D];
	}
	
	public static Betas randomBetas() {
		Betas b = new Betas();
		for (int i = 0; i < D; i++) {
			b.betas[i] = random.nextDouble();
		}
		return b;
	}
}
