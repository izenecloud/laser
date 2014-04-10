package com.b5m.admm;

import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdmmMapperContext {
	private static final Logger LOG = LoggerFactory
			.getLogger(AdmmMapperContext.class.getName());

	private static final double LAMBDA_VALUE = 1e-6;
	private String splitId;

	private Vector[] a;

	private double[] b;

	private double[] uInitial;

	private double[] xInitial;

	private double[] zInitial;

	private double rho;

	private double lambdaValue;

	private double primalObjectiveValue;

	private double rNorm;

	private double sNorm;

	public AdmmMapperContext(String splitId, Vector[] ab) {
		LOG.info("Initialize AdmmMapperContext, splitId = {}", splitId);
		this.splitId = splitId;
		if (null != ab) {
			this.a = ab;
			int numCols = this.a[0].size() - 1;
			int numRows = this.a.length;
			b = new double[numRows];
			for (int row = 0; row < numRows; row++) {
				this.b[row] = this.a[row].get(numCols);
				this.a[row].set(numCols, 0.0);
			}

			uInitial = new double[numCols];
			xInitial = new double[numCols];
			zInitial = new double[numCols];
		}

		rho = 1.0;
		lambdaValue = LAMBDA_VALUE;
		primalObjectiveValue = -1;
		rNorm = -1;
		sNorm = -1;

		LOG.info("Initialize AdmmMapperContext, Finish");
	}

	public AdmmMapperContext(String splitId, Vector[] ab, double rho) {
		this(splitId, ab);
		this.rho = rho;
	}

	public AdmmMapperContext(String splitId, Vector[] ab, double[] uInitial,
			double[] xInitial, double[] zInitial, double rho,
			double lambdaValue, double primalObjectiveValue, double rNorm,
			double sNorm) {
		this.splitId = splitId;
		if (null != ab) {
			this.a = ab;
			int numCols = this.a[0].size() - 1;
			int numRows = this.a.length;
			b = new double[numRows];
			for (int row = 0; row < numRows; row++) {
				b[row] = a[row].get(numCols);
				a[row].set(numCols, 0.0);
			}
		}

		this.uInitial = uInitial;
		this.xInitial = xInitial;
		this.zInitial = zInitial;

		this.rho = rho;
		this.lambdaValue = lambdaValue;
		this.primalObjectiveValue = primalObjectiveValue;
		this.rNorm = rNorm;
		this.sNorm = sNorm;
	}

	public AdmmMapperContext(String splitId, Vector[] a, double[] b,
			double[] uInitial, double[] xInitial, double[] zInitial,
			double rho, double lambdaValue, double primalObjectiveValue,
			double rNorm, double sNorm) {
		this.splitId = splitId;
		this.a = a;
		this.b = b;
		this.uInitial = uInitial;
		this.xInitial = xInitial;
		this.zInitial = zInitial;
		this.rho = rho;
		this.lambdaValue = lambdaValue;
		this.primalObjectiveValue = primalObjectiveValue;
		this.rNorm = rNorm;
		this.sNorm = sNorm;
	}

	public AdmmMapperContext() {
	}

	public void setAdmmMapperContext(AdmmMapperContext context) {
		this.splitId = context.splitId;
		this.a = context.a;
		this.b = context.b;
		this.uInitial = context.uInitial;
		this.xInitial = context.xInitial;
		this.zInitial = context.zInitial;
		this.rho = context.rho;
		this.lambdaValue = context.lambdaValue;
		this.primalObjectiveValue = context.primalObjectiveValue;
		this.rNorm = context.rNorm;
		this.sNorm = context.sNorm;
	}

	public Vector[] getA() {
		return a;
	}

	public double[] getB() {
		return b;
	}

	public double[] getUInitial() {
		return uInitial;
	}

	public double[] getXInitial() {
		return xInitial;
	}

	public double[] getZInitial() {
		return zInitial;
	}

	public double getRho() {
		return rho;
	}

	public void setRho(double rho) {
		this.rho = rho;
	}

	public double getLambdaValue() {
		return lambdaValue;
	}

	public double getPrimalObjectiveValue() {
		return primalObjectiveValue;
	}

	public void setPrimalObjectiveValue(double primalObjectiveValue) {
		this.primalObjectiveValue = primalObjectiveValue;
	}

	public double getRNorm() {
		return rNorm;
	}

	public void setRNorm(double rNorm) {
		this.rNorm = rNorm;
	}

	public double getSNorm() {
		return sNorm;
	}

	public void setSNorm(double sNorm) {
		this.sNorm = sNorm;
	}

	public String getSplitId() {
		return splitId;
	}
}
