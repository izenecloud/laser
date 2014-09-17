package io.izenecloud.admm;

public class AdmmReducerContext {

	private String splitId;

	// private double[] uInitial;

	// private double[] xInitial;

	// private double[] xUpdated;

	/*
	 * zInitial is only needed here for calculating the dual norm, used in the
	 * rho update.
	 * 
	 * refactor rho update
	 */

	// private double[] zInitial;

	private double[] uInitial;

	private double[] xUpdated;

	private double[] zUpdated;

	private double primalObjectiveValue;

	private double rho;

	private double lambdaValue;

	private long count;

	public AdmmReducerContext(String splitId, double[] uInitial,
			double[] xUpdated, double[] zUpdated, double primalObjectiveValue,
			double rho, double lambdaValue, long count) {
		this.splitId = splitId;
		this.uInitial = uInitial;
		this.xUpdated = xUpdated;
		this.zUpdated = zUpdated;

		// if (null == zUpdated) {
		// this.zUpdated = new double[this.xUpdated.length];
		// for (int i = 0; i < this.zUpdated.length; i++) {
		// this.zUpdated[i] = this.xUpdated[i] + this.uInitial[i];
		// }
		// } else {
		// this.zUpdated = zUpdated;
		// }

		this.primalObjectiveValue = primalObjectiveValue;
		this.rho = rho;
		this.lambdaValue = lambdaValue;
		this.count = count;
	}

	public AdmmReducerContext() {
	}

	public void setAdmmReducerContext(AdmmReducerContext context) {
		this.splitId = context.splitId;
		this.uInitial = context.uInitial;
		this.xUpdated = context.xUpdated;
		this.zUpdated = context.zUpdated;
		this.primalObjectiveValue = context.primalObjectiveValue;
		this.rho = context.rho;
		this.lambdaValue = context.lambdaValue;
		this.count = context.count;
	}

	public double[] getUInitial() {
		return uInitial;
	}

	public double[] getXUpdated() {
		return xUpdated;
	}

	public double[] getZUpdated() {
		return zUpdated;
	}

	public double getPrimalObjectiveValue() {
		return primalObjectiveValue;
	}

	public double getRho() {
		return rho;
	}

	public double getLambdaValue() {
		return lambdaValue;
	}

	public String getSplitId() {
		return splitId;
	}

	public long getCount() {
		return count;
	}
}
