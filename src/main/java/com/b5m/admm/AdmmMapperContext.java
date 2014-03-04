package com.b5m.admm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import static com.b5m.admm.AdmmIterationHelper.admmMapperContextToJson;
import static com.b5m.admm.AdmmIterationHelper.jsonToAdmmMapperContext;

public class AdmmMapperContext implements Writable {

	private static final double LAMBDA_VALUE = 1e-6;

	@JsonProperty("a")
	private Matrix a;

	@JsonProperty("b")
	private double[] b;

	@JsonProperty("uInitial")
	private double[] uInitial;

	@JsonProperty("xInitial")
	private double[] xInitial;

	@JsonProperty("zInitial")
	private double[] zInitial;

	@JsonProperty("rho")
	private double rho;

	@JsonProperty("lambdaValue")
	private double lambdaValue;

	@JsonProperty("primalObjectiveValue")
	private double primalObjectiveValue;

	@JsonProperty("rNorm")
	private double rNorm;

	@JsonProperty("sNorm")
	private double sNorm;

	public AdmmMapperContext(Matrix ab) {
		int numCols = ab.numCols() - 1;
		
		a = ab.like(ab.numRows(), ab.numCols() - 1);
		for (int row = 0; row < a.numRows(); row++) {
			Vector v = ab.viewRow(row);
			Vector av = a.viewRow(row);
			for (Element e : v.nonZeroes()) {
				if (numCols > e.index()) {
					av.setQuick(e.index(), e.get());
				}
			}
		}
		b = new double[ab.numRows()];
		for (int row = 0; row < ab.numRows(); row++) {
			b[row] = ab.get(row, numCols);
		}
		uInitial = new double[numCols];
		xInitial = new double[numCols];
		zInitial = new double[numCols];

		rho = 1.0;
		lambdaValue = LAMBDA_VALUE;
		primalObjectiveValue = -1;
		rNorm = -1;
		sNorm = -1;
	}

	public AdmmMapperContext(Matrix ab, double rho) {
		this(ab);
		this.rho = rho;
	}

	public AdmmMapperContext(Matrix ab, double[] uInitial, double[] xInitial,
			double[] zInitial, double rho, double lambdaValue,
			double primalObjectiveValue, double rNorm, double sNorm) {

		a = ab.like(ab.numRows(), ab.numCols() - 1);
		for (int row = 0; row < a.numRows(); row++) {
			Vector v = ab.viewRow(row);
			Vector av = a.viewRow(row);
			for (Element e : v.nonZeroes()) {
				av.setQuick(e.index(), e.get());
			}
		}
		
		b = new double[ab.numRows()];
		for (int row = 0; row < ab.numRows(); row++) {
			b[row] = ab.get(row,  ab.numCols() - 1);
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

	public AdmmMapperContext(Matrix a, double[] b, double[] uInitial,
			double[] xInitial, double[] zInitial, double rho, double lambdaValue,
			double primalObjectiveValue, double rNorm, double sNorm) {
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

	public void write(DataOutput out) throws IOException {
		Text contextJson = new Text(admmMapperContextToJson(this));
		contextJson.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		Text contextJson = new Text();
		contextJson.readFields(in);
		setAdmmMapperContext(jsonToAdmmMapperContext(contextJson.toString()));
	}

	@JsonProperty("a")
	public Matrix getA() {
		return a;
	}

	@JsonProperty("b")
	public double[] getB() {
		return b;
	}

	@JsonProperty("uInitial")
	public double[] getUInitial() {
		return uInitial;
	}

	@JsonProperty("xInitial")
	public double[] getXInitial() {
		return xInitial;
	}

	@JsonProperty("zInitial")
	public double[] getZInitial() {
		return zInitial;
	}

	@JsonProperty("rho")
	public double getRho() {
		return rho;
	}

	@JsonProperty("rho")
	public void setRho(double rho) {
		this.rho = rho;
	}

	@JsonProperty("lambdaValue")
	public double getLambdaValue() {
		return lambdaValue;
	}

	@JsonProperty("primalObjectiveValue")
	public double getPrimalObjectiveValue() {
		return primalObjectiveValue;
	}

	@JsonProperty("primalObjectiveValue")
	public void setPrimalObjectiveValue(double primalObjectiveValue) {
		this.primalObjectiveValue = primalObjectiveValue;
	}

	@JsonProperty("rNorm")
	public double getRNorm() {
		return rNorm;
	}

	@JsonProperty("rNorm")
	public void setRNorm(double rNorm) {
		this.rNorm = rNorm;
	}

	@JsonProperty("sNorm")
	public double getSNorm() {
		return sNorm;
	}

	@JsonProperty("sNorm")
	public void setSNorm(double sNorm) {
		this.sNorm = sNorm;
	}
}