package com.b5m.admm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.b5m.admm.AdmmIterationHelper.admmMapperContextToJson;
import static com.b5m.admm.AdmmIterationHelper.jsonToAdmmMapperContext;

public class AdmmMapperContext implements Writable {

    private static final double LAMBDA_VALUE = 1e-6;

    @JsonProperty("a")
    private double[][] a;

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

    public AdmmMapperContext(double[][] ab) {
        b = new double[ab.length];
        a = new double[ab.length][ab[0].length - 1];

        for (int row = 0; row < ab.length; row++) {
            b[row] = ab[row][ab[row].length - 1];
            for (int col = 0; col < ab[row].length - 1; col++) {
                a[row][col] = ab[row][col];
            }
        }

        uInitial = new double[a[0].length];
        xInitial = new double[a[0].length];
        zInitial = new double[a[0].length];

        rho = 1.0;
        lambdaValue = LAMBDA_VALUE;
        primalObjectiveValue = -1;
        rNorm = -1;
        sNorm = -1;
    }

    public AdmmMapperContext(double[][] ab, double rho) {
        this(ab);
        this.rho = rho;
    }

    public AdmmMapperContext(double[][] ab, double[] uInitial, double[] xInitial, double[] zInitial, double rho, double lambdaValue,
                             double primalObjectiveValue, double rNorm, double sNorm) {
        b = new double[ab.length];
        a = new double[ab.length][ab[0].length - 1];

        for (int row = 0; row < ab.length; row++) {
            b[row] = ab[row][ab[row].length - 1];
            for (int col = 0; col < ab[row].length - 1; col++) {
                a[row][col] = ab[row][col];
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

    public AdmmMapperContext(double[][] a,
                             double[] b,
                             double[] uInitial,
                             double[] xInitial,
                             double[] zInitial,
                             double rho,
                             double lambdaValue,
                             double primalObjectiveValue,
                             double rNorm,
                             double sNorm) {
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
    public double[][] getA() {
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