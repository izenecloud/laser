package io.izenecloud.lr;

import io.izenecloud.larser.feature.OnlineVectorWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.stanford.nlp.optimization.QNMinimizer;

public class LrIterationMapper extends
		Mapper<Text, ListWritable, String, LaserOnlineModel> {
	private static final double DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

	private double regularizationFactor;
	private QNMinimizer lbfgs;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		regularizationFactor = conf.getDouble(
				"lr.iteration.regulariztion.factor",
				DEFAULT_REGULARIZATION_FACTOR);
		lbfgs = new QNMinimizer();
		lbfgs.setRobustOptions();
	}

	protected void map(Text key, ListWritable valueWritable, Context context)
			throws IOException, InterruptedException {
		OnlineVectorWritable[] inputSplitData = new OnlineVectorWritable[valueWritable
				.get().size()];

		List<Writable> value = valueWritable.get();
		Iterator<Writable> iterator = value.iterator();
		int row = 0;
		while (iterator.hasNext()) {
			OnlineVectorWritable v = ((OnlineVectorWritable) (iterator.next()));
			inputSplitData[row] = v;
			row++;
		}
		LrIterationMapContext mapContext = new LrIterationMapContext(
				inputSplitData);
		mapContext = localMapperOptimization(mapContext);

		double[] x = mapContext.getX();
		context.write(key.toString(), new LaserOnlineModel(x));
	}

	private LrIterationMapContext localMapperOptimization(
			LrIterationMapContext context) {
		LogisticL2DiffFunction logistic = new LogisticL2DiffFunction(
				context.getA(), context.getB(), context.getKnowOffset(),
				context.getX(), regularizationFactor);
		double[] optimum = lbfgs.minimize(logistic, 1e-6, context.getX());
		context.setX(optimum);
		return context;
	}
}
