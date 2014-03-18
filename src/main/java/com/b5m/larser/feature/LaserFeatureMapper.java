package com.b5m.larser.feature;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.b5m.flume.B5MEvent;

import static com.b5m.larser.feature.LaserFeatureHelper.*;

public class LaserFeatureMapper extends
		Mapper<Writable, B5MEvent, IntLongPairWritable, VectorWritable> {
	private final int LOG_TYPE_ID = 1001;
	private final int POSITVE = 108;
	private final int NEGATIVE = 102;
	private int featureDimension;
	private final Utf8 ADS_ID = new Utf8("ol");
	private final Utf8 LOG_TYPE = new Utf8("lt");
	private final Utf8 ACTION_ID = new Utf8("ad");
	private final Utf8 CLIENT_TIMESTAMP = new Utf8("ct");

	protected void setup(Context context) throws IOException,
			InterruptedException {
		featureDimension = context.getConfiguration().getInt(
				"laser.feature.feature.dimension", 0);
	}

	protected void map(Writable key, B5MEvent value, Context context)
			throws IOException, InterruptedException {
		Map<CharSequence, CharSequence> args = value.getArgs();
		CharSequence logType = args.get(LOG_TYPE);
		if (null == logType
				|| Integer.valueOf(logType.toString()) != LOG_TYPE_ID) {
			return;
		}
		CharSequence adsId = args.get(ADS_ID);
		if (null == adsId) {
			return;
		}
		int adId = adsId.toString().hashCode();
		CharSequence ctss = args.get(CLIENT_TIMESTAMP);
		if (null == ctss) {
			return;
		}

		long cts = Long.valueOf(ctss.toString());

		IntLongPair outKey = new IntLongPair(adId, cts);

		Vector vector = new SequentialAccessSparseVector(featureDimension + 1);

		CharSequence actionId = args.get(ACTION_ID);
		if (null == actionId) {
			return;
		}
		int action = Integer.valueOf(actionId.toString());
		if (POSITVE == action) {
			vector.set(featureDimension, 1);
		} else if (NEGATIVE == action) {
			vector.set(featureDimension, -1);

		} else {
			return;
		}

		context.write(new IntLongPairWritable(outKey), new VectorWritable(
				vector));
	}

}
