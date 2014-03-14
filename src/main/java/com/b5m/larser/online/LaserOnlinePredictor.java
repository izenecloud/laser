package com.b5m.larser.online;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.mahout.math.Vector;

import com.b5m.larser.offline.DoubleIntPairWritable;
import com.b5m.larser.offline.LaserOfflineTopNResult;

public class LaserOnlinePredictor {
	private LaserOfflineTopNResult offlineTopN;
	private final LaserOnlineDelta delta;
	private final LaserOnlineEta eta;
	
	private static final int TOP_N = 1000;
	private static final int ITEM_DIMENSION = 1000000;
	private static final Random random = new Random();
	
	public LaserOnlinePredictor() {
		delta = new LaserOnlineDelta(null, null, null);
		eta = new LaserOnlineEta(null, null, null);
	}
	public List<Integer> topN(Vector user, int number) {
		List<Integer> items = new LinkedList<Integer>();
		PriorityQueue<DoubleIntPairWritable> queue = new PriorityQueue<DoubleIntPairWritable>();
		for (int i = 0; i < TOP_N; i++) {
			int itemId = random.nextInt() % ITEM_DIMENSION;
			if (0 >= itemId) {
				itemId *= -1;
			}
			double delta = this.delta.get().get(itemId);
			double eta = user.dot(this.eta.getEta(itemId));
			double val = delta+eta;
			if (queue.size() < number) {
				queue.add(new DoubleIntPairWritable(itemId, val));
			} else {
				DoubleIntPairWritable min = queue.peek();
				if (min.getValue() < val) {
					queue.poll();
					queue.add(new DoubleIntPairWritable(itemId, val));
				}
			}
		}
		Iterator<DoubleIntPairWritable> iterator = queue.iterator();
		while (iterator.hasNext()) {
			items.add(iterator.next().getKey());
		}
		return items;
	}
}
