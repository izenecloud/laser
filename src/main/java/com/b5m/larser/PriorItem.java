package com.b5m.larser;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class PriorItem {
	private static final Integer N = 1000;
	private static final Integer D = 10000000;
	private static final Random random = new Random();

	public static List<Long> random() {
		List<Long> items = new LinkedList<Long>();
		for (int i = 0; i < N; i++) {
			Long id = (long)(random.nextInt() / D);
			if (id < 0) {
				id *= -1;
			}
			items.add((long)i);
		}
		return items;
	}
}
