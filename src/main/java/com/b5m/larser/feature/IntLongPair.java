package com.b5m.larser.feature;

public class IntLongPair implements Comparable<IntLongPair>{

		private int first;
		private long second;

		public IntLongPair() {
		}

		public IntLongPair(int first, long second) {
			this.first = first;
			this.second = second;
		}

		public void setInt(int first) {
			this.first = first;
		}

		public void setLong(long v) {
			this.second = v;
		}

		public int getInt() {
			return first;
		}

		public long getLong() {
			return second;
		}

		public int compareTo(IntLongPair other) {
			if (first < other.first) {
				return -1;
			}
			else if (first > other.first) {
				return 1;
			}
			else if (second < other.second) {
				return -1;
			}
			else if (second > other.second) {
				return 1;
			}
			return 0;
		}
}
