/**
 * 
 */
package com.example;

/**
 * @author Galina Atamankina
 *
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//final version 
public class Eclat {
	/**
	 * Iteration counter
	 */
	static int iterator = 1;
	/**
	 * String return of the found FIS
	 */
	static Map<Integer, JavaPairRDD<List<Integer>, BitSet>> resultglobal = new LinkedHashMap<>();
	private static final Pattern SPACE = Pattern.compile(" ");
	static int support;
	/**
	 * counter for tids
	 */
	static int counter = -1;
	static long time;

	public static void runEclat(JavaSparkContext jsc, JavaRDD<String> transactions, int minSupport) {
		JavaSparkContext ctx = jsc;
		ctx.setLogLevel("WARN");
		time = System.currentTimeMillis();
		/**
		 * * input: args[0] path of the dataset to analyse args[1] the
		 * min_support value (positve Integer value)
		 * 
		 * read in every line of the selected textfile
		 */
		JavaRDD<String> lines = transactions;
		/**
		 * set the wanted min_sup
		 */
		support = minSupport;
		/**
		 * add tid to each transaction (one line from textfile)
		 */
		JavaPairRDD<Integer, String> transaction = lines
				.keyBy(new Function<String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(String s) {
						counter++;
						return counter;
					}
				});
		/**
		 * following two Functions get all productIDs with a BitSet from the
		 * dataset
		 */
		JavaPairRDD<List<Integer>, BitSet> pId = transaction
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, String>, List<Integer>, BitSet>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<List<Integer>, BitSet>> call(
							Tuple2<Integer, String> arg0) throws Exception {

						ArrayList<Tuple2<List<Integer>, BitSet>> call = new ArrayList<>();
						for (String a : (Arrays.asList(SPACE.split(arg0._2)))) {
							BitSet t = new BitSet(counter);
							List<Integer> b = new ArrayList<Integer>();
							int c = Integer.parseInt(a);
							b.add(c);
							t.set(arg0._1);
							call.add(new Tuple2<List<Integer>, BitSet>(b, t));
						}
						return call;
					}
				});

		JavaPairRDD<List<Integer>, BitSet> it1 = pId.reduceByKey(createTID)
				.filter(filterLength);

		if (it1 != null)
			checkFIS(it1);
		ctx.stop();
		System.out.println("****  Eclat takes: " + time / 1000 + "s. ****");

	}

	/**
	 * method returns the result of the calculation
	 */
	private static Map<Integer, JavaPairRDD<List<Integer>, BitSet>> result(
			int it) {

		if (it <= 1) {
			System.out
					.println("No FIS with the given min_support could be found");
			return null;
		}
		System.out.println("Final Eclat : "
				+ resultglobal.get(it).keys().collect());
		return resultglobal;
	}
	/**
	 * checks result of each iteration and ends it of it contains less than or
	 * equal 1 candidate
	 */
	private static void checkFIS(JavaPairRDD<List<Integer>, BitSet> it3) {
		it3.cache();
		if (it3.take(1).size() == 0) {
			time = System.currentTimeMillis() - time;
			result(iterator - 1);
		} else {
			resultglobal.put(iterator, it3);
			iterator++;
			JavaPairRDD<List<Integer>, BitSet> it = createCombinations(it3);
			it.cache();
			checkFIS(it);
		}
	}// end check FIS

	/**
	 * method creates, combines and filters candidates and returns the final set
	 * of each iteration
	 */
	private static JavaPairRDD<List<Integer>, BitSet> createCombinations(
			JavaPairRDD<List<Integer>, BitSet> it3) {

		JavaPairRDD<Tuple2<List<Integer>, BitSet>, Tuple2<List<Integer>, BitSet>> it = it3
				.cartesian(it3);

		JavaPairRDD<List<Integer>, BitSet> it1 = it
				.mapToPair(
						new PairFunction<Tuple2<Tuple2<List<Integer>, BitSet>, Tuple2<List<Integer>, BitSet>>, List<Integer>, BitSet>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<List<Integer>, BitSet> call(
									Tuple2<Tuple2<List<Integer>, BitSet>, Tuple2<List<Integer>, BitSet>> arg0)
									throws Exception {
								List<Integer> key = new ArrayList<Integer>();
								int size = arg0._1._1.size() + 1;
								if (arg0._1._1.get(0) <= arg0._2._1.get(0)) {
									if (size == 2
											|| (size > 2 && arg0._1._1
													.get(size - 2) >= arg0._2._1
													.get(0))) {
										key.addAll(0, arg0._1._1());
										int counter = 0;
										for (int i = 0; i < arg0._2._1.size(); i++) {

											if (!key.contains(arg0._2._1.get(i))) {
												counter++;
												if (counter >= 2)
													return null;
												else
													key.add(arg0._2._1.get(i));
											}
										}
										if (counter == 1) {
											Collections.sort(key);
											BitSet tid = (BitSet) arg0._1._2
													.clone();
											(tid).and(arg0._2._2);
											return new Tuple2<List<Integer>, BitSet>(
													key, tid);
										}
									}
								}
								return null;
							}
						}).filter(filterLength).distinct().cache();
		return it1;
	}

	/**
	 * used functions 1. : used for the reducebyKey() in the initial part
	 */
	static Function2<BitSet, BitSet, BitSet> createTID = new Function2<BitSet, BitSet, BitSet>() {

		private static final long serialVersionUID = 1L;

		@Override
		public BitSet call(BitSet arg0, BitSet arg1) throws Exception {
			arg0.or(arg1);
			return arg0;
		}
	};

	/**
	 * filter for reduction by min_support
	 */
	static Function<Tuple2<List<Integer>, BitSet>, Boolean> filterLength = new Function<Tuple2<List<Integer>, BitSet>, Boolean>() {
		int support1 = support;
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(Tuple2<List<Integer>, BitSet> arg0)
				throws Exception {
		//	System.out.println(support1);
			if (arg0 != null && arg0._2.cardinality() >= support1)
				return true;
			return false;
		}
	};
}
