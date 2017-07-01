/**
 * 
 */
package com.example;

/**
 * @author Galina Atamankina
 *
 */
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class Apriori {
	// final version
	static int iterator = 2;
	static Map<Integer, JavaPairRDD<List<Integer>, Integer>> resultglobal = new LinkedHashMap<>();
	private static final Pattern SPACE = Pattern.compile(" ");
	static Broadcast<Integer> support;
	static JavaRDD<ArrayList<Integer>> transaction;
	static List<Integer> product = new ArrayList<Integer>();
	static List<List<Integer>> products = new ArrayList<List<Integer>>();
	static long time;

	public static void runApriori(JavaSparkContext jsc, JavaRDD<String> transactions, int minSupport) {
		
		JavaSparkContext ctx = jsc;
		JavaPairRDD<List<Integer>, Integer> finalKeys = null;
		ctx.setLogLevel("WARN");
		time = System.currentTimeMillis();
		/**
		 * input: 
		 * args[0] path of the dataset to analyse
		 * args[1] the min_support value (positve Integer value)
		 *
		 * read in every line of the selected textfile
		 */
		JavaRDD<String> lines = transactions;
		/**
		 * see package-info for input information set the wanted min_sup
		 */
		// support = ctx.broadcast(600);

		support = ctx.broadcast(minSupport);
		/**
		 * get all productIDs with count over minsup from data
		 */
		final JavaPairRDD<Integer, Integer> words = lines
				.flatMapToPair(
						new PairFlatMapFunction<String, Integer, Integer>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Iterable<Tuple2<Integer, Integer>> call(
									String s) {
								ArrayList<Tuple2<Integer, Integer>> call = new ArrayList<>();
								for (String a : (Arrays.asList(SPACE.split(s)))) {
									int i = Integer.parseInt(a);
									call.add(new Tuple2<Integer, Integer>(i, 1));
								}
								return call;
							}
						}).reduceByKey(Func).filter(intFilter);

		product = words.keys().collect();

		/**
		 * transform lines into an ArrayList including all pId's that fulfill
		 * min_sup otherwise pID will be removed from transaction removes
		 * duplicates in transactions
		 */
		transaction = lines.map(new Function<String, ArrayList<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public ArrayList<Integer> call(String s) throws Exception {
				String[] parts = s.split(" ");
				ArrayList<Integer> t = new ArrayList<Integer>();

				for (int i = 0; i <= parts.length - 1; i++) {
					int j = Integer.parseInt(parts[i]);
					if (!t.contains(j) && product.contains(j)) {
						t.add(j);
					}
				}
				return (t);
			}
		}).filter(filterNULL2).cache();

		if (!product.isEmpty() && transaction != null) {
			/**
			 * 2. iteration from R-Apriori is used here to reduce candidate
			 * generation for the 2-itemset generation
			 */
			finalKeys = transaction.flatMapToPair(it2).reduceByKey(Func)
					.filter(intFilter1).cache();

		}
		if (finalKeys != null) {
			checkFIS(finalKeys);
		} else {
			System.out.println("**** NO FIS WITH THE " + support
					+ " COULD BE FOUND");
		}
		ctx.stop();
		System.out.println("****  Apriori takes: " + time / 1000 + "s. ****");
	}

	private static Map<Integer, JavaPairRDD<List<Integer>, Integer>> result(
			int it) {
		if (it == 1) {
			System.out.println(product);
			return null;
		}
		System.out.println("Final Apriori : " + resultglobal.get(it).collect());
		return resultglobal;
	}

	/**
	 * this method checks if a another iteration needs to be done or if all FIS
	 * are minded already
	 */
	private static void checkFIS(JavaPairRDD<List<Integer>, Integer> fis) {
		fis.cache();
		if (fis.isEmpty()) {
			time = System.currentTimeMillis() - time;
			result(iterator - 1);
		} else {
			resultglobal.put(iterator, fis);
			iterator++;
			JavaPairRDD<List<Integer>, Integer> fin = createCombinations3(fis
					.keys());
			fis.unpersist();
			fin.cache();
			checkFIS(fin);
		}
	}// end check FIS

	/**
	 * This method creates next candidate set
	 */
	private static JavaPairRDD<List<Integer>, Integer> createCombinations3(
			JavaRDD<List<Integer>> fis) {
		fis.cache();

		JavaPairRDD<List<Integer>, List<Integer>> can = fis.cartesian(fis);

		JavaRDD<List<Integer>> keySet = can
				.map(new Function<Tuple2<List<Integer>, List<Integer>>, List<Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public List<Integer> call(
							Tuple2<List<Integer>, List<Integer>> arg0)
							throws Exception {

						if (arg0._1.get(0) <= arg0._2.get(0)
								&& arg0._1.get(iterator - 2) >= arg0._2.get(0)) {
							List<Integer> key = new ArrayList<Integer>();
							key.addAll(0, arg0._1());
							int counter = 0;
							for (int i = 0; i < arg0._2.size(); i++) {
								if (!key.contains(arg0._2.get(i))) {
									counter++;
									if (counter >= 2)
										return null;
									else
										key.add(arg0._2.get(i));
								}
							}
							if (counter == 1) {
								Collections.sort(key);
								return key;
							}
						}
						return null;
					}
				}).filter(filterNULL3).distinct().cache();

		products = keySet.collect();
		List<Integer> productNew = new ArrayList<>();
		productNew = keySet
				.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Integer> call(List<Integer> arg0)
							throws Exception {
						List<Integer> key = new ArrayList<>();
						key.addAll(arg0);
						return key;
					}

				}).distinct().collect();

		if (productNew.size() < product.size()) {
			product = productNew;
			/**
			 * filter removes all transaction including less items than the
			 * current k-itemset requires
			 */
			transaction = transaction.filter(
					new Function<ArrayList<Integer>, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(ArrayList<Integer> arg0)
								throws Exception {
							arg0.retainAll(product);
							if (arg0.size() >= iterator)
								return true;
							return false;
						}
					}).cache();
		}
		keySet.unpersist();
		JavaPairRDD<List<Integer>, Integer> fisNew = transaction
				.flatMapToPair(it1).reduceByKey(Func).filter(intFilter1)
				.cache();

		return fisNew;
	}

	// Functions

	static Function2<Integer, Integer, Integer> Func = new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer call(Integer arg0, Integer arg1) throws Exception {
			int i = arg0 + arg1;
			return i;
		}
	};

	static Function<Tuple2<List<Integer>, Integer>, Boolean> intFilter1 = new Function<Tuple2<List<Integer>, Integer>, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(Tuple2<List<Integer>, Integer> i) throws Exception {
			if (i != null && i._2 >= support.value())
				return true;
			return false;
		}
	};

	static Function<List<Integer>, Boolean> filterNULL3 = new Function<List<Integer>, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(List<Integer> arg0) throws Exception {

			if (arg0 != null && !arg0.isEmpty())
				return true;
			return false;
		}
	};

	static Function<ArrayList<Integer>, Boolean> filterNULL2 = new Function<ArrayList<Integer>, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(ArrayList<Integer> arg0) throws Exception {
			if (arg0 != null && arg0.size() > 1)
				return true;
			return false;
		}

	};

	static PairFlatMapFunction<ArrayList<Integer>, List<Integer>, Integer> it2 = new PairFlatMapFunction<ArrayList<Integer>, List<Integer>, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<Tuple2<List<Integer>, Integer>> call(
				ArrayList<Integer> s) throws Exception {
			List<Tuple2<List<Integer>, Integer>> candidate = new ArrayList<Tuple2<List<Integer>, Integer>>();

			for (Integer g : product) {
				if (s.contains(g)) {
					for (Integer k : s) {
						int i = g.compareTo(k);
						if (i < 0) {
							List<Integer> candi = new ArrayList<Integer>(2);
							candi.add(0, g);
							candi.add(1, k);
							candidate.add(new Tuple2<List<Integer>, Integer>(
									candi, 1));
						}
					}
				}
			}
			return candidate;
		}
	};

	static Function<Tuple2<Integer, Integer>, Boolean> intFilter = new Function<Tuple2<Integer, Integer>, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(Tuple2<Integer, Integer> i) throws Exception {

			if (i._2 >= support.value())
				return true;

			return false;
		}
	};

	static PairFlatMapFunction<ArrayList<Integer>, List<Integer>, Integer> it1 = new PairFlatMapFunction<ArrayList<Integer>, List<Integer>, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<Tuple2<List<Integer>, Integer>> call(
				ArrayList<Integer> s) throws Exception {

			List<Tuple2<List<Integer>, Integer>> candidate = new ArrayList<Tuple2<List<Integer>, Integer>>();
			for (List<Integer> g : products) {
				if (s.containsAll(g)) {
					candidate.add(new Tuple2<List<Integer>, Integer>(g, 1));
				}
			}
			return candidate;
		}
	};
}