/**
 *
 */
package com.example;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Galina Atamankina
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String inputPath = "./data/OnlineRetailShort.csv";
		String pairsPath = "./data/OnlineRetailPairs";
		String pairsPathMapped = "./data/OnlineRetailPairsMapped";
		String transactionsPath = "./data/OnlineRetailTrans";
		String transactionsMappedPath = "./data/OnlineRetailTransMapped";
		String frequentItemsPath = "./data/OnlineRetailFrequentItems";
		String synthetic = "./data/SyntheticData.txt";

		/** set Spark context */
		SparkConf conf = new SparkConf()
				.setAppName("Mining Frequent Items with Preprocessing")
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		/** initialize transformer class */
		MarketBasketTransformer mbt = new MarketBasketTransformer();
		mbt.setJsc(jsc);

		/** transform input */
		JavaRDD<List<String>> transactions = mbt.transformToMarketBasketLists(inputPath, 1, 0, 1);
		transactions.saveAsTextFile(transactionsPath);

		/** run algorithm on the transformed input */
		FPGrowthRunner.runFPGrowth(transactions, 0.2, 1, frequentItemsPath);




		//JavaRDD<String> synt = jsc.textFile(synthetic, 1);
		

		//JavaRDD<String> transactions = mbt.transformToMarketBasketLines(inputPath, 1, 0, 1);

		//JavaRDD<String> transmapped = mbt.transformToMarketBasketLinesMapped(inputPath, 1, 0, 1);
		//mbt.transformToTwoTuples(inputPath, 1, 0, 1).saveAsTextFile(pairsPath);
		//mbt.transformToTwoTuplesMapped(inputPath, 1, 0, 1).saveAsTextFile(pairsPathMapped);

		//mbt.countUniquValues(inputPath, 1, 1).saveAsTextFile(transactionsPath);

		//transmapped.saveAsTextFile(transactionsMappedPath);


		jsc.stop();

	}

}
