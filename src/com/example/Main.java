/**
 * 
 */
package com.example;

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
		String transactionsPath = "./data/OnlineRetailTrans";
		String frequentItemsPath = "./data/OnlineRetailFrequentItems";
		String synthetic = "./data/SyntheticData.txt";
		
		SparkConf conf = new SparkConf()
				.setAppName("Mining Frequent Items with Preprocessing")
		//this config is only good for quick local testing, otherwise passed in command line
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		MarketBasketTransformer mbt = new MarketBasketTransformer();
		mbt.setJsc(jsc);
		
		JavaRDD<String> transactions = mbt.transformToMarketBasketLines(inputPath, 1, 0, 1);
		
		//transactions.saveAsTextFile(transactionsPath);
		
		JavaRDD<String> synt = jsc.textFile(synthetic, 1);
		
		Eclat.runEclat(jsc, synt, 2);
		//Apriori.runApriori(jsc, synt, 2);
		
		

	}

}
