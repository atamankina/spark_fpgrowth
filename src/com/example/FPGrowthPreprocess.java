/**
 * 
 */
package com.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

/**
 * @author Galina Atamankina
 *
 */
public class FPGrowthPreprocess {

	public static void main(String[] args) {
		
		String inputPath = "./data/OnlineRetailShort.csv";
		String dataPath = "./data/OnlineRetailData";
		String pairsPath = "./data/OnlineRetailPairs";
		String transTuplesPath = "./data/OnlineRetailTransTuples";
		String transValuesPath = "./data/OnlineRetailTransValues";
		String transactionsPath = "./data/OnlineRetailTrans";
		String frequentItemsPath = "./data/OnlineRetailFrequentItems";
		
		SparkConf conf = new SparkConf()
				.setAppName("FPGrowth with Preprocessing")
		//this config is only good for quick local testing, otherwise passed in command line
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/** take necessary CSV as input */		
		JavaRDD<String> input = jsc.textFile(inputPath);
		
		/** ignore first line in CSV */
		String header = input.first();
		JavaRDD<String> data = input.filter((String row) -> !row.equals(header));
		//data.saveAsTextFile(datapath);

		/** map data to pairs */
		JavaPairRDD<String, String> pairs = data.mapToPair(
					(String line) -> {
	    			String[] lines = line.split(",");
	    			return new Tuple2<> (lines[0], lines[1]);
	    			});
		//pairs.saveAsTextFile(pairspath);
		
		/** reduce pairs to transactions */
		JavaPairRDD<String, String> transactionTuples = pairs.reduceByKey(
					(String x, String y) ->
					x + " " + y
					);
		//transactionTuples.saveAsTextFile(transtuplesspath);
		
		/** extract transaction values */
		JavaRDD<String> transactionValues = transactionTuples.values();
		//transactionValues.saveAsTextFile(transvaluesspath);
		
		/** transform each transaction to a list of strings */
		JavaRDD<List<String>> transactions = transactionValues.map(
					(String line) -> {
					String[] lines = line.split(" ");
					List<String> entries = new ArrayList<String>();
				      for(int i = 0; i<= lines.length-1; i++){
				    	  if(!entries.contains(lines[i])) entries.add(lines[i]);
				      }
			        return entries;
					});
		//transactions.saveAsTextFile(transactionspath);
		
		/** FPGrowth */
		FPGrowth fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(1);
		FPGrowthModel<String> model = fpg.run(transactions);
		JavaRDD<FPGrowth.FreqItemset<String>> frequentItems = model.freqItemsets().toJavaRDD();
		
		frequentItems.saveAsTextFile(frequentItemsPath);
		
	}
	
}
