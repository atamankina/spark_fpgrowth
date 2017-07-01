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
		
		/** input and output paths */
		String inputPath = "./data/OnlineRetailShort.csv";
		String dataPath = "./data/OnlineRetailData";
		String pairsPath = "./data/OnlineRetailPairs";
		String transTuplesPath = "./data/OnlineRetailTransTuples";
		String transValuesPath = "./data/OnlineRetailTransValues";
		String transactionsPath = "./data/OnlineRetailTrans";
		String frequentItemsPath = "./data/OnlineRetailFrequentItems";
				
		/** create spark config and java context */
		SparkConf conf = new SparkConf()
				.setAppName("FPGrowth with Preprocessing")
		//this config is only good for quick local testing, otherwise passed in command line
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/** take necessary CSV as input */
		//for testing small data set, one partition is used, should be changed for bigger data set
		JavaRDD<String> input = jsc.textFile(inputPath, 1);
		
		/** remove first line in CSV */
		String header = input.first();
		JavaRDD<String> data = input.filter((String row) -> !row.equals(header));
		//data.saveAsTextFile(dataPath);

		/** map data to pairs transaction id - item id */
		JavaPairRDD<String, String> pairs = data.mapToPair(
					(String line) -> {
	    			String[] lines = line.split(",");
	    			return new Tuple2<> (lines[0], lines[1]);
	    			});
		//pairs.saveAsTextFile(pairsPath);
		
		/** reduce pairs to transactions by transaction id */
		JavaPairRDD<String, String> transactionTuples = pairs.reduceByKey(
					(String x, String y) -> x + " " + y
					);
		//transactionTuples.saveAsTextFile(transTuplesPath);
		
		/** extract transaction values, remove transaction ids */
		JavaRDD<String> transactionValues = transactionTuples.values();
		//transactionValues.saveAsTextFile(transValuesPath);
		
		/** transform each transaction to a list of items */
		JavaRDD<List<String>> transactions = transactionValues.map(
					(String line) -> {
					String[] lines = line.split(" ");
					List<String> entries = new ArrayList<String>();
				        for(String item:lines){
				    	    if(!entries.contains(item)) entries.add(item);
				        }
			        return entries;
					});
		transactions.saveAsTextFile(transactionsPath);
		
		/** FPGrowth */
		FPGrowth fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(1);
		FPGrowthModel<String> model = fpg.run(transactions);
		JavaRDD<FPGrowth.FreqItemset<String>> frequentItems = model.freqItemsets().toJavaRDD();
		
		/** save the resulting frequent items to a text file */
		frequentItems.saveAsTextFile(frequentItemsPath);
		
		/** stop java context */
		jsc.stop();
	}
	
}
