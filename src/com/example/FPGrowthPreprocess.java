/**
 * 
 */
package com.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple1;
import scala.Tuple2;

import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * @author Galina Atamankina
 *
 */
public class FPGrowthPreprocess {
	
	public static Integer mapkey = 1;

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
		
/*		JavaRDD<String> allvalues = pairs.values();
		allvalues.saveAsTextFile(pairsPath);*/
		
		/** get the list of unique items*/
		JavaRDD<String> uniqueValues = data.mapToPair(
					(String line) -> {
						String[] lines = line.split(",");
		    			return new Tuple2<> (lines[1], 1);	
					}).reduceByKey(
					(Integer x, Integer y) -> x + y		
							).keys();
		
		/** create map of unique items and integer keys assigned to them*/
		Map<String, String> valuesMap = uniqueValues.mapToPair(
					(String key) -> {
						Integer valInt = mapkey++;
						String valString = valInt.toString();
						return new Tuple2<>(key, valString);
					}).collectAsMap();
		
		 BiMap<String, String> biMap = HashBiMap.create();
		 biMap.putAll(valuesMap);
		 //System.out.println("POST"+biMap.get("POST"));
		 //System.out.println("16"+biMap.inverse().get("16"));
		
		
		/** create tuples transaction id - integer item id*/
		JavaPairRDD<String, String> transactionKeys = data.mapToPair(
				(String line) ->{
						String[] lines = line.split(",");
						String key = valuesMap.get(lines[1]);
		    			return new Tuple2<>(lines[0], key);	
				});
		
		transactionKeys.saveAsTextFile(pairsPath);
		
		
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
		//transactions.saveAsTextFile(transactionsPath);
		
		/** FPGrowth */
		FPGrowth fpg = new FPGrowth().setMinSupport(0.1).setNumPartitions(1);
		FPGrowthModel<String> model = fpg.run(transactions);
		JavaRDD<FPGrowth.FreqItemset<String>> frequentItems = model.freqItemsets().toJavaRDD();
		
		/** save the resulting frequent items to a text file */
		frequentItems.saveAsTextFile(frequentItemsPath);
		

		
		/** stop java context */
		jsc.stop();
	}
	
}
