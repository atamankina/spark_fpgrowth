/**
 * 
 */
package com.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Galina Atamankina
 *
 */
public class MarketBasketTransformer {
	
	public JavaSparkContext jsc;

	/**
	 * @param jsc
	 */
	public void setJsc(JavaSparkContext jsc) {
		this.jsc = jsc;
	}

	/**
	 * @param inputPath
	 * @param numPartitions
	 * @return
	 */
	private JavaRDD<String> cutOffHeader(String inputPath, int numPartitions){
		
		JavaRDD<String> input = jsc.textFile(inputPath, numPartitions);
		String header = input.first();
		JavaRDD<String> data = input.filter((String row) -> !row.equals(header));
		
		return data;
		
	}
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param transIdIndex
	 * @param itemIdIndex
	 * @return
	 */
	public JavaPairRDD<String, String> transformToTwoTuples
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		JavaPairRDD<String, String> pairs = this.cutOffHeader(inputPath, numPartitions)
				.mapToPair((String line) -> {
		    			String[] lines = line.split(",");
		    			return new Tuple2<> (lines[transIdIndex], lines[itemIdIndex]);
		    			});
		
		return pairs;
	}
	
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param transIdIndex
	 * @param itemIdIndex
	 * @return
	 */
	public JavaPairRDD<String, String> transformToTransactionTuples
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		JavaPairRDD<String, String> transactionTuples = 
				this.transformToTwoTuples(inputPath, numPartitions, transIdIndex, itemIdIndex)
				.reduceByKey(
						(String x, String y) -> x + " " + y
						);
		
		return transactionTuples;
	}
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param transIdIndex
	 * @param itemIdIndex
	 * @return
	 */
	public JavaRDD<String> transformToMarketBasketLines
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		JavaRDD<String> transactionValues = this
				.transformToTransactionTuples(inputPath, numPartitions, transIdIndex, itemIdIndex)
				.values();
		
		return transactionValues;
		
	}
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param transIdIndex
	 * @param itemIdIndex
	 * @return
	 */
	public JavaRDD<List<String>> transformToMarketBasketLists
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		JavaRDD<List<String>> transactionLists = this
				.transformToMarketBasketLines(inputPath, numPartitions, transIdIndex, itemIdIndex)
				.map((String line) -> {
					String[] lines = line.split(" ");
					List<String> entries = new ArrayList<String>();
				        for(String item:lines){
				    	    if(!entries.contains(item)) entries.add(item);
				        }
			        return entries;
					});
		
		
		return transactionLists;
		
	}

}
