/**
 * 
 */
package com.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import scala.Tuple2;

/**
 * @author Galina Atamankina
 *
 */
public class MarketBasketTransformer {
	
	public static JavaSparkContext jsc;
	private static Map<String, String> valuesMap;
	public static BiMap<String, String> valuesBiMap;
	private static Integer mapkey = 1;

	
	
	/**
	 * @return
	 */
	public BiMap<String, String> getValuesBiMap() {
		return valuesBiMap;
	}

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
	public JavaPairRDD<String, String> transformToTwoTuplesMapped
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		JavaRDD<String> data = this.cutOffHeader(inputPath, numPartitions);
		
		JavaRDD<String> uniqueValues = data
				.mapToPair(
				(String line) -> {
					String[] lines = line.split(",");
	    			return new Tuple2<> (lines[itemIdIndex], 1);	
				}).reduceByKey(
				(Integer x, Integer y) -> x + y		
						).keys();
		
		Map<String, String> valuesMap = uniqueValues.mapToPair(
				(String key) -> {
					Integer valInt = mapkey++;
					String valString = valInt.toString();
					return new Tuple2<>(key, valString);
				}).collectAsMap();
		
		valuesBiMap = HashBiMap.create();
		valuesBiMap.putAll(valuesMap);
		
		JavaPairRDD<String, String> transactionKeys = data
				.mapToPair(
				(String line) ->{
						String[] lines = line.split(",");
						String key = valuesMap.get(lines[itemIdIndex]);
		    			return new Tuple2<>(lines[transIdIndex], key);	
				});
			
		return transactionKeys;
		
	}
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param transIdIndex
	 * @param itemIdIndex
	 * @return
	 */
	private JavaPairRDD<String, String> transformToTransactionTuples
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
	private JavaPairRDD<String, String> transformToTransactionTuplesMapped
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		
		JavaPairRDD<String, String> transactionTuplesMapped = 
				this.transformToTwoTuplesMapped(inputPath, numPartitions, transIdIndex, itemIdIndex)
				.reduceByKey(
						(String x, String y) -> x + " " + y
						);
		
		return transactionTuplesMapped;
		
	}
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param transIdIndex
	 * @param itemIdIndex
	 * @return
	 */
	public JavaRDD<String> transformToMarketBasketLinesMapped
		(String inputPath, int numPartitions, int transIdIndex, int itemIdIndex){
		
		JavaRDD<String> transactionValuesMapped = this
				.transformToTransactionTuplesMapped(inputPath, numPartitions, transIdIndex, itemIdIndex)
				.values();
		
		return transactionValuesMapped;
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
	
	/**
	 * @param inputPath
	 * @param numPartitions
	 * @param itemIdIndex
	 * @return
	 */
	public JavaPairRDD<String, Integer> countUniquValues
		(String inputPath, int numPartitions, int itemIdIndex){
	
		JavaPairRDD<String, Integer> uniqueValues = this.cutOffHeader(inputPath, numPartitions)
				.mapToPair(
				(String line) -> {
					String[] lines = line.split(",");
	    			return new Tuple2<> (lines[itemIdIndex], 1);	
				}).reduceByKey(
				(Integer x, Integer y) -> x + y	);
		return uniqueValues;
		
	}

}
