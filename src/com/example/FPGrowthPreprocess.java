/**
 * 
 */
package com.example;

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
		
		String inputpath = "./data/OnlineRetailShort.csv";
		String temppath = "./data/OnlineRetailTemp";
		String transactionspath = "./data/OnlineRetailTrans";
		
		SparkConf conf = new SparkConf()
				.setAppName("FPGrowth with Preprocessing")
		//this config is only good for quick local testing, otherwise passed in command line
				.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//take necessary CSV as input
		JavaRDD<String> input = jsc.textFile(inputpath);
		
		//ignore first line in CSV
		String header = input.first();
		JavaRDD<String> data = input.filter((String row) -> !row.equals(header));

	    //generate 2 element tuples
		JavaRDD<Tuple2<String, String>> tuples = data
	    		.map((String line) -> {
	    			String[] lines = line.split(",");
	    			return new Tuple2<> (lines[0], lines[1]);
	    		});

		//map data to pairs
		JavaPairRDD<String, String> pairs = data
				.mapToPair((String line) -> {
	    			String[] lines = line.split(",");
	    			return new Tuple2<> (lines[0], lines[1]);});
		

		

	}
	
}
