/**
 * 
 */
package com.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
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
	
	 //   JavaRDD<Tuple2<String, String>> tuples = jsc.textFile(temppath).map();
		
		

	}
	
}
