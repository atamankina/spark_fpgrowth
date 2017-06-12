/**
 * 
 */
package com.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

/**
 * @author Galina Atamankina
 *
 */
public class FPGrowthPreprocess {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("FPGrowth with Preprocessing")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> data = sc.textFile("./data/OnlineRetailShort.csv");
		
		
		

	}
	
}
