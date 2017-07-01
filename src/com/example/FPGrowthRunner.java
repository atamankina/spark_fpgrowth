/**
 * 
 */
package com.example;

import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;


/**
 * @author Galina Atamankina
 *
 */
public class FPGrowthRunner {
	
	public static void runFPGrowth(JavaRDD<List<String>> transactions, double minSupport,
									int numPartitions, String outputPath){
		
		FPGrowth fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions);
		FPGrowthModel<String> model = fpg.run(transactions);
		JavaRDD<FPGrowth.FreqItemset<String>> frequentItems = model.freqItemsets().toJavaRDD();
		frequentItems.saveAsTextFile(outputPath);
		
	}

}
