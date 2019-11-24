package com.nycsurge.utils;

import org.apache.spark.sql.SparkSession;

import com.nycsurge.constants.SurgepricingConstants;

/**
 * creation of Spark Singleton instance
 */
public class JavaSparksessionSingleton implements SurgepricingConstants {

	/** The spark session instance. */
	private static volatile SparkSession sparkSessionInstance = null;

	/**
	 * Instantiates a new java sparksession singleton.
	 */
	private JavaSparksessionSingleton() {
		// do nothing
	}

	/**
	 * Gets the single instance of JavaSparksessionSingleton.
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @return single instance of sparksession
	 */
	public static SparkSession getInstance(SparkSession sparkSession) {

		if (sparkSessionInstance == null) {
			sparkSessionInstance = SparkSession.builder().appName("sqltest").master("local").getOrCreate();
			sparkSessionInstance.conf().set("spark.sql.shuffle.partitions", "10");
			sparkSessionInstance.conf().set("spark.driver.allowMultipleContexts", "true");

		}
		return sparkSessionInstance;
	}

}
