package com.nycsurge.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaStreamingContextSingleton {
	private static volatile JavaStreamingContext streamingContext = null;

	/**
	 * Instantiates a new java sparksession singleton.
	 */
	private JavaStreamingContextSingleton() {
		// do nothing
	}

	/**
	 * Gets the single instance of JavaSparksessionSingleton.
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @return single instance of sparksession
	 */
	public static JavaStreamingContext getInstance(JavaStreamingContext streamingContext,int duration) {

		if (streamingContext == null) {
			SparkConf conf = new SparkConf().setAppName("nyc_streaming").setMaster("local");
			conf.set("spark.driver.allowMultipleContexts", "true");
			streamingContext = new JavaStreamingContext(conf, new Duration(duration));

		}
		return streamingContext;
	}
}
