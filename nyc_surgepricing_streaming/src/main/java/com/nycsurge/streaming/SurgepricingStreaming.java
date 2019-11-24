package com.nycsurge.streaming;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.nycsurge.constants.SurgepricingConstants;
import com.nycsurge.model.CabModel;
import com.nycsurge.utils.JavaSparksessionSingleton;
import com.nycsurge.utils.JavaStreamingContextSingleton;
import com.nycsurge.utils.SurgepricingUtils;

public class SurgepricingStreaming implements SurgepricingConstants {
	private static SparkSession sparkSession = null;
	private static JavaStreamingContext streamingContext = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		streamingContext = JavaStreamingContextSingleton.getInstance(streamingContext, 600000);
		surgePricingCalInStreaming(streamingContext, sparkSession);

	}

	public static void surgePricingCalInStreaming(JavaStreamingContext streamingContext, SparkSession sparkSession) {
		try {
			Set<String> topics = Collections.singleton("supply");
			Map<String, Object> kafkaParams = SurgepricingUtils.getKafkaParams();
			JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
					streamingContext, LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
			JavaDStream<String> dstream = directKafkaStream.map(val -> val.value());
			dstream.foreachRDD(rdd -> {
				JavaRDD<CabModel> rowRdd = rdd.map(msg -> {
					CabModel mdl = new CabModel(RowFactory.create(msg));
					return mdl;
				});

				SparkSession sparkSessionLocal = JavaSparksessionSingleton.getInstance(sparkSession);
				Dataset<Row> weatherDF = SurgepricingHelperClass.fetchWeatherDataSet(sparkSessionLocal);
				Dataset<Row> yelloCabDF = sparkSessionLocal.createDataFrame(rowRdd, CabModel.class);
				Dataset<Row> taxiWithAttrDF = SurgepricingHelperClass.deriveTripAttributes(sparkSessionLocal,
						yelloCabDF);
				SurgepricingHelperClass.writeBackSuppyDemandRatio(sparkSessionLocal, taxiWithAttrDF, weatherDF,
						SURGE_PRICING_ESTIMATION);
				SurgepricingHelperClass.writeBackTrafficCongestion(sparkSession, taxiWithAttrDF, weatherDF,
						TRAFFIC_CONGESTION);

			});

			streamingContext.start();

			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			streamingContext.stop();
		}

	}
}
