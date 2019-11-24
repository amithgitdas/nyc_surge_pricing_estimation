package com.surgepricing.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import com.surgepricing.constants.SurgepricingProducerConstants;

public class SurgepricingProducer implements SurgepricingProducerConstants {
	private static final Logger LOG = Logger.getLogger(SurgepricingProducer.class);

	public static void main(String[] args) {
		String topicName = "supply";
		publishMessageToProduer(topicName);

	}

	public static void publishMessageToProduer(String topicName) {
		Properties kafkaProps = setKafkaServerProperties();
		Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
		BufferedReader reader = readFileFromResources();
		try {
			for (String nycTripRecord; (nycTripRecord = reader.readLine()) != null;) {
				producer.send(new ProducerRecord<String, String>(topicName, "nyc_data", nycTripRecord), new Callback() {

					public void onCompletion(RecordMetadata metData, Exception exception) {
						if (exception != null) {
							LOG.info(exception.getMessage());
						}
					}
				});
				LOG.info("trip record sent====>" + nycTripRecord);
				System.out.println("trip record sent====>" + nycTripRecord);
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException exception) {
					LOG.error(exception.getMessage());
				}
			}
		} catch (IOException exception) {
			LOG.error(exception.getMessage());
		}
		producer.close();

	}

	public static Properties setKafkaServerProperties() {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("acks", "all");
		kafkaProps.put("retries", 0);
		kafkaProps.put("batch.size", 10);
		kafkaProps.put("linger.ms", 10);
		kafkaProps.put("buffer.memory", 33554432);
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return kafkaProps;
	}

	public static BufferedReader readFileFromResources() {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		InputStream input = loader.getResourceAsStream("nyc_taxi_data.csv");
		InputStreamReader streamReader = new InputStreamReader(input, StandardCharsets.UTF_8);
		BufferedReader reader = new BufferedReader(streamReader);
		return reader;

	}
}
