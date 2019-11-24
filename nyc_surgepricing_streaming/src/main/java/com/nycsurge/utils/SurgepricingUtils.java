package com.nycsurge.utils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SurgepricingUtils {
	private static final Logger LOG = Logger.getLogger(SurgepricingUtils.class);

	/**
	 * Reading the file from path to dataframe
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param filePath
	 *            the file path
	 * @param format
	 *            the format
	 * @return the dataframe
	 */
	public static Dataset<Row> getDataSetFromFile(SparkSession sparkSession, String filePath, String format) {
		return sparkSession.read().format(format).option("inferSchema", "true").option("header", "true").load(filePath);
	}

	/**
	 * Select the dataframe for given comma seperated column values
	 *
	 * @param inputDF
	 *            Input dataframe
	 * @param ipcols
	 *            Columns for selection
	 * @return the dataframe with selected columns
	 */
	public static Dataset<Row> getSelectedColDF(Dataset<Row> inputDF, String ipcols) {
		List<Column> colList = new ArrayList<>();
		Column[] cols = new Column[ipcols.split(",").length];
		for (String columName : ipcols.split(",")) {
			Column col = new Column(columName);
			colList.add(col);
		}
		colList.toArray(cols);
		return inputDF.select(cols);
	}
	
	/**
	 * Write back to my SQL table.
	 *
	 * @param outputDF
	 *            the output DF
	 * @param table
	 *            the table
	 */
	public static void writeBackToMySQL(Dataset<Row> outputDF, String table) {
		Map<String, String> options = getMariDBConfigMap();
		Properties connProp = new Properties();
		connProp.put("driver", options.get("driver"));
		connProp.put("url", options.get("url"));
		outputDF.write().format("jdbc").mode(SaveMode.Append).jdbc(options.get("url"), table, connProp);
	}
	
	/**
	 * Gets the mari DB config map.
	 *
	 * @return the mari DB config map
	 */
	public static Map<String, String> getMariDBConfigMap() {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		InputStream input = loader.getResourceAsStream("conn.properties");
		Properties prop = new Properties();
		Map<String, String> options = new HashMap<>();
		try {
			prop.load(input);
			final String mysqlURL = prop.getProperty("CONN_URL");
			options.put("driver", prop.getProperty("MYSQL_DRIVER"));
			options.put("url", mysqlURL);
		} catch (Exception exception) {

		}
		return options;
	}
	
	 public static Map<String, Object> getKafkaParams() {
	        Map<String, Object> kafkaParams = new HashMap<>();
	        kafkaParams.put("bootstrap.servers", "localhost:9092");
	        kafkaParams.put("key.deserializer", StringDeserializer.class);
	        kafkaParams.put("value.deserializer", StringDeserializer.class);
	        kafkaParams.put("group.id", "DEFAULT_GROUP_ID");
	        kafkaParams.put("auto.offset.reset", "earliest");
	        kafkaParams.put("enable.auto.commit", false);
	        return kafkaParams;
	    }
}
