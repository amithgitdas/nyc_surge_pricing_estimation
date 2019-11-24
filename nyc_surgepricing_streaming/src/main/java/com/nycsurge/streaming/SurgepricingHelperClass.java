package com.nycsurge.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import com.nycsurge.constants.SurgepricingConstants;
import com.nycsurge.utils.SurgepricingUtils;


public class SurgepricingHelperClass implements SurgepricingConstants {
	/**
	 * Calculate supply vs demand ratio including weather effect and write back
	 * to MYSQL-SURGE_PRICING_ESTIMATION table
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param yelloCabDF
	 *            yellowTaxi-Data set
	 * @param weatherDF
	 *            Hourly Weather- data set
	 */
	public static void writeBackSuppyDemandRatio(SparkSession sparkSession, Dataset<Row> yelloCabDF,
			Dataset<Row> weatherDF,String tableName) {
		Dataset<Row> demandSupplyDF = deriveSupplyDemandRatio(sparkSession, yelloCabDF);
		Dataset<Row> surgeWithWeatherEffDF = corelateWeatherEffect(sparkSession, demandSupplyDF, weatherDF);
		SurgepricingUtils.writeBackToMySQL(surgeWithWeatherEffDF, tableName);
	}

	/**
	 * Calculate traffic congestion including weather effect and write back to
	 * MYSQL-TRAFFIC_CONGESTION table
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param yelloCabDF
	 *            yellowTaxi-Data set
	 * @param weatherDF
	 *            Hourly Weather- data set
	 */
	public static void writeBackTrafficCongestion(SparkSession sparkSession, Dataset<Row> yelloCabDF,
			Dataset<Row> weatherDF,String tableName) {
		Dataset<Row> trafficCongestionDF = calculateTrafficCongestion(sparkSession, yelloCabDF);
		Dataset<Row> trafficWithWeatherEffDF = corelateWeatherEffect(sparkSession, trafficCongestionDF, weatherDF);
		SurgepricingUtils.writeBackToMySQL(trafficWithWeatherEffDF, tableName);
	}

	/**
	 * Load Yello taxi dataset to spark dataframe
	 *
	 * @param sparkSession
	 *            the spark session
	 * @return the YelloCab DataFrame
	 */
	public static Dataset<Row> getYellCabDF(SparkSession sparkSession) {
		return SurgepricingUtils.getDataSetFromFile(sparkSession, YELLOW_CAB_PATH, CSV_FORMAT);
	}

	/**
	 * Derive Taxi trip releated attributes like
	 * PICKUP_GEOHASH,DROPOFF_GEOHASH,TRIP_DISTANCE,TRIP_SPEED,PICKUP_DATE,
	 * DROPOFF_DATE,PICKUP_HOUR,DROPOFF_HOUR
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param yelloCabDF
	 *            yellowTaxi-Data set
	 * @return the Yellow Taxi datafram with above derived attributes
	 */
	public static Dataset<Row> deriveTripAttributes(SparkSession sparkSession, Dataset<Row> yelloCabDF) {
		yelloCabDF = yelloCabDF.withColumn(PICKUP_DATE, yelloCabDF.col(PICKUP_DATETIME).cast(DataTypes.DateType))
				.withColumn(DROPOFF_DATE, yelloCabDF.col(DROPOFF_DATETIME).cast(DataTypes.DateType))
				.withColumn(PICKUP_HOUR, functions.hour(yelloCabDF.col(PICKUP_DATETIME)))
				.withColumn(DROPOFF_HOUR, functions.hour(yelloCabDF.col(DROPOFF_DATETIME)))
				.withColumn(DROPOFF_DATE, yelloCabDF.col(DROPOFF_DATETIME).cast(DataTypes.DateType));
		SurgepricingUDF.registerDistanceUDF(sparkSession);
		SurgepricingUDF.registerGeoHashUDF(sparkSession);
		yelloCabDF = yelloCabDF.withColumn(TRIP_DISTANCE,
				functions.callUDF(DISTANCE_CALC_UDF, yelloCabDF.col(PICKUP_LATITUDE), yelloCabDF.col(PICKUP_LONGITUDE),
						yelloCabDF.col(DROPOFF_LATITUDE), yelloCabDF.col(DROPOFF_LONGITUDE)));
		yelloCabDF = yelloCabDF.withColumn(PICKUP_GEOHASH,
				functions.callUDF(GEOHASH_CALC_UDF, yelloCabDF.col(PICKUP_LATITUDE), yelloCabDF.col(PICKUP_LONGITUDE)));
		yelloCabDF = yelloCabDF.withColumn(DROPOFF_GEOHASH, functions.callUDF(GEOHASH_CALC_UDF,
				yelloCabDF.col(DROPOFF_LATITUDE), yelloCabDF.col(DROPOFF_LONGITUDE)));
		yelloCabDF = yelloCabDF.withColumn(TRIP_SPEED,
				yelloCabDF.col(TRIP_DISTANCE).multiply(functions.lit(3600)).divide(yelloCabDF.col(TRIP_DURATION)));

		return yelloCabDF;
	}

	/**
	 * Calculate traffic congestion for historic dataset by calculating average
	 * speed of day and hour.
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param yelloCabDF
	 *            yellowTaxi-Data set
	 * @return the traffic congestion dataframe
	 */
	public static Dataset<Row> calculateTrafficCongestion(SparkSession sparkSession, Dataset<Row> yelloCabDF) {
		WindowSpec avgSpeedDay = Window.partitionBy(PICKUP_GEOHASH, PICKUP_DATE);
		WindowSpec avgSpeedHour = Window.partitionBy(PICKUP_GEOHASH, PICKUP_DATE, PICKUP_HOUR);
		yelloCabDF = yelloCabDF.withColumn(AVG_SPEED_PER_DAY,
				functions.avg(yelloCabDF.col(TRIP_SPEED)).over(avgSpeedDay));
		yelloCabDF = yelloCabDF.withColumn(AVG_SPEED_PER_HOUR,
				functions.avg(yelloCabDF.col(TRIP_SPEED)).over(avgSpeedHour));

		yelloCabDF = SurgepricingUtils.getSelectedColDF(yelloCabDF, TRAFFIC_COLS).distinct();
		yelloCabDF = yelloCabDF.withColumn(SPEED_DIFF,
				yelloCabDF.col(AVG_SPEED_PER_DAY).minus(yelloCabDF.col(AVG_SPEED_PER_HOUR)));
		yelloCabDF = yelloCabDF.withColumn(TRAFFIC_LEVEL,
				functions.when(yelloCabDF.col(SPEED_DIFF).leq(0), functions.lit(1))
						.when(yelloCabDF.col(SPEED_DIFF).between(0, 5), functions.lit(2))
						.when(yelloCabDF.col(SPEED_DIFF).gt(5), functions.lit(4))
						.otherwise(functions.lit(2)));
		Dataset<Row> trafficCongestionDF = yelloCabDF.withColumnRenamed(PICKUP_GEOHASH, GEOHASH)
				.withColumnRenamed(PICKUP_DATE, TRIP_DATE).withColumnRenamed(PICKUP_HOUR, TRIP_HOUR);

		return trafficCongestionDF;

	}

	/**
	 * Derive supply demand ratio using PICKUP_CAB_REQUESTS/DROP_OFF_CAB_REQUETS
	 * at each geohash per hour
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param yelloCabDF
	 *            yellowTaxi-Data set
	 * @return the surge pricing value
	 */
	public static Dataset<Row> deriveSupplyDemandRatio(SparkSession sparkSession, Dataset<Row> yelloCabDF) {
		Dataset<Row> pickUpCabsCountByGeoHash = yelloCabDF
				.groupBy(yelloCabDF.col(PICKUP_GEOHASH), yelloCabDF.col(PICKUP_DATE), yelloCabDF.col(PICKUP_HOUR))
				.agg(functions.count(yelloCabDF.col(PICKUP_GEOHASH)).as(PICKUP_CAB_CNT),
						functions.min(yelloCabDF.col(PICKUP_LONGITUDE)).as(PICKUP_LONGITUDE),
						functions.min(yelloCabDF.col(PICKUP_LATITUDE)).as(PICKUP_LATITUDE));
		Dataset<Row> dropOffCabsCountByGeoHash = yelloCabDF
				.groupBy(yelloCabDF.col(DROPOFF_GEOHASH), yelloCabDF.col(DROPOFF_DATE), yelloCabDF.col(DROPOFF_HOUR))
				.agg(functions.count(yelloCabDF.col(DROPOFF_GEOHASH)).as(DROPOFF_CAB_CNT));
		Dataset<Row> cabCountByGeoHash = pickUpCabsCountByGeoHash.join(dropOffCabsCountByGeoHash,
				pickUpCabsCountByGeoHash.col(PICKUP_GEOHASH).equalTo(dropOffCabsCountByGeoHash.col(DROPOFF_GEOHASH))
						.and(pickUpCabsCountByGeoHash.col(PICKUP_DATE)
								.equalTo(dropOffCabsCountByGeoHash.col(DROPOFF_DATE)))
						.and(pickUpCabsCountByGeoHash.col(PICKUP_HOUR)
								.equalTo(dropOffCabsCountByGeoHash.col(DROPOFF_HOUR))),
				INNER_JOIN);
		cabCountByGeoHash = cabCountByGeoHash.withColumn(SURGE_PRICE_INTER, functions
				.round(cabCountByGeoHash.col(PICKUP_CAB_CNT).divide(cabCountByGeoHash.col(DROPOFF_CAB_CNT)), 2));

		Dataset<Row> surgePriceDF = cabCountByGeoHash.withColumn(SURGE_PRICE,
				functions.when(cabCountByGeoHash.col(SURGE_PRICE_INTER).isNull(), functions.lit(1))
						.when(cabCountByGeoHash.col(SURGE_PRICE_INTER).gt(functions.lit(5)), functions.lit(5))
						.when(cabCountByGeoHash.col(SURGE_PRICE_INTER).lt(functions.lit(1)), functions.lit(1))
						.otherwise(cabCountByGeoHash.col(SURGE_PRICE_INTER)));
		surgePriceDF = surgePriceDF.withColumnRenamed(PICKUP_GEOHASH, GEOHASH).withColumnRenamed(PICKUP_DATE, TRIP_DATE)
				.withColumnRenamed(PICKUP_HOUR, TRIP_HOUR).withColumnRenamed(PICKUP_CAB_CNT, DEMAND)
				.withColumnRenamed(DROPOFF_CAB_CNT, SUPPLY);
		return SurgepricingUtils.getSelectedColDF(surgePriceDF, SUPPLY_DEMAND_COLS);
	}

	/**
	 * Corelate weather effect with surge pricing and traffic congestion hourly
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @param supplyTrafficDF
	 *            Traffic dataset
	 * @param weatherDF
	 *            Hourly Weather- data set
	 * @return the dataset including weather condition
	 */
	public static Dataset<Row> corelateWeatherEffect(SparkSession sparkSession, Dataset<Row> supplyTrafficDF,
			Dataset<Row> weatherDF) {

		return supplyTrafficDF
				.join(weatherDF,
						supplyTrafficDF.col(TRIP_DATE).equalTo(weatherDF.col(PICKUP_DATE))
								.and(supplyTrafficDF.col(TRIP_HOUR).equalTo(weatherDF.col(PICKUP_HOUR))),
						INNER_JOIN)
				.select(supplyTrafficDF.col("*"), weatherDF.col(ICON).as(WEATHER_CONDTION));

	}

	/**
	 * Load weather dataset from hourly weather file.
	 *
	 * @param sparkSession
	 *            sparksession instance
	 * @return the Weather dataset
	 */
	public static Dataset<Row> fetchWeatherDataSet(SparkSession sparkSession) {
		Dataset<Row> hourlyWeatherDF = SurgepricingUtils.getDataSetFromFile(sparkSession, WEATHER_PATH, CSV_FORMAT);
		hourlyWeatherDF = SurgepricingUtils.getSelectedColDF(hourlyWeatherDF, WEATHER_DATA_COLS);
		hourlyWeatherDF = hourlyWeatherDF
				.withColumn(PICKUP_DATE, hourlyWeatherDF.col(PICKUP_DATETIME).cast(DataTypes.DateType))
				.withColumn(PICKUP_HOUR, functions.hour(hourlyWeatherDF.col(PICKUP_DATETIME)));
		WindowSpec weatherWindow = Window.partitionBy(PICKUP_DATE, PICKUP_HOUR).orderBy(PICKUP_DATE);
		hourlyWeatherDF = hourlyWeatherDF.withColumn(ROW_NUM, functions.row_number().over(weatherWindow));
		hourlyWeatherDF = hourlyWeatherDF.filter(hourlyWeatherDF.col(ROW_NUM).equalTo(functions.lit(1)));
		return SurgepricingUtils.getSelectedColDF(hourlyWeatherDF, FINAL_WEATHER_SET_COLS).distinct();

	}
}
