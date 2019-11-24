package com.nycsurge.constants;

/**
 * The Interface SurgepricingConstants.
 */
public interface SurgepricingConstants {

	/** The Constant CSV_FORMAT. */
	static final String CSV_FORMAT = "csv";
	
	/** The Constant YELLOW_CAB_PATH. */
	static final String YELLOW_CAB_PATH = "C:\\Users\\AMITH DAS\\workspace\\nyc_surgepricing_calculation\\src\\main\\resources\\nyc_taxi_data.csv";
	
	/** The Constant WEATHER_PATH. */
	static final String WEATHER_PATH = "C:\\Users\\AMITH DAS\\workspace\\nyc_surgepricing_streaming\\src\\main\\resources\\weather.csv";
	
	/** The Constant GEO_HASH_PRECISON. */
	static final int GEO_HASH_PRECISON = 6;
	
	/** The Constant PICKUP_HOUR. */
	static final String PICKUP_HOUR = "pickup_hour";
	
	/** The Constant DROPOFF_HOUR. */
	static final String DROPOFF_HOUR = "dropoff_hour";
	
	/** The Constant DROPOFF_DATETIME. */
	static final String DROPOFF_DATETIME = "dropoff_datetime";
	
	/** The Constant DROPOFF_DATE. */
	static final String DROPOFF_DATE = "dropoff_date";
	
	/** The Constant PICKUP_DATETIME. */
	static final String PICKUP_DATETIME = "pickup_datetime";
	
	/** The Constant PICKUP_DATE. */
	static final String PICKUP_DATE = "pickup_date";
	
	/** The Constant TRIP_DURATION. */
	static final String TRIP_DURATION = "trip_duration";
	
	/** The Constant DROPOFF_GEOHASH. */
	static final String DROPOFF_GEOHASH = "dropoff_geohash";
	
	/** The Constant PICKUP_GEOHASH. */
	static final String PICKUP_GEOHASH = "pickup_geohash";
	
	/** The Constant TRIP_SPEED. */
	static final String TRIP_SPEED = "trip_speed";
	
	/** The Constant TRIP_DISTANCE. */
	static final String TRIP_DISTANCE = "trip_distance";
	
	/** The Constant DISTANCE_CALC_UDF. */
	static final String DISTANCE_CALC_UDF = "distance_calc";
	
	/** The Constant GEOHASH_CALC_UDF. */
	static final String GEOHASH_CALC_UDF = "geohash_calc";
	
	/** The Constant DROPOFF_LONGITUDE. */
	static final String DROPOFF_LONGITUDE = "dropoff_longitude";
	
	/** The Constant PICKUP_LONGITUDE. */
	static final String PICKUP_LONGITUDE = "pickup_longitude";
	
	/** The Constant DROPOFF_LATITUDE. */
	static final String DROPOFF_LATITUDE = "dropoff_latitude";
	
	/** The Constant PICKUP_LATITUDE. */
	static final String PICKUP_LATITUDE = "pickup_latitude";
	
	/** The Constant INNER_JOIN. */
	static final String INNER_JOIN = "inner";
	
	/** The Constant LEFT_JOIN. */
	static final String LEFT_JOIN = "left";
	
	/** The Constant DROPOFF_CAB_CNT. */
	static final String DROPOFF_CAB_CNT = "dropoff_cab_cnt";
	
	/** The Constant PICKUP_CAB_CNT. */
	static final String PICKUP_CAB_CNT = "pickup_cab_cnt";
	
	/** The Constant SURGE_PRICE. */
	static final String SURGE_PRICE = "surge_price";
	
	/** The Constant SURGE_PRICE_INTER. */
	static final String SURGE_PRICE_INTER = "surge_price_inter";
	
	/** The Constant SURGE_PRICING_ESTIMATION. */
	static final String SURGE_PRICING_ESTIMATION = "surge_pricing_estimation_streaming";
	
	/** The Constant SUPPLY_DEMAND_COLS. */
	static final String SUPPLY_DEMAND_COLS = "geohash,trip_date,trip_hour,demand,supply,surge_price,pickup_latitude,pickup_longitude";
	
	/** The Constant TRAFFIC_CONGESTION. */
	static final String TRAFFIC_CONGESTION = "traffic_congestion_streaming";

	/** The Constant FINAL_WEATHER_SET_COLS. */
	static final String FINAL_WEATHER_SET_COLS = "pickup_date,pickup_hour,icon";

	/** The Constant WEATHER_DATA_COLS. */
	static final String WEATHER_DATA_COLS = "pickup_datetime,icon";

	/** The Constant WEATHER_CONDTION. */
	static final String WEATHER_CONDTION = "weather_condtion";

	/** The Constant ICON. */
	static final String ICON = "icon";

	/** The Constant SUPPLY. */
	static final String SUPPLY = "supply";

	/** The Constant DEMAND. */
	static final String DEMAND = "demand";

	/** The Constant TRAFFIC_LEVEL. */
	static final String TRAFFIC_LEVEL = "traffic_level";

	/** The Constant TRIP_DATE. */
	static final String TRIP_DATE = "trip_date";

	/** The Constant TRIP_HOUR. */
	static final String TRIP_HOUR = "trip_hour";

	/** The Constant GEOHASH. */
	static final String GEOHASH = "geohash";

	/** The Constant HUGE. */
	static final String HUGE = "Huge";

	/** The Constant MODERATE. */
	static final String MODERATE = "Moderate";

	/** The Constant LESS. */
	static final String LESS = "Less";

	/** The Constant AVG_SPEED_PER_HOUR. */
	static final String AVG_SPEED_PER_HOUR = "avg_speed_per_hour";

	/** The Constant AVG_SPEED_PER_DAY. */
	static final String AVG_SPEED_PER_DAY = "avg_speed_per_day";

	/** The Constant TRAFFIC_COLS. */
	static final String TRAFFIC_COLS = "pickup_geohash,pickup_date,pickup_hour,avg_speed_per_day,avg_speed_per_hour,pickup_latitude,pickup_longitude";

	/** The Constant SPEED_DIFF. */
	static final String SPEED_DIFF = "speed_diff";
	static final String ROW_NUM = "row_num";
}
