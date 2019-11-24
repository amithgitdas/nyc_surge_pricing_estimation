package com.nyc.surgepricing.calculator;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import com.nyc.surgepricing.constants.SurgepricingConstants;

import ch.hsr.geohash.GeoHash;

/**
 * Contains user defined functions for Deriving Geohash and distance between two
 * point in map
 */
public class SurgepricingUDF implements SurgepricingConstants {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(SurgepricingUDF.class);

	/**
	 * Geohash derivation based on Latitude and longitude
	 *
	 * @param sparkSession
	 *            sparksession instance
	 */
	public static void registerGeoHashUDF(SparkSession sparkSession) {
		sparkSession.udf().register("geohash_calc", new UDF2<Double, Double, String>() {

			private static final long serialVersionUID = 1L;

			public String call(Double longitude, Double latitude) throws Exception {
				// TODO Auto-generated method stub
				String geoHash = null;
				try {
					geoHash = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, GEO_HASH_PRECISON);
				} catch (Exception exception) {
					LOG.error(exception.getMessage());
				}
				return geoHash;
			}
		}, DataTypes.StringType);
	}

	/**
	 * Deriving distance between two points based on latitude and longitude
	 *
	 * @param sparkSession
	 *            sparksession instance
	 */
	public static void registerDistanceUDF(SparkSession sparkSession) {
		sparkSession.udf().register("distance_calc", new UDF4<Double, Double, Double, Double, Double>() {

			private static final long serialVersionUID = 1L;

			public Double call(Double lat1, Double lon1, Double lat2, Double lon2) throws Exception {
				// TODO Auto-generated method stub
				// The math module contains a function
				// named toRadians which converts from
				// degrees to radians.
				lon1 = Math.toRadians(lon1);
				lon2 = Math.toRadians(lon2);
				lat1 = Math.toRadians(lat1);
				lat2 = Math.toRadians(lat2);

				// Haversine formula
				double dlon = lon2 - lon1;
				double dlat = lat2 - lat1;
				double a = Math.pow(Math.sin(dlat / 2), 2)
						+ Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2);

				double c = 2 * Math.asin(Math.sqrt(a));

				// Radius of earth in kilometers. Use 3956
				// for miles
				double r = 3956;

				// calculate the result
				return (c * r);
			}
		}, DataTypes.DoubleType);
	}
}
