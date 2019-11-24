package com.nyc.surgepricing.constants;

// TODO: Auto-generated Javadoc
/**
 * The Class SurgepricingSQLQyeryConstants.
 */
public class SurgepricingSQLQyeryConstants {

	/**
	 * Fetch demand and traffic.
	 *
	 * @return the string
	 */
	public static String fetchDemandAndTraffic()
	{
		return "(select distinct a.geohash,a.trip_date,a.trip_hour,a.surge_price,b.traffic_level from surge_pricing_estimation a join traffic_congestion b on a.geohash=b.geohash and a.trip_date=b.trip_date and a.trip_hour =b.trip_hour)";
	}
}
