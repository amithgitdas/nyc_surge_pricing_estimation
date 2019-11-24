package com.nyc.surgepricing.model;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.spark.sql.Row;

public class CabModel implements Serializable {
	private static final long serialVersionUID = 1L;
	SimpleDateFormat datetimeformat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
	private String id;
	private String vendor_id;
	private Timestamp pickup_datetime;
	private Timestamp dropoff_datetime;
	private Date pickup_date;
	private Date dropoff_date;
	private String passenger_count;
	private double pickup_longitude;
	private double pickup_latitude;
	private double dropoff_longitude;
	private double dropoff_latitude;
	private String store_and_fwd_flag;
	private Integer trip_duration;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getVendor_id() {
		return vendor_id;
	}

	public void setVendor_id(String vendor_id) {
		this.vendor_id = vendor_id;
	}

	

	public String getPassenger_count() {
		return passenger_count;
	}

	public void setPassenger_count(String passenger_count) {
		this.passenger_count = passenger_count;
	}

	public double getPickup_longitude() {
		return pickup_longitude;
	}

	public void setPickup_longitude(double pickup_longitude) {
		this.pickup_longitude = pickup_longitude;
	}

	public double getPickup_latitude() {
		return pickup_latitude;
	}

	public void setPickup_latitude(double pickup_latitude) {
		this.pickup_latitude = pickup_latitude;
	}

	public double getDropoff_longitude() {
		return dropoff_longitude;
	}

	public void setDropoff_longitude(double dropoff_longitude) {
		this.dropoff_longitude = dropoff_longitude;
	}

	public double getDropoff_latitude() {
		return dropoff_latitude;
	}

	public void setDropoff_latitude(double dropoff_latitude) {
		this.dropoff_latitude = dropoff_latitude;
	}

	public String getStore_and_fwd_flag() {
		return store_and_fwd_flag;
	}

	public void setStore_and_fwd_flag(String store_and_fwd_flag) {
		this.store_and_fwd_flag = store_and_fwd_flag;
	}

	public Integer getTrip_duration() {
		return trip_duration;
	}

	public void setTrip_duration(Integer trip_duration) {
		this.trip_duration = trip_duration;
	}

	public Date getPickup_date() {
		return pickup_date;
	}

	public void setPickup_date(Date pickup_date) {
		this.pickup_date = pickup_date;
	}

	public Date getDropoff_date() {
		return dropoff_date;
	}

	public void setDropoff_date(Date dropoff_date) {
		this.dropoff_date = dropoff_date;
	}

	public CabModel(Row row) {
		try {
			String[] data = row.mkString("|").split(",");
			System.out.println("row--->"+row.mkString("|"));
			this.vendor_id = data[0];
			this.pickup_datetime =  new java.sql.Timestamp(datetimeformat.parse(data[2]).getTime());
			this.dropoff_datetime = new java.sql.Timestamp(datetimeformat.parse(data[3]).getTime());
			this.passenger_count = data[4];
			this.pickup_longitude = Double.parseDouble(data[5]);
			this.pickup_latitude = Double.parseDouble(data[6]);
			this.dropoff_longitude = Double.parseDouble(data[7]);
			this.dropoff_latitude = Double.parseDouble(data[8]);
			this.store_and_fwd_flag = data[9];
			this.trip_duration = Integer.parseInt(data[10]);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
	}

	public Timestamp getPickup_datetime() {
		return pickup_datetime;
	}

	public void setPickup_datetime(Timestamp pickup_datetime) {
		this.pickup_datetime = pickup_datetime;
	}

	public Timestamp getDropoff_datetime() {
		return dropoff_datetime;
	}

	public void setDropoff_datetime(Timestamp dropoff_datetime) {
		this.dropoff_datetime = dropoff_datetime;
	}
}
