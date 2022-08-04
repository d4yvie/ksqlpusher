package org.sql.pusher.timescale;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class TimeScaleClientFactory {

	private static final String HOST = "localhost";
	private static final String PORT = "5432";
	private static final String USER = "postgres";
	private static final String PASSWORD = "password";
	private static final String DATABASE_NAME = "postgres";
	private static final String CONNECTION_URL = "jdbc:postgresql://%s:%s/%s?user=%s&password=%s";

	public static Connection getConnection() {
		try {
			return DriverManager.getConnection(String.format(CONNECTION_URL, HOST, PORT, DATABASE_NAME, USER, PASSWORD));
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String... args) throws SQLException {
		Connection conn = getConnection();
		// """DROP TABLE creditcard_data"""
		var createSensorTableQuery = 
				"""
				CREATE TABLE creditcard_data (Entrynumber integer, TIMESTAMPTZ timestamp, cc_num varchar, merchant varchar, category varchar, amt double precision, firstname varchar, lastname varchar, gender varchar, street varchar, city varchar, state varchar, zip integer, lat double precision, long double precision, city_pop integer, job varchar, trans_num varchar, unix_time integer, merch_lat double precision, merch_long double precision, is_fraud integer)  
				""";

		try (var stmt = conn.createStatement()) {
			stmt.execute(createSensorTableQuery);
		}
	}
}
