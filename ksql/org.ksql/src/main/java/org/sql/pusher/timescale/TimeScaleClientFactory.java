package org.sql.pusher.timescale;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TimeScaleClientFactory {
	
	private static final String HOST = "localhost";
	private static final String PORT = "5432";
	private static final String CONNECTION_URL = String.format("jdbc:postgresql://%s:%s/dbname?user=username&password=password", HOST, PORT);
	
	public static Connection getConnection() {
		try {
			return DriverManager.getConnection(CONNECTION_URL);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
}
