package org.sql.pusher.timescale;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.SqlPusher;

public class SimpleTimeScalePusher implements SqlPusher {
	
	private final Connection connection = TimeScaleClientFactory.getConnection();

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		long t1 = System.nanoTime();
		System.out.println("STARTING SIMPLE TIMESCALE INSERTS");
		String queryTemplate = String.format("INSERT INTO %s (Time, Amount, Fraud_check) VALUES (?, ?, ?)", stream);
		try (var stmt = connection.prepareStatement(queryTemplate)) {
			recordsStream.forEach(record -> {
				try {
					stmt.setString(1, record.get(0));
					stmt.setString(2, record.get(29));
					stmt.setString(3, record.get(30));
					stmt.executeUpdate();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(String.format("Success! Simple TimeScale Inserting took %d seconds",
				TimeUnit.SECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS)));
	}
}
