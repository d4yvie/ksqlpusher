package org.sql.pusher.timescale;

import java.sql.Connection;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.SqlPusher;

public class TimeScalePusher implements SqlPusher {
	
	public final Connection connection = TimeScaleClientFactory.getConnection();

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		System.out.println(connection);
		
	}

}
