package org.sql.pusher;

import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;

public class TimeScalePusher implements SqlPusher {

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		// TODO Auto-generated method stub
		
	}

}