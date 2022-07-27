package org.sql.pusher.timescale;

import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.SqlPusher;

public class TimeScalePusher implements SqlPusher {

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		// TODO Auto-generated method stub
		
	}

}
