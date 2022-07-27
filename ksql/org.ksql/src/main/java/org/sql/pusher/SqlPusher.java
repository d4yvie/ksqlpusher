package org.sql.pusher;

import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;

public interface SqlPusher {
	void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream);
}
