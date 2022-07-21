package org.ksql;

import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;

public interface KsqlPusher {
	void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream);
}
