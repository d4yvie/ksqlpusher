package org.ksql;

import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.KsqlObject;

public interface RecordTransformer {
	public default KsqlObject recordToObject(CSVRecord record) {
		String columnOne = record.get(0);
		String columnThree = record.get(29);
		String columnFour = record.get(30);
		return new KsqlObject().put("Time", columnOne).put("Amount", columnThree).put("Fraud_check", columnFour);
	}
}
