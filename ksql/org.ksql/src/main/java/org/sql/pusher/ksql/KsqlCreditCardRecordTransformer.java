package org.sql.pusher.ksql;

import java.sql.Timestamp;

import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.KsqlObject;

public interface KsqlCreditCardRecordTransformer {
	public default KsqlObject recordToObject(CSVRecord record) {
//		String columnOne = record.get(0);
//		String columnThree = record.get(29);
//		String columnFour = record.get(30);
//		return new KsqlObject().put("Time", columnOne).put("Amount", columnThree).put("Fraud_check", columnFour);
		return new KsqlObject().put("Entrynumber", parseInt(record.get(0))).put("time", toTimeStamp(record.get(1)))
				.put("cc_num", record.get(2)).put("merchant", record.get(3)).put("category", record.get(4))
				.put("amt", parseDouble(record.get(5))).put("firstname", record.get(6)).put("lastname", record.get(7))
				.put("lastname", record.get(7)).put("gender", record.get(8)).put("street", record.get(9))
				.put("city", record.get(10)).put("state", record.get(11)).put("zip", parseInt(record.get(12)))
				.put("lat", parseDouble(record.get(13))).put("long", parseDouble(record.get(14)))
				.put("city_pop", parseInt(record.get(15))).put("job", record.get(16)).put("trans_num", record.get(17))
				.put("unix_time", parseInt(record.get(18))).put("merch_lat", parseDouble(record.get(19)))
				.put("merch_long", parseDouble(record.get(20))).put("is_fraud", parseInt(record.get(21)));
	}

	private String toTimeStamp(String timeStamp) {
		return Timestamp.valueOf(timeStamp).toString().replace(" ", "T");
	}

	private int parseInt(String number) {
		return Integer.parseInt(number);
	}

	private double parseDouble(String number) {
		return Double.parseDouble(number);
	}
}