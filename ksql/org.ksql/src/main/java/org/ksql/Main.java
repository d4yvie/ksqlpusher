package org.ksql;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.KsqlObject;

public class Main {

	public static void main(String[] args) throws IOException {
		new Main().doIt();
	}

	public void doIt() throws IOException {
		Reader in = new FileReader("src/main/resources/creditcard.csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
		StreamSupport.stream(records.spliterator(), false)
			.map(this::handleRecord)
			.collect(CompletableFutureUtil.collectResult());

		Client client = KsqlClientFactory.retrieveClient();
		client.close();
	}

	public CompletableFuture<Void> handleRecord(CSVRecord record) {
		String columnOne = record.get(0);
		String columnThree = record.get(29);
		String columnFour = record.get(30);
		// System.out.println(String.format("%s %s %s %s", columnOne, columnTwo, columnThree, columnFour));

		Client client = KsqlClientFactory.retrieveClient();
		KsqlObject row = new KsqlObject().put("Time", columnOne).put("Amount", columnThree).put("Fraud_check", columnFour);
		try {
			return client.insertInto("DA_TABLE", row);
		} catch (Exception e) {
			e.printStackTrace();
			return CompletableFuture.allOf(null);
		}
	}
}
