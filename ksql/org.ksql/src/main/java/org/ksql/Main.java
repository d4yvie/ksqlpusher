package org.ksql;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlObject;

public class Main {
	
	private static String TABLE = "DA_TABLE";

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		new Main().doIt();
	}

	public void doIt() throws IOException, InterruptedException, ExecutionException {
		Reader in = new FileReader("src/main/resources/creditcard.csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
		Client client = KsqlClientFactory.retrieveClient();
		
		// not reactive handling
		StreamSupport.stream(records.spliterator(), false)
			.map(this::recordToObject)
			.map(this::handleRecord)
			.collect(CompletableFutureUtil.collectResult());

		// reactive handling
//		InsertsPublisher insertsPublisher = new InsertsPublisher();
//		AcksPublisher acksPublisher = client.streamInserts(TABLE, insertsPublisher).get();
//		StreamSupport.stream(records.spliterator(), false)
//			.map(this::recordToObject)
//			.map(record -> this.handleRecordReactive(record, insertsPublisher))
//			.collect(Collectors.toList());
//		insertsPublisher.complete();

		client.close();
	}

	public KsqlObject recordToObject(CSVRecord record) {
		String columnOne = record.get(0);
		String columnThree = record.get(29);
		String columnFour = record.get(30);
		return new KsqlObject().put("Time", columnOne).put("Amount", columnThree).put("Fraud_check", columnFour);
	}

	public CompletableFuture<Void> handleRecord(KsqlObject row) {
		Client client = KsqlClientFactory.retrieveClient();
		try {
			return client.insertInto(TABLE, row);
		} catch (Exception e) {
			e.printStackTrace();
			return CompletableFuture.allOf(null);
		}
	}

	public boolean handleRecordReactive(KsqlObject record, InsertsPublisher insertsPublisher) {
		return insertsPublisher.accept(record);
	}
}
