package org.ksql;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlObject;

public class Main {
	
	private static String TABLE = "creditcard_data";

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

	public void initDB() throws InterruptedException, ExecutionException {
		String sql = String.format("CREATE STREAM %s (Time BIGINT, Amount DOUBLE, Fraud_check INT)"
	               + "WITH (KAFKA_TOPIC='check_creditcard', VALUE_FORMAT='json');", TABLE);
		Client client = KsqlClientFactory.retrieveClient();
		client.executeStatement(sql).get();
		// creates new ksqlDB stream with topic
		client.executeStatement(String.format("DROP TABLE %s;", TABLE)).get();
		// drop table (name) if exist
		String sql1 = "CREATE TABLE creditcard_BY_USER AS "
	               + "SELECT Time, COUNT(*) as COUNT " //, "SELECT Amount, COUNT(*) as COUNT ", "SELECT Fraud_check, COUNT(*) as COUNT "
	               + String.format("FROM %s GROUP BY Time EMIT CHANGES;", TABLE);
		Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
		ExecuteStatementResult result = client.executeStatement(sql1, properties).get();
		System.out.println("Query ID: " + result.queryId().orElse("<null>"));
		// starts persistent query with stream from 
		client.executeStatement("TERMINATE CTAS_ORDERS_BY_USER_0;").get();
	}

	public boolean handleRecordReactive(KsqlObject record, InsertsPublisher insertsPublisher) {
		return insertsPublisher.accept(record);
	}
}
