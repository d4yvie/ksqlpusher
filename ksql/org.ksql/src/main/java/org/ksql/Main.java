package org.ksql;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;

public class Main {

	private static String STREAM = "creditcard_data";
	private static int BUFFER_SIZE = 2000000;
	private static int REACTIVE_BATCH_SIZE = 150;
	private static int TIME_BETWEEN_REQUEST_IN_MS = 1;

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		new Main().doIt();
	}

	public void doIt() throws IOException, InterruptedException, ExecutionException {
		long t1 = System.nanoTime();
		Reader in = new FileReader("src/main/resources/creditcard.csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
		Stream<CSVRecord> recordsStream = StreamSupport.stream(records.spliterator(), false);
		System.out.println("STARTING INSERTS");
		// sendThrottling(recordsStream);
		sendReactiveBatched(recordsStream);
		System.out.println(String.format("Success! Parsing and Inserting took %d seconds", TimeUnit.SECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS)));
		System.out.println("SHUTTING DOWN");	
		KsqlClientFactory.retrieveClient().close();
	}

	public void sendReactiveBatched(Stream<CSVRecord> recordsStream) throws InterruptedException, ExecutionException {
		Client client = KsqlClientFactory.retrieveClient();
		List<KsqlObject> allRecords = recordsStream
				.map(this::recordToObject)
				.collect(Collectors.toList());
		long t1 = System.nanoTime();
		Stream<List<KsqlObject>> batches = Batcher.ofSubLists(allRecords, REACTIVE_BATCH_SIZE);
		System.out.println("STARTING REACTIVE INSERTS");
		batches
			.map(List::stream)
			.flatMap(batch -> {
			InsertsPublisher insertsPublisher = new InsertsPublisher(BUFFER_SIZE);
			try {
				AcksPublisher acksPublisher = client.streamInserts(STREAM, insertsPublisher).get();
				acksPublisher.subscribe(new AcksSubscriber());
				List<Boolean> results = batch
						.map(ksqlObject -> insertRecordReactive(ksqlObject, insertsPublisher))
						.collect(Collectors.toList());
				insertsPublisher.complete();
				return results.stream();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return Stream.of();
		}).collect(Collectors.toList());
		System.out.println(String.format("Success! Reactive Inserting took %d seconds", TimeUnit.SECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS)));
	}

	public void sendThrottling(Stream<CSVRecord> recordsStream) throws InterruptedException, ExecutionException {
		Client client = KsqlClientFactory.retrieveClient();
		recordsStream
				.map(this::recordToObject)
				.map(record -> this.insertRecordThrottling(record)).collect(CompletableFutureUtil.collectResult());
	}

	public KsqlObject recordToObject(CSVRecord record) {
		String columnOne = record.get(0);
		String columnThree = record.get(29);
		String columnFour = record.get(30);
		return new KsqlObject().put("Time", columnOne).put("Amount", columnThree).put("Fraud_check", columnFour);
	}

	public CompletableFuture<Void> insertRecordThrottling(KsqlObject row) {
		Client client = KsqlClientFactory.retrieveClient();
		try {
			Thread.sleep(TIME_BETWEEN_REQUEST_IN_MS);
			return client.insertInto(STREAM, row);
		} catch (Exception e) {
			e.printStackTrace();
			return CompletableFuture.allOf(null);
		}
	}

	public boolean insertRecordReactive(KsqlObject record, InsertsPublisher insertsPublisher) {
		return insertsPublisher.accept(record);
	}

	public void runSelect() {
		String selectQuery = String.format("SELECT * FROM %s EMIT CHANGES;", STREAM);

		Client client = KsqlClientFactory.retrieveClient();
		client.streamQuery(selectQuery).thenAccept(streamedQueryResult -> {
			System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());
			RowSubscriber subscriber = new RowSubscriber();
			streamedQueryResult.subscribe(subscriber);
		}).exceptionally(e -> {
			System.out.println("Request failed: " + e);
			return null;
		});

		System.out.println(selectQuery);
	}

	public void listInfo() throws InterruptedException, ExecutionException {

		Client client = KsqlClientFactory.retrieveClient();

		List<StreamInfo> streams = client.listStreams().get();
		for (StreamInfo stream : streams) {
			System.out.println(stream.getName() + " " + stream.getTopic() + " " + stream.getKeyFormat() + " "
					+ stream.getValueFormat() + " " + stream.isWindowed());
		}

		List<TableInfo> tables = client.listTables().get();
		for (TableInfo table : tables) {
			System.out.println(table.getName() + " " + table.getTopic() + " " + table.getKeyFormat() + " "
					+ table.getValueFormat() + " " + table.isWindowed());
		}

		List<TopicInfo> topics = client.listTopics().get();
		for (TopicInfo topic : topics) {
			System.out.println(topic.getName() + " " + topic.getPartitions() + " " + topic.getReplicasPerPartition());
		}

		List<QueryInfo> queries = client.listQueries().get();
		for (QueryInfo query : queries) {
			System.out.println(query.getQueryType() + " " + query.getId());
			if (query.getQueryType() == QueryType.PERSISTENT) {
				System.out.println(query.getSink().get() + " " + query.getSinkTopic().get());
			}
		}
	}
}
