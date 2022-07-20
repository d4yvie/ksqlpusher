package org.ksql;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

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
	
	private static String TABLE = "creditcard_data";
	private static int BUFFER_SIZE = 2000000000;
	private static int TIME_BETWEEN_REQUEST_IN_MS = 1;

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		new Main().doIt();
	}

	public void doIt() throws IOException, InterruptedException, ExecutionException {
		Reader in = new FileReader("src/main/resources/creditcard.csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
		
		// sendReactive(records);
		
		send(records);

		KsqlClientFactory.retrieveClient().close();
	}
	
	public void sendReactiveBatched(Iterable<CSVRecord> records) throws InterruptedException, ExecutionException { 
		UnmodifiableIterator<List<CSVRecord>> batchIterator = Iterators.partition(records.iterator(), 200);
		Client client = KsqlClientFactory.retrieveClient();
		
		
		
		InsertsPublisher insertsPublisher = new InsertsPublisher(BUFFER_SIZE);
		AcksPublisher acksPublisher = client.streamInserts(TABLE, insertsPublisher).get();
		acksPublisher.subscribe(new AcksSubscriber());
		List<Boolean> successful = StreamSupport.stream(records.spliterator(), false)
			.map(this::recordToObject)
			
			.map(record -> this.handleRecordReactive(record, insertsPublisher))
			.collect(Collectors.toList());
	}
	
	public void sendReactive(Iterable<CSVRecord> records) throws InterruptedException, ExecutionException { 
		Client client = KsqlClientFactory.retrieveClient();
		InsertsPublisher insertsPublisher = new InsertsPublisher(BUFFER_SIZE);
		AcksPublisher acksPublisher = client.streamInserts(TABLE, insertsPublisher).get();
		acksPublisher.subscribe(new AcksSubscriber());
		List<Boolean> successful = StreamSupport.stream(records.spliterator(), false)
			.map(this::recordToObject)
			.map(record -> this.handleRecordReactive(record, insertsPublisher))
			.collect(Collectors.toList());
	}

	public void send(Iterable<CSVRecord> records) throws InterruptedException, ExecutionException { 
		Client client = KsqlClientFactory.retrieveClient();
		StreamSupport.stream(records.spliterator(), false)
			.map(this::recordToObject)
			.map(record -> this.handleRecord(record))
			.collect(CompletableFutureUtil.collectResult());
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
			Thread.sleep(TIME_BETWEEN_REQUEST_IN_MS);
			return client.insertInto(TABLE, row);
		} catch (Exception e) {
			e.printStackTrace();
			return CompletableFuture.allOf(null);
		}
	}
	
	public void runSelect() {
		String selectQuery = String.format("SELECT * FROM %s EMIT CHANGES;", TABLE);
		
		Client client = KsqlClientFactory.retrieveClient();
		client.streamQuery(selectQuery)
	    .thenAccept(streamedQueryResult -> {
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
		  System.out.println(
		     stream.getName() 
		     + " " + stream.getTopic() 
		     + " " + stream.getKeyFormat()
		     + " " + stream.getValueFormat()
		     + " " + stream.isWindowed()
		  );
		}
		
		List<TableInfo> tables = client.listTables().get();
		for (TableInfo table : tables) {
		  System.out.println(
		       table.getName() 
		       + " " + table.getTopic() 
		       + " " + table.getKeyFormat()
		       + " " + table.getValueFormat()
		       + " " + table.isWindowed()
		    );
		}
		
		List<TopicInfo> topics = client.listTopics().get();
		for (TopicInfo topic : topics) {
		  System.out.println(
		       topic.getName() 
		       + " " + topic.getPartitions() 
		       + " " + topic.getReplicasPerPartition()
		  );
		}
		
		List<QueryInfo> queries = client.listQueries().get();
		for (QueryInfo query : queries) {
		  System.out.println(query.getQueryType() + " " + query.getId());
		  if (query.getQueryType() == QueryType.PERSISTENT) {
		    System.out.println(query.getSink().get() + " " + query.getSinkTopic().get());
		  }
		}
	}

	public boolean handleRecordReactive(KsqlObject record, InsertsPublisher insertsPublisher) {
		try {
			Thread.sleep(TIME_BETWEEN_REQUEST_IN_MS);
			return insertsPublisher.accept(record);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			return insertsPublisher.accept(record);
		}
	}
}
