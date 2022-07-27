package org.sql.pusher;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;

public class Main {

	private final String STREAM = "creditcard_data";
	private final boolean LOOP_FOREVER = false;
	private final SqlPusher KSQL_PUSHER = new ReactiveKsqlPusher(); // new ThrottlingKsqlPusher();
	// private final int THREAD_AMOUNT = 1;
	// 	private final ForkJoinExecutor EXECUTOR = new ForkJoinExecutor(THREAD_AMOUNT);

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		new Main().setup();
	}

	public void setup() throws IOException, InterruptedException, ExecutionException {
		do {
			try {
				pushToKsql();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} while (LOOP_FOREVER);
	}

	public void pushToKsql() throws Exception {
		long t1 = System.nanoTime();
		Reader in = new FileReader("src/main/resources/creditcard.csv");
		Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(in);
		Stream<CSVRecord> recordsStream = StreamSupport.stream(records.spliterator(), false);
		System.out.println("STARTING INSERTS");
		KSQL_PUSHER.sendCsvToKsql(recordsStream, STREAM);
		System.out.println(String.format("Success! Parsing and Inserting took %d seconds", TimeUnit.SECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS)));
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
