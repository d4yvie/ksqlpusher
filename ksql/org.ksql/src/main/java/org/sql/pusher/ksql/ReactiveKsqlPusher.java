package org.sql.pusher.ksql;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.Batcher;
import org.sql.pusher.SqlPusher;

import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.InsertsPublisher;
import io.confluent.ksql.api.client.KsqlObject;

public class ReactiveKsqlPusher implements SqlPusher, KsqlCreditCardRecordTransformer {

	private final int BUFFER_SIZE = 2000000;
	private final int REACTIVE_BATCH_SIZE = 150;

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
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
				AcksPublisher acksPublisher = client.streamInserts(stream, insertsPublisher).get();
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

	private boolean insertRecordReactive(KsqlObject record, InsertsPublisher insertsPublisher) {
		return insertsPublisher.accept(record);
	}
}
