package org.sql.pusher.ksql;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.sql.pusher.CompletableFutureUtil;
import org.sql.pusher.SqlPusher;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.KsqlObject;

public class ThrottlingKsqlPusher implements SqlPusher, RecordTransformer {

	private final int TIME_BETWEEN_REQUEST_IN_MS = 1;

	@Override
	public void sendCsvToKsql(Stream<CSVRecord> recordsStream, String stream) {
		System.out.println("STARTING THROTTLING INSERTS");
		long t1 = System.nanoTime();
		recordsStream
				.parallel()
				.map(this::recordToObject)
				.map(record -> this.insertRecordThrottling(record, stream))
				.collect(CompletableFutureUtil.collectResult());
		System.out.println(String.format("Success! Throttling Inserting took %d seconds", TimeUnit.SECONDS.convert(System.nanoTime() - t1, TimeUnit.NANOSECONDS)));
	}

	private CompletableFuture<Void> insertRecordThrottling(KsqlObject row, String stream) {
		Client client = KsqlClientFactory.retrieveClient();
		try {
			Thread.sleep(TIME_BETWEEN_REQUEST_IN_MS);
			return client.insertInto(stream, row);
		} catch (Exception e) {
			e.printStackTrace();
			return CompletableFuture.allOf(null);
		}
	}
}
